package main

import (
	"context"
	"crypto/tls"
	"database/sql"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strings"

	// MongoDB for Agenda support
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	mysql "github.com/go-sql-driver/mysql"

	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"

	"github.com/go-redis/redis/v8"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"google.golang.org/grpc"
)

var ctx = context.Background()

func setupMeterProvider(exporterType, otlpEndpoint, otelServiceName, otelServiceNamespace, otelServiceEnv, otelHost string) (*sdkmetric.MeterProvider, error) {
	var exporter sdkmetric.Exporter
	var err error
	switch exporterType {
	case "console":
		exporter, err = stdoutmetric.New(
			stdoutmetric.WithPrettyPrint(),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create console exporter: %w", err)
		}
	case "otlp":
		exporter, err = otlpmetricgrpc.New(ctx,
			otlpmetricgrpc.WithInsecure(),
			otlpmetricgrpc.WithEndpoint(otlpEndpoint),
			otlpmetricgrpc.WithDialOption(grpc.WithBlock()),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create OTLP exporter: %w", err)
		}
	default:
		return nil, fmt.Errorf("unknown exporter type: %s", exporterType)
	}

	res, err := resource.New(
		context.Background(),
		resource.WithAttributes(
			semconv.ServiceNameKey.String(otelServiceName),
			semconv.ServiceNamespaceKey.String(otelServiceNamespace),
			semconv.DeploymentEnvironmentKey.String(otelServiceEnv),
			semconv.HostNameKey.String(otelHost),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter)),
	)
	otel.SetMeterProvider(provider)

	return provider, nil
}

func getSidekiqQueueLengths(rdb *redis.Client) (map[string]int, error) {
	queues := make(map[string]int)

	keys, err := rdb.Keys(ctx, "queue:*").Result()
	if err != nil {
		return nil, err
	}

	for _, key := range keys {
		queueName := key[len("queue:"):]
		length, err := rdb.LLen(ctx, key).Result()
		if err != nil {
			// If the key exists but is not a list, treat as zero
			queues[queueName] = 0
			continue
		}
		// Always include the queue, even if length is 0
		queues[queueName] = int(length)
	}

	return queues, nil
}
func main() {
	var (
		jobType              = flag.String("job-type", "sidekiq", "Type of jobs to collect: 'sidekiq', 'laravel', or 'agenda'")
		mysqlDSN             = flag.String("mysql-dsn", "laravel:secret@tcp(mysql:3306)/laravel", "MySQL DSN or URL (e.g. user:pass@tcp(host:port)/db or mysql://host:port/db?useSSL=true)")
		redisAddr            = flag.String("redis-addr", "localhost:6379", "Redis server address (host:port)")
		redisPassword        = flag.String("redis-password", "", "Redis password (optional)")
		redisDB              = flag.Int("redis-db", 0, "Redis database number")
		redisTLS             = flag.Bool("redis-tls", false, "Enable TLS/SSL for Redis connection")
		showHelp             = flag.Bool("help", false, "Show help message")
		exporterType         = flag.String("exporter", "console", "Exporter type: 'otlp' or 'console'")
		otlpEndpoint         = flag.String("otlp-endpoint", "localhost:4317", "OTLP exporter endpoint (host:port)")
		daemonMode           = flag.Bool("daemon", false, "Run as a daemon (repeat at interval)")
		interval             = flag.Duration("interval", 10_000_000_000, "Interval between metric collections (e.g., 10s, 1m)")
		otelServiceName      = flag.String("otel-service-name", "job-metrics", "OpenTelemetry service name")
		otelServiceNamespace = flag.String("otel-service-namespace", "default", "OpenTelemetry service namespace")
		otelServiceEnv       = flag.String("otel-service-env", "dev", "OpenTelemetry deployment environment")
		otelHost             = flag.String("otel-host", "localhost", "OpenTelemetry host name")
		agendaMongoURI       = flag.String("agenda-mongo-uri", "mongodb://localhost:27017/agenda", "MongoDB connection string for Agenda jobs")
		agendaCollection     = flag.String("agenda-collection", "agendaJobs", "MongoDB collection name for Agenda jobs")
	)
	flag.Parse()

	if *showHelp {
		fmt.Println("Usage of job-metrics:")
		flag.PrintDefaults()
		os.Exit(0)
	}

	var rdb *redis.Client
	var db *sql.DB
	var mongoClient *mongo.Client
	var mongoDBName string
	if *jobType == "sidekiq" {
		redisOpts := &redis.Options{
			Addr:     *redisAddr,
			Password: *redisPassword,
			DB:       *redisDB,
		}
		if *redisTLS {
			tlsConfig := &tls.Config{
				InsecureSkipVerify: true,
			}
			redisOpts.TLSConfig = tlsConfig
		}
		rdb = redis.NewClient(redisOpts)
	} else if *jobType == "laravel" {
		dsn := *mysqlDSN
		// If the DSN looks like a URL, convert to DSN and handle SSL
		if strings.HasPrefix(dsn, "mysql://") {
			u, err := url.Parse(dsn)
			if err != nil {
				fmt.Println("Invalid MySQL URL:", err)
				os.Exit(1)
			}
			// Extract user/pass
			user := ""
			pass := ""
			if u.User != nil {
				user = u.User.Username()
				pass, _ = u.User.Password()
			}
			host := u.Host
			dbName := strings.TrimPrefix(u.Path, "/")
			params := u.Query()
			// SSL options
			useSSL := params.Get("useSSL") == "true"
			requireSSL := params.Get("requireSSL") == "true"
			tlsName := ""
			if useSSL || requireSSL {
				tlsConfig := &tls.Config{
					MinVersion:         tls.VersionTLS12,
					InsecureSkipVerify: false,
				}
				if params.Get("insecureSkipVerify") == "true" {
					tlsConfig.InsecureSkipVerify = true
				}
				tlsName = "custom"
				err := mysql.RegisterTLSConfig(tlsName, tlsConfig)
				if err != nil {
					fmt.Println("Failed to register MySQL TLS config:", err)
					os.Exit(1)
				}
			}
			// Build DSN
			dsn = fmt.Sprintf("%s:%s@tcp(%s)/%s", user, pass, host, dbName)
			if tlsName != "" {
				dsn += fmt.Sprintf("?tls=%s", tlsName)
			}
		}
		var err error
		db, err = sql.Open("mysql", dsn)
		if err != nil {
			fmt.Println("Error connecting to MySQL:", err)
			os.Exit(1)
		}
		defer db.Close()
	} else if *jobType == "agenda" {
		uri := *agendaMongoURI
		clientOpts := options.Client().ApplyURI(uri)
		var err error
		mongoClient, err = mongo.Connect(ctx, clientOpts)
		if err != nil {
			fmt.Println("Error connecting to MongoDB:", err)
			os.Exit(1)
		}
		u, err := url.Parse(uri)
		if err != nil {
			fmt.Println("Invalid MongoDB URI:", err)
			os.Exit(1)
		}
		mongoDBName = strings.TrimPrefix(u.Path, "/")
		if mongoDBName == "" {
			fmt.Println("MongoDB URI must include a database name")
			os.Exit(1)
		}
		defer func() { _ = mongoClient.Disconnect(ctx) }()
	} else {
		fmt.Println("Unknown job type. Use 'sidekiq', 'laravel', or 'agenda'.")
		os.Exit(1)
	}

	mp, err := setupMeterProvider(*exporterType, *otlpEndpoint, *otelServiceName, *otelServiceNamespace, *otelServiceEnv, *otelHost)
	if err != nil {
		fmt.Println("Failed to setup meter provider:", err)
		os.Exit(1)
	}
	defer func() { _ = mp.Shutdown(ctx) }()

	meter := otel.Meter("job-metrics")

	queueGauge, err := meter.Float64ObservableGauge(
		"job_queue_length",
	)
	if err != nil {
		fmt.Println("Error creating gauge:", err)
		os.Exit(1)
	}

	if *daemonMode {
		fmt.Printf("Collecting %s metrics every %s... (Ctrl+C to stop)\n", *jobType, interval.String())
		for {
			_, err := meter.RegisterCallback(
				func(ctx context.Context, o metric.Observer) error {
					if *jobType == "sidekiq" {
						queues, err := getSidekiqQueueLengths(rdb)
						if err != nil {
							return err
						}
						for name, count := range queues {
							o.ObserveFloat64(queueGauge, float64(count), metric.WithAttributes(attribute.String("queue", name)))
						}
					} else if *jobType == "laravel" {
						queueCounts, err := getLaravelJobsQueueCounts(db)
						if err != nil {
							return err
						}
						for queue, count := range queueCounts {
							o.ObserveFloat64(queueGauge, float64(count), metric.WithAttributes(attribute.String("queue", queue)))
						}
					} else if *jobType == "agenda" {
						agendaCounts, err := getAgendaQueueCounts(mongoClient, mongoDBName, *agendaCollection)
						if err != nil {
							return err
						}
						for queue, count := range agendaCounts {
							o.ObserveFloat64(queueGauge, float64(count), metric.WithAttributes(attribute.String("queue", queue)))
						}
					}
					return nil
				},
				queueGauge,
			)
			if err != nil {
				fmt.Println("Error registering callback:", err)
			}
			<-time.After(*interval)
		}
	} else {
		// One-shot: print to console for visibility
		if *jobType == "sidekiq" {
			queues, err := getSidekiqQueueLengths(rdb)
			if err != nil {
				fmt.Println("Error collecting queue lengths:", err)
				os.Exit(1)
			}
			for name, count := range queues {
				fmt.Printf("queue=%s length=%d\n", name, count)
			}
			_, err = meter.RegisterCallback(
				func(ctx context.Context, o metric.Observer) error {
					for name, count := range queues {
						o.ObserveFloat64(queueGauge, float64(count), metric.WithAttributes(attribute.String("queue", name)))
					}
					return nil
				},
				queueGauge,
			)
			if err != nil {
				fmt.Println("Error registering callback:", err)
				os.Exit(1)
			}
		} else if *jobType == "laravel" {
			queueCounts, err := getLaravelJobsQueueCounts(db)
			if err != nil {
				fmt.Println("Error collecting laravel job queue counts:", err)
				os.Exit(1)
			}
			for queue, count := range queueCounts {
				fmt.Printf("queue=%s length=%d\n", queue, count)
			}
			_, err = meter.RegisterCallback(
				func(ctx context.Context, o metric.Observer) error {
					for queue, count := range queueCounts {
						o.ObserveFloat64(queueGauge, float64(count), metric.WithAttributes(attribute.String("queue", queue)))
					}
					return nil
				},
				queueGauge,
			)
			if err != nil {
				fmt.Println("Error registering callback:", err)
				os.Exit(1)
			}
		} else if *jobType == "agenda" {
			agendaCounts, err := getAgendaQueueCounts(mongoClient, mongoDBName, *agendaCollection)
			if err != nil {
				fmt.Println("Error collecting agenda job queue counts:", err)
				os.Exit(1)
			}
			for queue, count := range agendaCounts {
				fmt.Printf("queue=%s length=%d\n", queue, count)
			}
			_, err = meter.RegisterCallback(
				func(ctx context.Context, o metric.Observer) error {
					for queue, count := range agendaCounts {
						o.ObserveFloat64(queueGauge, float64(count), metric.WithAttributes(attribute.String("queue", queue)))
					}
					return nil
				},
				queueGauge,
			)
			if err != nil {
				fmt.Println("Error registering callback:", err)
				os.Exit(1)
			}
		}
		// Give time for the exporter to flush
		time.Sleep(2 * time.Second)
	}
}

// getAgendaQueueCounts returns the number of jobs per queue (name) in the Agenda jobs collection
func getAgendaQueueCounts(client *mongo.Client, dbName, collectionName string) (map[string]int, error) {
	coll := client.Database(dbName).Collection(collectionName)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pipeline := mongo.Pipeline{
		bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$name"}, {Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}}}}},
	}
	cursor, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	result := make(map[string]int)
	for cursor.Next(ctx) {
		var doc struct {
			ID    string `bson:"_id"`
			Count int    `bson:"count"`
		}
		if err := cursor.Decode(&doc); err != nil {
			return nil, err
		}
		result[doc.ID] = doc.Count
	}
	return result, nil
}

// getLaravelJobsQueueCounts returns the number of pending jobs per queue in the default Laravel jobs table
func getLaravelJobsQueueCounts(db *sql.DB) (map[string]int, error) {
	rows, err := db.Query("SELECT queue, COUNT(*) FROM jobs GROUP BY queue")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string]int)
	for rows.Next() {
		var queue string
		var count int
		if err := rows.Scan(&queue, &count); err != nil {
			return nil, err
		}
		result[queue] = count
	}
	return result, nil
}
