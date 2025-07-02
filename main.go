package main

import (
	   "context"
	   "crypto/tls"
	   "flag"
	   "fmt"
	   "os"
	   "time"

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
	// Flags
	   var (
			   redisAddr            = flag.String("redis-addr", "localhost:6379", "Redis server address (host:port)")
			   redisPassword        = flag.String("redis-password", "", "Redis password (optional)")
			   redisDB              = flag.Int("redis-db", 0, "Redis database number")
			   redisTLS             = flag.Bool("redis-tls", false, "Enable TLS/SSL for Redis connection")
			   showHelp             = flag.Bool("help", false, "Show help message")
			   exporterType         = flag.String("exporter", "console", "Exporter type: 'otlp' or 'console'")
			   otlpEndpoint         = flag.String("otlp-endpoint", "localhost:4317", "OTLP exporter endpoint (host:port)")
			   daemonMode           = flag.Bool("daemon", false, "Run as a daemon (repeat at interval)")
			   interval             = flag.Duration("interval", 10_000_000_000, "Interval between metric collections (e.g., 10s, 1m)")
			   otelServiceName      = flag.String("otel-service-name", "sidekiq-metrics", "OpenTelemetry service name")
			   otelServiceNamespace = flag.String("otel-service-namespace", "default", "OpenTelemetry service namespace")
			   otelServiceEnv       = flag.String("otel-service-env", "dev", "OpenTelemetry deployment environment")
			   otelHost             = flag.String("otel-host", "localhost", "OpenTelemetry host name")
	   )
	flag.Parse()

	if *showHelp {
		fmt.Println("Usage of sidekiq-metrics:")
		flag.PrintDefaults()
		os.Exit(0)
	}

	   redisOpts := &redis.Options{
			   Addr:     *redisAddr,
			   Password: *redisPassword,
			   DB:       *redisDB,
	   }
	   if *redisTLS {
			   redisOpts.TLSConfig = &tls.Config{}
	   }
	   rdb := redis.NewClient(redisOpts)

	mp, err := setupMeterProvider(*exporterType, *otlpEndpoint, *otelServiceName, *otelServiceNamespace, *otelServiceEnv, *otelHost)
	if err != nil {
		fmt.Println("Failed to setup meter provider:", err)
		os.Exit(1)
	}
	defer func() { _ = mp.Shutdown(ctx) }()

	meter := otel.Meter("sidekiq-metrics")

	queueGauge, err := meter.Float64ObservableGauge(
		"sidekiq_queue_length",
	)
	if err != nil {
		fmt.Println("Error creating gauge:", err)
		os.Exit(1)
	}

	if *daemonMode {
		fmt.Printf("Collecting Sidekiq metrics every %s... (Ctrl+C to stop)\n", interval.String())
		for {
			_, err := meter.RegisterCallback(
				func(ctx context.Context, o metric.Observer) error {
					queues, err := getSidekiqQueueLengths(rdb)
					if err != nil {
						return err
					}
					for name, count := range queues {
						o.ObserveFloat64(queueGauge, float64(count), metric.WithAttributes(attribute.String("queue", name)))
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
		// Give time for the exporter to flush
		time.Sleep(2 * time.Second)
	}
}
