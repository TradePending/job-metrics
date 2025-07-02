docker compose build
docker create --name temp-job-metrics job-metrics
docker cp temp-job-metrics:/app/job-metrics ./job-metrics
docker rm temp-job-metrics
chmod +x ./job-metrics
