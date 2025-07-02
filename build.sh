docker compose build
docker create --name temp-sidekiq-metrics sidekiq-metrics
docker cp temp-sidekiq-metrics:/app/sidekiq-metrics ./sidekiq-metrics
docker rm temp-sidekiq-metrics
chmod +x ./sidekiq-metrics
