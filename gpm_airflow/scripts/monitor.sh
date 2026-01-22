#!/bin/bash

echo "📊 GPM Pipeline Monitoring Dashboard"
echo "====================================="

# Check docker compose command
if command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE="docker-compose"
elif docker compose version &> /dev/null; then
    DOCKER_COMPOSE="docker compose"
else
    echo "❌ Error: Docker Compose not found"
    exit 1
fi

echo "Service Status:"
$DOCKER_COMPOSE ps 2>/dev/null || echo "No services running"

echo -e "\nDisk Usage:"
du -sh data/gpm/* 2>/dev/null | sort -hr || echo "No data directory"

echo -e "\nDAG Status:"
$DOCKER_COMPOSE exec airflow-webserver airflow dags list 2>/dev/null | grep -E "(gpm|running|failed)" || echo "Airflow not running"

echo -e "\nRecent Task Instances:"
$DOCKER_COMPOSE exec airflow-webserver airflow tasks list gpm_monthly_processor 2>/dev/null || echo "DAG not found or Airflow not running"

echo -e "\nLog Summary:"
$DOCKER_COMPOSE logs --tail=20 airflow-scheduler 2>/dev/null || echo "Cannot get logs"