#!/bin/bash

echo "🚀 Deploying GPM Airflow Pipeline"

# Set up environment
export AIRFLOW_UID=$(id -u)

# Create data directories
mkdir -p data/gpm/{raw_daily_nc,geotiffs,metadata,logs,temp}

# Set permissions
sudo chmod -R 775 data/ 2>/dev/null || chmod -R 775 data/

# Create .netrc if it doesn't exist
if [ ! -f ~/.netrc ]; then
    echo "Creating .netrc file for NASA credentials..."
    echo "machine urs.earthdata.nasa.gov" > ~/.netrc
    echo "    login ${NASA_USERNAME:-your_username}" >> ~/.netrc
    echo "    password ${NASA_PASSWORD:-your_password}" >> ~/.netrc
    chmod 600 ~/.netrc
fi

# Check if docker compose is available
if command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE="docker-compose"
elif docker compose version &> /dev/null; then
    DOCKER_COMPOSE="docker compose"
else
    echo "❌ Error: Neither docker-compose nor docker compose found"
    echo "Install Docker Compose: https://docs.docker.com/compose/install/"
    exit 1
fi

echo "Using: $DOCKER_COMPOSE"

# Build and start Airflow
echo "Starting Airflow services..."
$DOCKER_COMPOSE down 2>/dev/null || true
$DOCKER_COMPOSE up --build -d

# Wait for services
echo "Waiting for services to start..."
sleep 30

# Initialize Airflow
echo "Initializing Airflow..."
$DOCKER_COMPOSE exec airflow-webserver airflow db init 2>/dev/null || true

# Create admin user if not exists
$DOCKER_COMPOSE exec airflow-webserver airflow users create \
    --username admin \
    --password admin123 \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com 2>/dev/null || true

# Create connection pools
echo "Creating connection pools..."
$DOCKER_COMPOSE exec airflow-webserver airflow pools set gpm_download_pool 5 "GPM Download Pool" 2>/dev/null || true
$DOCKER_COMPOSE exec airflow-webserver airflow pools set gpm_process_pool 3 "GPM Process Pool" 2>/dev/null || true

echo "✅ Deployment complete!"
echo "🌐 Airflow UI: http://localhost:8080"
echo "👤 Username: admin"
echo "🔑 Password: admin123"
echo ""
echo "📋 Available DAGs:"
$DOCKER_COMPOSE exec airflow-webserver airflow dags list 2>/dev/null | grep -i gpm || echo "No GPM DAGs found yet"