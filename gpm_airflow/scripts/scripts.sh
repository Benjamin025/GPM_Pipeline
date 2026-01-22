#!/bin/bash

echo "🚀 Deploying GPM Airflow Pipeline"

# Set up environment
export AIRFLOW_UID=$(id -u)

# Create data directories
mkdir -p data/gpm/{raw_daily_nc,geotiffs,metadata,logs,temp}

# Set permissions
chmod -R 775 data/

# Create .netrc if it doesn't exist
if [ ! -f ~/.netrc ]; then
    echo "Creating .netrc file for NASA credentials..."
    echo "machine urs.earthdata.nasa.gov" > ~/.netrc
    echo "    login ${NASA_USERNAME:-your_username}" >> ~/.netrc
    echo "    password ${NASA_PASSWORD:-your_password}" >> ~/.netrc
    chmod 600 ~/.netrc
fi

# Build and start Airflow
echo "Starting Airflow services..."
docker-compose down 2>/dev/null
docker-compose up --build -d

# Wait for services
echo "Waiting for services to start..."
sleep 30

# Initialize Airflow
echo "Initializing Airflow..."
docker-compose exec airflow-webserver airflow db init 2>/dev/null || true

# Create admin user if not exists
docker-compose exec airflow-webserver airflow users create \
    --username admin \
    --password admin123 \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com 2>/dev/null || true

# Create connection pools
echo "Creating connection pools..."
docker-compose exec airflow-webserver airflow pools set gpm_download_pool 5 "GPM Download Pool" || true
docker-compose exec airflow-webserver airflow pools set gpm_process_pool 3 "GPM Process Pool" || true

echo "✅ Deployment complete!"
echo "🌐 Airflow UI: http://localhost:8080"
echo "👤 Username: admin"
echo "🔑 Password: admin123"
echo ""
echo "📋 Available DAGs:"
docker-compose exec airflow-webserver airflow dags list | grep gpm