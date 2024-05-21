docker-compose down -v

docker-compose up -d db

Write-Host "Waiting for db service to be ready..."
do {
    Start-Sleep -Seconds 1
} until ((docker-compose logs db) -match 'database system is ready to accept connections')

Write-Host "Running init_data script..."
python init_data.py

docker-compose up --build -d

Write-Host "Running run.py script on spark-master container..."
docker-compose exec spark-master python3 /load_data/run.py