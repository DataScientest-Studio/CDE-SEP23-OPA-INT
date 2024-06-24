docker-compose down -v

docker-compose up --build -d

Write-Host "Waiting for containers to be ready..."

while ((docker-compose ps | Select-String 'spark' | Select-String 'Up' | Measure-Object -Line).Lines -ne (docker-compose ps | Select-String 'spark' | Measure-Object -Line).Lines) {
  Write-Host "Spark containers not ready yet, going to sleep"
  Start-Sleep -Seconds 5
}

Write-Host "All containers are ready."

docker-compose exec -it spark-master python3 /load_data/run.py
