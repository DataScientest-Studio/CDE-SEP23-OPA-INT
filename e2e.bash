docker-compose down -v

docker-compose up --build -d

echo "Waiting for containers to be ready..."

while [ "$(docker-compose ps | grep 'spark' | grep 'Up' | wc -l)" != "$(docker-compose ps | grep 'spark' | wc -l)" ]; do
  echo "Spark containers not ready yet, going to sleep"
  sleep 5
done

echo "All containers are ready."

docker-compose exec -it spark-master python3 /load_data/run.py

docker-compose exec -it trading_bot_api python3 create_fast_model.py