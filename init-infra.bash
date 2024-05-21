#!/bin/bash

docker-compose down -v
docker-compose up -d db

echo "Waiting for db service to be ready..."
while ! docker-compose logs db | grep 'database system is ready to accept connections'; do
  sleep 1
done

echo "Running init_data script..."
python init_data.py

docker-compose up -d

docker-compose exec spark-master python3 /load_data/run.py