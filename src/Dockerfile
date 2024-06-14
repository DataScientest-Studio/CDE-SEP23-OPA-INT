FROM python:3.12

WORKDIR /app

COPY . .

RUN apt-get update && apt-get install -y libhdf5-dev
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r trading_bot_api/requirements.txt

EXPOSE 8086

WORKDIR /app/trading_bot_api

CMD uvicorn  tradingbot_api:api --host 0.0.0.0 --port 8086

