FROM python:3.12-alpine

WORKDIR /app

ADD . /app

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

CMD python init_db.py