FROM bitnami/spark:3.5.1

# Install Python dependencies
COPY . /load_data
RUN pip install -r /load_data/requirements.txt

USER root
RUN chmod -R 777 /load_data
USER 1001