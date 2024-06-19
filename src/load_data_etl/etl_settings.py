import os

spark = dict(
    host="spark://spark-master:7077"
)

pg = dict(
    conn="jdbc:postgresql://postgres:5432/opa_db",
    user="db_user",
    password="pgpassword123"
)

download_year = os.environ.get("DOWNLOAD_YEAR", 2024)