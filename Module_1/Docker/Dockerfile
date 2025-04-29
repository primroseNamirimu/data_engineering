FROM python:3.9

RUN apt-get install wget
RUN pip install pandas pgcli sqlalchemy  psycopg2
WORKDIR /app

COPY ingest_data_.py  ingest_data_.py

ENTRYPOINT [ "python","ingest_data_.py"]

