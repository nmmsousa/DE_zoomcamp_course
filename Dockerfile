FROM python:3.12.8

RUN pip install pandas sqlalchemy psycopg2 wget pyarrow

WORKDIR /app
COPY ingest_data.py ingest_data.py

ENTRYPOINT ["python", "ingestion_data.py"]