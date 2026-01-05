FROM python:3.10-slim

RUN apt-get update && \
    apt-get install -y default-jre procps curl libpq-dev gcc && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
WORKDIR /app

RUN pip install pyspark pandas matplotlib psycopg2-binary bcrypt streamlit

RUN mkdir -p /opt/spark-jars && \
    curl -o /opt/spark-jars/postgresql.jar https://jdbc.postgresql.org/download/postgresql-42.7.2.jar

COPY . .