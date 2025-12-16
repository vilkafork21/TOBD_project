FROM apache/airflow:2.8.0

USER root

# Установка Java (требуется для PySpark)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Установка Python зависимостей для Spark
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    psycopg2-binary==2.9.9 \
    pandas==2.0.3 \
    numpy==1.24.3

# Загрузка PostgreSQL JDBC драйвера
RUN mkdir -p /home/airflow/.ivy2/jars && \
    curl -L -o /home/airflow/.ivy2/jars/postgresql-42.7.1.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.1.jar

