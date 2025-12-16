"""
Airflow DAG для ETL пайплайна энергетической аналитики
Использует PySpark в режиме local для обработки данных
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import sys

# Добавляем путь к spark_jobs в PYTHONPATH
sys.path.insert(0, '/opt/airflow/spark_jobs')

# Параметры по умолчанию
default_args = {
    'owner': 'energy_analytics',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Определение DAG
dag = DAG(
    'energy_analytics_etl',
    default_args=default_args,
    description='ETL пайплайн для анализа потребления электроэнергии',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['energy', 'etl', 'spark', 'postgres'],
)

# Пути к данным
DATA_PATH = "/opt/airflow/data/raw"
PROCESSED_PATH = "/opt/airflow/data/processed"

# Переменные окружения для PostgreSQL
PG_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'energy_analytics'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'postgres'),
}


def check_data_availability():
    """Проверка наличия данных"""
    import glob
    
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(f"Директория {DATA_PATH} не существует")
    
    csv_files = glob.glob(f"{DATA_PATH}/*.csv")
    if not csv_files:
        raise FileNotFoundError(f"Не найдены CSV файлы в {DATA_PATH}")
    
    print(f"Найдено CSV файлов: {len(csv_files)}")
    for f in csv_files:
        print(f"  - {f}")
    return csv_files


def check_postgres_connection():
    """Проверка подключения к PostgreSQL"""
    import psycopg2
    
    print(f"Подключение к PostgreSQL: {PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['database']}")
    
    try:
        conn = psycopg2.connect(
            host=PG_CONFIG['host'],
            port=PG_CONFIG['port'],
            database=PG_CONFIG['database'],
            user=PG_CONFIG['user'],
            password=PG_CONFIG['password']
        )
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"PostgreSQL подключен: {version[:50]}...")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        raise ConnectionError(f"Не удалось подключиться к PostgreSQL: {e}")


def run_extract():
    """Extract: Загрузка CSV в PostgreSQL"""
    import glob
    import psycopg2
    from psycopg2.extras import execute_values
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, to_date, to_timestamp
    
    print("=" * 50)
    print("EXTRACT: Начало загрузки данных")
    print("=" * 50)
    
    # Создание SparkSession в режиме local
    spark = SparkSession.builder \
        .appName("EnergyAnalytics-Extract") \
        .master("local[*]") \
        .config("spark.driver.memory", "1g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    try:
        # Чтение CSV файлов
        print(f"Чтение CSV из {DATA_PATH}")
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(f"{DATA_PATH}/*.csv")
        
        print(f"Загружено строк: {df.count()}")
        print(f"Колонки: {df.columns}")
        df.show(5)
        
        # Преобразование колонок
        if "day" in df.columns:
            df = df.withColumn("day", to_date(col("day"), "yyyy-MM-dd"))
        if "timestamp" in df.columns:
            df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
        if "energy_kwh" in df.columns:
            df = df.withColumn("energy_kwh", col("energy_kwh").cast("double"))
        
        # Очистка данных
        df_cleaned = df.filter(
            col("LCLid").isNotNull() &
            col("day").isNotNull() &
            col("energy_kwh").isNotNull()
        ).dropDuplicates(["LCLid", "day", "timestamp"])
        
        print(f"После очистки строк: {df_cleaned.count()}")
        
        # Сбор данных
        rows = df_cleaned.select("LCLid", "day", "energy_kwh", "timestamp").collect()
        
        if not rows:
            print("Нет данных для загрузки")
            return
        
        # Подключение к PostgreSQL
        conn = psycopg2.connect(
            host=PG_CONFIG['host'],
            port=PG_CONFIG['port'],
            database=PG_CONFIG['database'],
            user=PG_CONFIG['user'],
            password=PG_CONFIG['password']
        )
        cursor = conn.cursor()
        
        # Очистка таблицы перед загрузкой
        cursor.execute("TRUNCATE TABLE raw.energy_consumption")
        
        # Подготовка данных
        data_to_insert = []
        for row in rows:
            data_to_insert.append((
                row.LCLid,
                row.day,
                float(row.energy_kwh) if row.energy_kwh is not None else None,
                row.timestamp
            ))
        
        # Вставка данных
        insert_query = """
            INSERT INTO raw.energy_consumption (lclid, day, energy_kwh, timestamp)
            VALUES %s
        """
        execute_values(cursor, insert_query, data_to_insert, page_size=1000)
        conn.commit()
        
        print(f"Успешно загружено {len(data_to_insert)} записей в raw.energy_consumption")
        
        cursor.close()
        conn.close()
        
    finally:
        spark.stop()
    
    print("EXTRACT: Завершено успешно!")


def run_transform():
    """Transform: Агрегация и обработка данных"""
    import psycopg2
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, sum as spark_sum, avg, max as spark_max, min as spark_min,
        countDistinct, date_trunc, when, abs as spark_abs, stddev, lit
    )
    
    print("=" * 50)
    print("TRANSFORM: Начало обработки данных")
    print("=" * 50)
    
    # Создание SparkSession
    spark = SparkSession.builder \
        .appName("EnergyAnalytics-Transform") \
        .master("local[*]") \
        .config("spark.driver.memory", "1g") \
        .config("spark.jars", "/home/airflow/.ivy2/jars/postgresql-42.7.1.jar") \
        .getOrCreate()
    
    try:
        # Загрузка данных из PostgreSQL
        jdbc_url = f"jdbc:postgresql://{PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['database']}"
        
        print(f"Чтение данных из PostgreSQL: {jdbc_url}")
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "raw.energy_consumption") \
            .option("user", PG_CONFIG['user']) \
            .option("password", PG_CONFIG['password']) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        row_count = df.count()
        print(f"Загружено строк: {row_count}")
        
        if row_count == 0:
            raise ValueError("Таблица raw.energy_consumption пуста!")
        
        # Очистка данных
        df_cleaned = df.filter(
            col("lclid").isNotNull() &
            col("day").isNotNull() &
            col("energy_kwh").isNotNull() &
            (col("energy_kwh") >= 0) &
            (col("energy_kwh") <= 100)
        )
        
        print(f"После очистки: {df_cleaned.count()} строк")
        
        # Агрегация по дням
        print("Агрегация по дням...")
        daily = df_cleaned.groupBy("day").agg(
            spark_sum("energy_kwh").alias("total_consumption"),
            avg("energy_kwh").alias("avg_consumption"),
            spark_max("energy_kwh").alias("max_consumption"),
            spark_min("energy_kwh").alias("min_consumption"),
            countDistinct("lclid").alias("household_count")
        )
        
        # Агрегация по часам
        print("Агрегация по часам...")
        hourly = df_cleaned.filter(col("timestamp").isNotNull()).groupBy(
            date_trunc("hour", col("timestamp")).alias("hour")
        ).agg(
            spark_sum("energy_kwh").alias("total_consumption"),
            avg("energy_kwh").alias("avg_consumption"),
            spark_max("energy_kwh").alias("max_consumption"),
            spark_min("energy_kwh").alias("min_consumption"),
            countDistinct("lclid").alias("household_count")
        )
        
        # Агрегация по неделям
        print("Агрегация по неделям...")
        weekly = df_cleaned.withColumn(
            "week_start", date_trunc("week", col("day"))
        ).groupBy("week_start").agg(
            spark_sum("energy_kwh").alias("total_consumption"),
            avg("energy_kwh").alias("avg_consumption"),
            spark_max("energy_kwh").alias("max_consumption"),
            spark_min("energy_kwh").alias("min_consumption"),
            countDistinct("lclid").alias("household_count")
        )
        
        # Агрегация по месяцам
        print("Агрегация по месяцам...")
        monthly = df_cleaned.withColumn(
            "month", date_trunc("month", col("day"))
        ).groupBy("month").agg(
            spark_sum("energy_kwh").alias("total_consumption"),
            avg("energy_kwh").alias("avg_consumption"),
            spark_max("energy_kwh").alias("max_consumption"),
            spark_min("energy_kwh").alias("min_consumption"),
            countDistinct("lclid").alias("household_count")
        )
        
        # Статистика по домохозяйствам
        print("Статистика по домохозяйствам...")
        household_stats = df_cleaned.groupBy("lclid").agg(
            spark_sum("energy_kwh").alias("total_consumption"),
            avg("energy_kwh").alias("avg_daily_consumption"),
            spark_max("energy_kwh").alias("max_daily_consumption"),
            spark_min("energy_kwh").alias("min_daily_consumption"),
            countDistinct("day").alias("days_count")
        )
        
        # Выявление аномалий (Z-score > 3)
        print("Выявление аномалий...")
        stats = df_cleaned.groupBy("lclid").agg(
            avg("energy_kwh").alias("mean_consumption"),
            stddev("energy_kwh").alias("stddev_consumption")
        )
        
        df_with_stats = df_cleaned.join(stats, on="lclid", how="left")
        df_with_stats = df_with_stats.withColumn(
            "z_score",
            when(col("stddev_consumption") > 0,
                 spark_abs((col("energy_kwh") - col("mean_consumption")) / col("stddev_consumption"))
            ).otherwise(lit(0))
        )
        
        anomalies = df_with_stats.filter(col("z_score") > 3).select(
            col("lclid"),
            col("timestamp"),
            col("energy_kwh"),
            col("z_score"),
            lit("outlier").alias("anomaly_type")
        )
        
        print(f"Найдено аномалий: {anomalies.count()}")
        
        # Сохранение в Parquet
        import os
        os.makedirs(PROCESSED_PATH, exist_ok=True)
        
        aggregated = {
            "daily": daily,
            "hourly": hourly,
            "weekly": weekly,
            "monthly": monthly,
            "household_stats": household_stats,
            "anomalies": anomalies
        }
        
        for name, data in aggregated.items():
            output_path = f"{PROCESSED_PATH}/{name}"
            count = data.count()
            if count > 0:
                data.coalesce(1).write.mode("overwrite").parquet(output_path)
                print(f"Сохранено: {name} -> {output_path} ({count} строк)")
            else:
                print(f"Пропущено: {name} (0 строк)")
        
    finally:
        spark.stop()
    
    print("TRANSFORM: Завершено успешно!")


def run_load():
    """Load: Загрузка агрегатов в PostgreSQL DWH"""
    import os
    import psycopg2
    from psycopg2.extras import execute_values
    from pyspark.sql import SparkSession
    
    print("=" * 50)
    print("LOAD: Загрузка в DWH")
    print("=" * 50)
    
    # Создание SparkSession
    spark = SparkSession.builder \
        .appName("EnergyAnalytics-Load") \
        .master("local[*]") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()
    
    # Маппинг файлов на таблицы
    table_mapping = {
        "daily": ("analytics.daily_consumption", ["day", "total_consumption", "avg_consumption", "max_consumption", "min_consumption", "household_count"]),
        "hourly": ("analytics.hourly_consumption", ["hour", "total_consumption", "avg_consumption", "max_consumption", "min_consumption", "household_count"]),
        "weekly": ("analytics.weekly_consumption", ["week_start", "total_consumption", "avg_consumption", "max_consumption", "min_consumption", "household_count"]),
        "monthly": ("analytics.monthly_consumption", ["month", "total_consumption", "avg_consumption", "max_consumption", "min_consumption", "household_count"]),
        "household_stats": ("analytics.household_stats", ["lclid", "total_consumption", "avg_daily_consumption", "max_daily_consumption", "min_daily_consumption", "days_count"]),
        "anomalies": ("analytics.anomalies", ["lclid", "timestamp", "energy_kwh", "z_score", "anomaly_type"]),
    }
    
    try:
        conn = psycopg2.connect(
            host=PG_CONFIG['host'],
            port=PG_CONFIG['port'],
            database=PG_CONFIG['database'],
            user=PG_CONFIG['user'],
            password=PG_CONFIG['password']
        )
        cursor = conn.cursor()
        
        for name, (table_name, columns) in table_mapping.items():
            parquet_path = f"{PROCESSED_PATH}/{name}"
            
            if not os.path.exists(parquet_path):
                print(f"Пропускаем {name}: директория не существует")
                continue
            
            print(f"Загрузка {name} -> {table_name}")
            
            try:
                df = spark.read.parquet(parquet_path)
                rows = df.collect()
                
                if not rows:
                    print(f"  Нет данных для загрузки")
                    continue
                
                # Очистка таблицы
                cursor.execute(f"TRUNCATE TABLE {table_name}")
                
                # Подготовка данных
                data_to_insert = []
                for row in rows:
                    row_data = []
                    for col_name in columns:
                        val = getattr(row, col_name, None)
                        if val is not None and hasattr(val, 'item'):
                            val = val.item()  # Convert numpy types
                        row_data.append(val)
                    data_to_insert.append(tuple(row_data))
                
                # Вставка
                columns_str = ", ".join(columns)
                placeholders = ", ".join(["%s"] * len(columns))
                insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES %s"
                
                execute_values(cursor, insert_query, data_to_insert, page_size=1000)
                conn.commit()
                
                print(f"  Загружено {len(data_to_insert)} записей")
                
            except Exception as e:
                print(f"  Ошибка при загрузке {name}: {e}")
                conn.rollback()
        
        cursor.close()
        conn.close()
        
    finally:
        spark.stop()
    
    print("LOAD: Завершено успешно!")


# Определение задач
check_data_task = PythonOperator(
    task_id='check_data_availability',
    python_callable=check_data_availability,
    dag=dag,
)

check_postgres_task = PythonOperator(
    task_id='check_postgres_connection',
    python_callable=check_postgres_connection,
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract',
    python_callable=run_extract,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=run_transform,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=run_load,
    dag=dag,
)

# Определение зависимостей
[check_data_task, check_postgres_task] >> extract_task >> transform_task >> load_task
