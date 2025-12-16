"""
Extract Task: Загрузка CSV файлов в PostgreSQL (raw schema)
"""
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, DateType
from pyspark.sql.functions import col, to_timestamp, to_date, when, isnan, isnull
import psycopg2
from psycopg2.extras import execute_values

def get_postgres_connection():
    """Создать подключение к PostgreSQL"""
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=os.getenv('POSTGRES_PORT', '5432'),
        database=os.getenv('POSTGRES_DB', 'energy_analytics'),
        user=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD', 'postgres')
    )

def validate_csv_structure(df):
    """Валидация структуры CSV файла"""
    required_columns = ['LCLid', 'day', 'energy_kwh']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        raise ValueError(f"Отсутствуют обязательные колонки: {missing_columns}")
    
    return True

def extract_data(spark, data_path):
    """
    Извлечение данных из CSV файлов
    
    Args:
        spark: SparkSession
        data_path: путь к директории с CSV файлами
    
    Returns:
        DataFrame с загруженными данными
    """
    print(f"Загрузка данных из: {data_path}")
    
    # Определение схемы данных
    schema = StructType([
        StructField("LCLid", StringType(), True),
        StructField("day", DateType(), True),
        StructField("energy_kwh", DoubleType(), True),
        StructField("timestamp", TimestampType(), True)
    ])
    
    # Загрузка CSV файлов
    try:
        # Попытка загрузить с указанной схемой
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .schema(schema) \
            .csv(f"{data_path}/*.csv")
    except Exception as e:
        print(f"Ошибка при загрузке с схемой, пробуем без схемы: {e}")
        # Если не получилось, загружаем без схемы и преобразуем
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(f"{data_path}/*.csv")
        
        # Преобразование колонок
        if "day" in df.columns:
            df = df.withColumn("day", to_date(col("day"), "yyyy-MM-dd"))
        if "timestamp" in df.columns:
            df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
        if "energy_kwh" in df.columns:
            df = df.withColumn("energy_kwh", col("energy_kwh").cast("double"))
    
    # Валидация структуры
    validate_csv_structure(df)
    
    # Очистка данных: удаление строк с null значениями в ключевых полях
    df_cleaned = df.filter(
        col("LCLid").isNotNull() &
        col("day").isNotNull() &
        col("energy_kwh").isNotNull()
    )
    
    # Удаление дубликатов
    df_cleaned = df_cleaned.dropDuplicates(["LCLid", "day", "timestamp"])
    
    print(f"Загружено строк: {df_cleaned.count()}")
    print(f"Уникальных домохозяйств: {df_cleaned.select('LCLid').distinct().count()}")
    
    return df_cleaned

def load_to_postgres(df, table_name="raw.energy_consumption"):
    """
    Загрузка данных в PostgreSQL
    
    Args:
        df: Spark DataFrame
        table_name: имя таблицы для загрузки
    """
    print(f"Загрузка данных в таблицу: {table_name}")
    
    # Получаем данные из Spark DataFrame
    rows = df.select("LCLid", "day", "energy_kwh", "timestamp").collect()
    
    if not rows:
        print("Нет данных для загрузки")
        return
    
    # Подключение к PostgreSQL
    conn = get_postgres_connection()
    cursor = conn.cursor()
    
    try:
        # Подготовка данных для вставки
        data_to_insert = []
        for row in rows:
            data_to_insert.append((
                row.LCLid,
                row.day,
                float(row.energy_kwh) if row.energy_kwh is not None else None,
                row.timestamp
            ))
        
        # Вставка данных (batch insert)
        insert_query = f"""
            INSERT INTO {table_name} (lclid, day, energy_kwh, timestamp)
            VALUES %s
            ON CONFLICT DO NOTHING
        """
        
        execute_values(cursor, insert_query, data_to_insert, page_size=1000)
        conn.commit()
        
        print(f"Успешно загружено {len(data_to_insert)} записей в {table_name}")
        
    except Exception as e:
        conn.rollback()
        print(f"Ошибка при загрузке в PostgreSQL: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def main():
    """Главная функция Extract задачи"""
    # Путь к данным (может быть передан как аргумент)
    data_path = sys.argv[1] if len(sys.argv) > 1 else "/opt/spark/data/raw"
    
    # Проверка существования директории
    import os
    if not os.path.exists(data_path):
        print(f"Предупреждение: директория {data_path} не существует")
        # Попробуем альтернативный путь
        data_path = "/opt/airflow/data/raw"
        if not os.path.exists(data_path):
            print(f"Ошибка: директория {data_path} также не существует")
            sys.exit(1)
    
    # Создание SparkSession
    spark = SparkSession.builder \
        .appName("EnergyAnalytics-Extract") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    try:
        # Извлечение данных
        df = extract_data(spark, data_path)
        
        # Загрузка в PostgreSQL
        load_to_postgres(df)
        
        print("Extract задача выполнена успешно!")
        
    except Exception as e:
        print(f"Ошибка в Extract задаче: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

