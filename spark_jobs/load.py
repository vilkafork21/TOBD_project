"""
Load Task: Загрузка агрегированных данных в PostgreSQL DWH (analytics schema)
"""
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
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

def load_parquet_to_postgres(spark, parquet_path, table_name, schema_name="analytics"):
    """
    Загрузка данных из Parquet в PostgreSQL
    
    Args:
        spark: SparkSession
        parquet_path: путь к Parquet файлам
        table_name: имя таблицы
        schema_name: имя схемы
    """
    print(f"Загрузка данных из {parquet_path} в {schema_name}.{table_name}")
    
    try:
        # Чтение Parquet файлов
        df = spark.read.parquet(parquet_path)
        
        # Получение данных
        rows = df.collect()
        
        if not rows:
            print(f"Нет данных для загрузки в {table_name}")
            return
        
        # Подключение к PostgreSQL
        conn = get_postgres_connection()
        cursor = conn.cursor()
        
        try:
            # Очистка таблицы перед загрузкой (опционально)
            truncate_query = f"TRUNCATE TABLE {schema_name}.{table_name}"
            cursor.execute(truncate_query)
            
            # Подготовка данных для вставки
            data_to_insert = []
            for row in rows:
                row_dict = row.asDict()
                # Преобразуем значения в tuple в правильном порядке
                values = tuple(row_dict.values())
                data_to_insert.append(values)
            
            # Определение колонок для вставки
            columns = df.columns
            columns_str = ", ".join(columns)
            placeholders = ", ".join(["%s"] * len(columns))
            
            # Вставка данных
            insert_query = f"""
                INSERT INTO {schema_name}.{table_name} ({columns_str})
                VALUES ({placeholders})
                ON CONFLICT DO NOTHING
            """
            
            execute_values(cursor, insert_query, data_to_insert, page_size=1000)
            conn.commit()
            
            print(f"Успешно загружено {len(data_to_insert)} записей в {schema_name}.{table_name}")
            
        except Exception as e:
            conn.rollback()
            print(f"Ошибка при загрузке в PostgreSQL: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        print(f"Ошибка при чтении Parquet файлов: {e}")
        raise

def load_hourly_data(spark, processed_path):
    """Загрузка почасовых данных"""
    parquet_path = f"{processed_path}/hourly"
    load_parquet_to_postgres(spark, parquet_path, "hourly_consumption")

def load_daily_data(spark, processed_path):
    """Загрузка дневных данных"""
    parquet_path = f"{processed_path}/daily"
    load_parquet_to_postgres(spark, parquet_path, "daily_consumption")

def load_weekly_data(spark, processed_path):
    """Загрузка недельных данных"""
    parquet_path = f"{processed_path}/weekly"
    load_parquet_to_postgres(spark, parquet_path, "weekly_consumption")

def load_monthly_data(spark, processed_path):
    """Загрузка месячных данных"""
    parquet_path = f"{processed_path}/monthly"
    load_parquet_to_postgres(spark, parquet_path, "monthly_consumption")

def load_household_stats(spark, processed_path):
    """Загрузка статистики по домохозяйствам"""
    parquet_path = f"{processed_path}/household_stats"
    load_parquet_to_postgres(spark, parquet_path, "household_stats")

def load_anomalies(spark, processed_path):
    """Загрузка данных об аномалиях"""
    parquet_path = f"{processed_path}/anomalies"
    load_parquet_to_postgres(spark, parquet_path, "anomalies")

def create_indexes():
    """Создание индексов для оптимизации запросов"""
    print("Создание индексов...")
    
    conn = get_postgres_connection()
    cursor = conn.cursor()
    
    try:
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_hourly_hour ON analytics.hourly_consumption(hour)",
            "CREATE INDEX IF NOT EXISTS idx_daily_day ON analytics.daily_consumption(day)",
            "CREATE INDEX IF NOT EXISTS idx_weekly_week_start ON analytics.weekly_consumption(week_start)",
            "CREATE INDEX IF NOT EXISTS idx_monthly_month ON analytics.monthly_consumption(month)",
            "CREATE INDEX IF NOT EXISTS idx_household_lclid ON analytics.household_stats(lclid)",
            "CREATE INDEX IF NOT EXISTS idx_anomalies_timestamp ON analytics.anomalies(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_anomalies_lclid ON analytics.anomalies(lclid)"
        ]
        
        for index_query in indexes:
            cursor.execute(index_query)
        
        conn.commit()
        print("Индексы созданы успешно")
        
    except Exception as e:
        conn.rollback()
        print(f"Ошибка при создании индексов: {e}")
    finally:
        cursor.close()
        conn.close()

def main():
    """Главная функция Load задачи"""
    # Путь к обработанным данным
    processed_path = sys.argv[1] if len(sys.argv) > 1 else "/opt/spark/data/processed"
    
    # Создание SparkSession
    spark = SparkSession.builder \
        .appName("EnergyAnalytics-Load") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    try:
        # Загрузка всех агрегированных данных
        print("Начало загрузки данных в DWH...")
        
        load_hourly_data(spark, processed_path)
        load_daily_data(spark, processed_path)
        load_weekly_data(spark, processed_path)
        load_monthly_data(spark, processed_path)
        load_household_stats(spark, processed_path)
        load_anomalies(spark, processed_path)
        
        # Создание индексов
        create_indexes()
        
        print("Load задача выполнена успешно!")
        
    except Exception as e:
        print(f"Ошибка в Load задаче: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

