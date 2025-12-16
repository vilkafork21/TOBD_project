

"""
Transform Task: Очистка данных, агрегация и выявление аномалий с использованием Spark
"""
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, avg, max as spark_max, min as spark_min,
    count, countDistinct, date_trunc, window, when, abs,
    stddev, mean, lit, hour, dayofweek, month, year
)
from pyspark.sql.types import DoubleType
import psycopg2

def get_postgres_connection():
    """Создать подключение к PostgreSQL"""
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=os.getenv('POSTGRES_PORT', '5432'),
        database=os.getenv('POSTGRES_DB', 'energy_analytics'),
        user=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD', 'postgres')
    )

def load_raw_data(spark):
    """Загрузка сырых данных из PostgreSQL"""
    print("Загрузка данных из PostgreSQL...")
    
    # Читаем данные из PostgreSQL через JDBC
    df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'postgres')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'energy_analytics')}") \
        .option("dbtable", "raw.energy_consumption") \
        .option("user", os.getenv('POSTGRES_USER', 'postgres')) \
        .option("password", os.getenv('POSTGRES_PASSWORD', 'postgres')) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    
    print(f"Загружено строк: {df.count()}")
    return df

def clean_data(df):
    """
    Очистка данных: обработка пропусков и выбросов
    
    Args:
        df: Spark DataFrame с сырыми данными
    
    Returns:
        Очищенный DataFrame
    """
    print("Очистка данных...")
    
    # Удаление строк с null значениями
    df_cleaned = df.filter(
        col("lclid").isNotNull() &
        col("day").isNotNull() &
        col("energy_kwh").isNotNull() &
        col("timestamp").isNotNull()
    )
    
    # Удаление отрицательных значений потребления
    df_cleaned = df_cleaned.filter(col("energy_kwh") >= 0)
    
    # Удаление экстремальных выбросов (потребление > 100 кВтч за час - физически невозможно)
    df_cleaned = df_cleaned.filter(col("energy_kwh") <= 100)
    
    print(f"После очистки строк: {df_cleaned.count()}")
    return df_cleaned

def detect_anomalies(df):
    """
    Выявление аномалий методом Z-score (3 sigma rule)
    
    Args:
        df: Spark DataFrame
    
    Returns:
        DataFrame с аномалиями
    """
    print("Выявление аномалий...")
    
    # Вычисление статистики по каждому домохозяйству
    stats = df.groupBy("lclid").agg(
        avg("energy_kwh").alias("mean_consumption"),
        stddev("energy_kwh").alias("stddev_consumption")
    )
    
    # Объединение с исходными данными
    df_with_stats = df.join(stats, on="lclid", how="left")
    
    # Вычисление Z-score
    df_with_stats = df_with_stats.withColumn(
        "z_score",
        when(col("stddev_consumption") > 0,
             abs((col("energy_kwh") - col("mean_consumption")) / col("stddev_consumption"))
        ).otherwise(lit(0))
    )
    
    # Выделение аномалий (Z-score > 3)
    anomalies = df_with_stats.filter(col("z_score") > 3).select(
        col("lclid"),
        col("timestamp"),
        col("energy_kwh"),
        col("z_score"),
        lit("outlier").alias("anomaly_type")
    )
    
    anomaly_count = anomalies.count()
    print(f"Найдено аномалий: {anomaly_count}")
    
    return anomalies

def aggregate_hourly(df):
    """Агрегация данных по часам"""
    print("Агрегация по часам...")
    
    hourly = df.groupBy(
        date_trunc("hour", col("timestamp")).alias("hour")
    ).agg(
        spark_sum("energy_kwh").alias("total_consumption"),
        avg("energy_kwh").alias("avg_consumption"),
        spark_max("energy_kwh").alias("max_consumption"),
        spark_min("energy_kwh").alias("min_consumption"),
        countDistinct("lclid").alias("household_count")
    )
    
    return hourly

def aggregate_daily(df):
    """Агрегация данных по дням"""
    print("Агрегация по дням...")
    
    daily = df.groupBy("day").agg(
        spark_sum("energy_kwh").alias("total_consumption"),
        avg("energy_kwh").alias("avg_consumption"),
        spark_max("energy_kwh").alias("max_consumption"),
        spark_min("energy_kwh").alias("min_consumption"),
        countDistinct("lclid").alias("household_count")
    )
    
    return daily

def aggregate_weekly(df):
    """Агрегация данных по неделям"""
    print("Агрегация по неделям...")
    
    # Добавляем колонку с началом недели (понедельник)
    df_with_week = df.withColumn(
        "week_start",
        date_trunc("week", col("day"))
    )
    
    weekly = df_with_week.groupBy("week_start").agg(
        spark_sum("energy_kwh").alias("total_consumption"),
        avg("energy_kwh").alias("avg_consumption"),
        spark_max("energy_kwh").alias("max_consumption"),
        spark_min("energy_kwh").alias("min_consumption"),
        countDistinct("lclid").alias("household_count")
    )
    
    return weekly

def aggregate_monthly(df):
    """Агрегация данных по месяцам"""
    print("Агрегация по месяцам...")
    
    # Добавляем колонку с началом месяца
    df_with_month = df.withColumn(
        "month",
        date_trunc("month", col("day"))
    )
    
    monthly = df_with_month.groupBy("month").agg(
        spark_sum("energy_kwh").alias("total_consumption"),
        avg("energy_kwh").alias("avg_consumption"),
        spark_max("energy_kwh").alias("max_consumption"),
        spark_min("energy_kwh").alias("min_consumption"),
        countDistinct("lclid").alias("household_count")
    )
    
    return monthly

def aggregate_household_stats(df):
    """Статистика по каждому домохозяйству"""
    print("Агрегация статистики по домохозяйствам...")
    
    household_stats = df.groupBy("lclid").agg(
        spark_sum("energy_kwh").alias("total_consumption"),
        avg("energy_kwh").alias("avg_daily_consumption"),
        spark_max("energy_kwh").alias("max_daily_consumption"),
        spark_min("energy_kwh").alias("min_daily_consumption"),
        countDistinct("day").alias("days_count")
    )
    
    return household_stats

def transform_data(spark):
    """
    Главная функция трансформации данных
    
    Args:
        spark: SparkSession
    
    Returns:
        Словарь с агрегированными данными
    """
    # Загрузка сырых данных
    df_raw = load_raw_data(spark)
    
    # Очистка данных
    df_cleaned = clean_data(df_raw)
    
    # Выявление аномалий
    anomalies = detect_anomalies(df_cleaned)
    
    # Агрегация по различным временным периодам
    hourly_data = aggregate_hourly(df_cleaned)
    daily_data = aggregate_daily(df_cleaned)
    weekly_data = aggregate_weekly(df_cleaned)
    monthly_data = aggregate_monthly(df_cleaned)
    household_stats = aggregate_household_stats(df_cleaned)
    
    return {
        "hourly": hourly_data,
        "daily": daily_data,
        "weekly": weekly_data,
        "monthly": monthly_data,
        "household_stats": household_stats,
        "anomalies": anomalies
    }

def main():
    """Главная функция Transform задачи"""
    # Создание SparkSession
    spark = SparkSession.builder \
        .appName("EnergyAnalytics-Transform") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1") \
        .getOrCreate()
    
    try:
        # Трансформация данных
        aggregated_data = transform_data(spark)
        
        # Сохранение результатов во временные таблицы для последующей загрузки
        print("Сохранение результатов трансформации...")
        processed_base_path = "/opt/spark/data/processed"
        import os
        os.makedirs(processed_base_path, exist_ok=True)
        
        for key, df in aggregated_data.items():
            output_path = f"{processed_base_path}/{key}"
            df.coalesce(1).write.mode("overwrite").parquet(output_path)
            print(f"Сохранено: {key} -> {output_path}")
        
        print("Transform задача выполнена успешно!")
        
    except Exception as e:
        print(f"Ошибка в Transform задаче: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

