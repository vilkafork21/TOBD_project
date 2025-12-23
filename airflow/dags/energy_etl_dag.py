"""
Airflow DAG для ETL пайплайна энергетической аналитики
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os

# Параметры по умолчанию
default_args = {
    'owner': 'energy_analytics',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Определение DAG
dag = DAG(
    'energy_analytics_etl',
    default_args=default_args,
    description='ETL пайплайн для анализа потребления электроэнергии',
    schedule_interval=timedelta(days=1),  # Запуск раз в день
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['energy', 'etl', 'spark', 'postgres'],
)

# Пути к данным и скриптам
DATA_PATH = "/opt/airflow/data/raw"
PROCESSED_PATH = "/opt/airflow/data/processed"
SPARK_JOBS_PATH = "/opt/airflow/spark_jobs"
SPARK_MASTER = "spark://spark-master:7077"

# Переменные окружения для Spark
spark_env = {
    'POSTGRES_HOST': os.getenv('POSTGRES_HOST', 'postgres'),
    'POSTGRES_PORT': os.getenv('POSTGRES_PORT', '5432'),
    'POSTGRES_DB': os.getenv('POSTGRES_DB', 'energy_analytics'),
    'POSTGRES_USER': os.getenv('POSTGRES_USER', 'postgres'),
    'POSTGRES_PASSWORD': os.getenv('POSTGRES_PASSWORD', 'postgres'),
}

def check_data_availability():
    """Проверка наличия данных"""
    import glob
    csv_files = glob.glob(f"{DATA_PATH}/*.csv")
    if not csv_files:
        raise FileNotFoundError(f"Не найдены CSV файлы в {DATA_PATH}")
    print(f"Найдено CSV файлов: {len(csv_files)}")
    return csv_files

# Task 1: Extract - Извлечение данных
extract_task = BashOperator(
    task_id='extract',
    bash_command=f"""
    docker exec energy_spark_master bash -c "
    export POSTGRES_HOST={spark_env['POSTGRES_HOST']}
    export POSTGRES_PORT={spark_env['POSTGRES_PORT']}
    export POSTGRES_DB={spark_env['POSTGRES_DB']}
    export POSTGRES_USER={spark_env['POSTGRES_USER']}
    export POSTGRES_PASSWORD={spark_env['POSTGRES_PASSWORD']}
    
    /opt/spark/bin/spark-submit \
        --master {SPARK_MASTER} \
        --packages org.postgresql:postgresql:42.7.1 \
        /opt/spark/app/extract.py /opt/spark/data/raw
    "
    """,
    dag=dag,
)

# Task 2: Transform - Трансформация данных
transform_task = BashOperator(
    task_id='transform',
    bash_command=f"""
    docker exec energy_spark_master bash -c "
    export POSTGRES_HOST={spark_env['POSTGRES_HOST']}
    export POSTGRES_PORT={spark_env['POSTGRES_PORT']}
    export POSTGRES_DB={spark_env['POSTGRES_DB']}
    export POSTGRES_USER={spark_env['POSTGRES_USER']}
    export POSTGRES_PASSWORD={spark_env['POSTGRES_PASSWORD']}
    
    /opt/spark/bin/spark-submit \
        --master {SPARK_MASTER} \
        --packages org.postgresql:postgresql:42.7.1 \
        /opt/spark/app/transform.py
    "
    """,
    dag=dag,
)

# Task 3: Load - Загрузка данных в DWH
load_task = BashOperator(
    task_id='load',
    bash_command=f"""
    docker exec energy_spark_master bash -c "
    export POSTGRES_HOST={spark_env['POSTGRES_HOST']}
    export POSTGRES_PORT={spark_env['POSTGRES_PORT']}
    export POSTGRES_DB={spark_env['POSTGRES_DB']}
    export POSTGRES_USER={spark_env['POSTGRES_USER']}
    export POSTGRES_PASSWORD={spark_env['POSTGRES_PASSWORD']}
    
    /opt/spark/bin/spark-submit \
        --master {SPARK_MASTER} \
        --packages org.postgresql:postgresql:42.7.1 \
        /opt/spark/app/load.py /opt/spark/data/processed
    "
    """,
    dag=dag,
)

# Проверка наличия данных (опционально)
check_data_task = PythonOperator(
    task_id='check_data_availability',
    python_callable=check_data_availability,
    dag=dag,
)

# Определение зависимостей между задачами
check_data_task >> extract_task >> transform_task >> load_task

