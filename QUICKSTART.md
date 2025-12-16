# Быстрый старт

## Шаг 1: Установка Docker Desktop

Скачайте и установите Docker Desktop для macOS:
https://www.docker.com/products/docker-desktop/

## Шаг 2: Подготовка данных

### Вариант A: Использование тестовых данных

В проекте уже есть файл `data/raw/sample_data.csv` с тестовыми данными. Можно использовать его для тестирования.

### Вариант B: Загрузка реального датасета

1. Зарегистрируйтесь на [Kaggle](https://www.kaggle.com/)
2. Скачайте датасет: [Smart meters in London](https://www.kaggle.com/datasets/jeanmidev/smart-meters-in-london)
3. Распакуйте CSV файлы в директорию `data/raw/`

## Шаг 3: Запуск проекта

```bash
# Перейдите в директорию проекта
cd /Users/antonzyukov/Desktop/Tobd_project

# Запустите все сервисы
docker-compose up -d

# Проверьте статус
docker-compose ps
```

Ожидайте 2-3 минуты, пока все сервисы запустятся.

## Шаг 4: Доступ к интерфейсам

### Airflow (http://localhost:8081)
- Логин: `airflow`
- Пароль: `airflow`

### Streamlit Dashboard (http://localhost:8501)
- Открывается автоматически

### Spark Master UI (http://localhost:8080)
- Мониторинг Spark кластера

## Шаг 5: Запуск ETL пайплайна

1. Откройте Airflow: http://localhost:8081
2. Найдите DAG `energy_analytics_etl`
3. Включите DAG (переключите тумблер)
4. Нажмите кнопку "Play" для запуска

## Шаг 6: Просмотр результатов

Откройте Streamlit Dashboard: http://localhost:8501

## Остановка проекта

```bash
docker-compose down
```

Для полной очистки (включая данные):
```bash
docker-compose down -v
```

## Решение проблем

### Проблема: Порты заняты

Если порты 8080, 8081, 8501 или 5432 уже заняты, измените их в `docker-compose.yml`

### Проблема: Airflow не запускается

```bash
# Проверьте логи
docker-compose logs airflow-init

# Пересоздайте контейнеры
docker-compose down -v
docker-compose up -d
```

### Проблема: Нет данных в дашборде

Убедитесь, что:
1. ETL пайплайн выполнен успешно в Airflow
2. Данные загружены в БД (проверьте через Airflow логи)

