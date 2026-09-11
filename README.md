Full-Stack Data Pipeline: CDC & ETL Workflow

Этот проект представляет собой комплексный учебный пайплайн обработки данных, реализующий архитектуру Modern Data Stack. В проекте настроен процесс переноса данных из транзакционных источников (MongoDB, PostgreSQL) в аналитическое хранилище (ClickHouse) с использованием промежуточного слоя S3 (MinIO).
Архитектура и поток данных

Пайплайн разделен на три ключевых этапа (DAG), которые обеспечивают автоматизацию и отказоустойчивость:

    Extract (L1):
        Извлечение сырых событий из MongoDB инкрементально (отсечка по timestamp хранится в Redis).
        Сохранение данных в формате Parquet (сжатие LZ4) и загрузка в S3 (MinIO).
        Использование Telegram API для уведомлений о статусе выполнения (успех, ошибка, пропуск).

    Infrastructure & CDC (L2):
        Автоматическое создание схемы данных в ClickHouse.
        Настройка Kafka Engine таблиц для приема данных через Debezium (CDC из PostgreSQL).
        Создание Materialized Views для автоматической трансформации данных «на лету» (обработка операций Insert/Update/Delete).
        Использование движков ReplacingMergeTree для хранения актуальных состояний и AggregatingMergeTree для витрин данных.

    Transform & Load (L3):
        Сложная логика ветвления (BranchPythonOperator через TaskFlow API).
        Обогащение данных (Join) фактов продаж с измерениями (товары и категории).
        Обновление кэша в Redis для ускорения трансформаций.
        Загрузка финальных витрин (fact_sales) в ClickHouse для аналитики.

 Технологический стек

    Orchestration: Apache Airflow (TaskFlow API)
    Data Warehouse: ClickHouse (MergeTree, Kafka Engine, Materialized Views)
    Data Lake: S3 (MinIO)
    Databases: MongoDB, PostgreSQL, Redis
    CDC (Change Data Capture): Debezium (Kafka)
    Python Libraries: Pandas, PyArrow (обработка Parquet)
    Monitoring: Telegram Bot (Callbacks)

 Описание DAG-файлов

    create_table_dag.py: “Инфраструктурный” слой. Создает таблицы, движки S3/Kafka в ClickHouse и материализованные представления.
    extract_from_mongo_dag.py: Инкрементальный экспорт событий из NoSQL. После успешного завершения триггерит следующий этап.
    transform_data_dag.py: “Мозг” пайплайна. Управляет зависимостями, проверяет наличие таблиц, обновляет файлы в S3 и выполняет финальную загрузку в DWH.

 Особенности реализации

    Идемпотентность: Пайплайн проверяет наличие данных и состояние таблиц перед запуском.
    CDC Logic: Реализована ручная обработка изменений (op — ‘c’, ‘u’, ‘d’) для синхронизации данных в S3.
    Уведомления: Кастомные коллбэки (on_failure_callback и др.) отправляют отчеты в Telegram.
    Оптимизация: Использование Parquet с LZ4 и кэширование в Redis снижает нагрузку на дисковую подсистему и сеть.
