# Data Engineer Portfolio 👋

## 📦 ETL Pipeline with PostgreSQL

Простой ETL-пайплайн на Python, который:
- Читает данные о сотрудниках
- Валидирует и обрабатывает их
- Загружает в PostgreSQL (в Docker)

### 🚀 Быстрый старт

```bash
# 1. Запусти PostgreSQL в Docker
docker run --name postgres-de -e POSTGRES_PASSWORD=postgres123 -p 5432:5432 -d postgres:15

# 2. Установи зависимости
pip install psycopg2-binary

# 3. Запусти пайплайн
python etl_pipeline.py
