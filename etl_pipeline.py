import psycopg2
from psycopg2 import Error

# Параметры подключения
DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'database': 'etl_db',
    'user': 'postgres',
    'password': 'postgres123'
}


def create_connection():
    """Создает подключение к базе данных"""
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        print("✅ Успешное подключение к PostgreSQL!")
        return connection
    except Error as e:
        print(f"❌ Ошибка подключения: {e}")
        return None


def create_table(connection):
    """Создает таблицу employees"""
    try:
        cursor = connection.cursor()

        create_table_query = '''
        CREATE TABLE IF NOT EXISTS employees (
            id SERIAL PRIMARY KEY, 
            name VARCHAR(100) NOT NULL,
            salary INTEGER,
            department VARCHAR(50)
        );
        '''

        cursor.execute(create_table_query)
        connection.commit()
        print("✅ Таблица employees создана!")

        cursor.close()
    except Error as e:
        print(f"❌ Ошибка создания таблицы: {e}")


def insert_employees(connection, employees_data):
    """Вставляет данные о сотрудниках"""
    try:
        cursor = connection.cursor()

        insert_query = '''
        INSERT INTO employees (name, salary, department)
        VALUES (%s, %s, %s) 
        '''

        cursor.executemany(insert_query, employees_data)
        connection.commit()

        print(f"✅ Вставлено {cursor.rowcount} записей!")
        cursor.close()

    except Error as e:
        print(f"❌ Ошибка вставки данных: {e}")
        connection.rollback()


def select_employees(connection):
    """Выбирает и выводит всех сотрудников"""
    try:
        cursor = connection.cursor()

        select_query = "SELECT * FROM employees ORDER BY id"
        cursor.execute(select_query)

        employees = cursor.fetchall()

        print("\n📊 Данные в таблице employees:")
        print("-" * 50)
        print(f"{'ID':<5} {'Имя':<15} {'Зарплата':<10} {'Отдел':<15}")
        print("-" * 50)

        for emp in employees:
            print(f"{emp[0]:<5} {emp[1]:<15} {emp[2]:<10} {emp[3]:<15}")

        print("-" * 50)
        cursor.close()

    except Error as e:
        print(f"❌ Ошибка выборки данных: {e}")


def main():
    # Данные для вставки (как в наших предыдущих заданиях)
    employees_data = [
        ('Alex', 40000, 'IT'),
        ('Maria', 60000, 'Marketing'),
        ('John', 70000, 'IT'),
        ('Olga', 55000, 'HR')
    ]

    # Основной пайплайн
    connection = create_connection()

    if connection:
        create_table(connection)
        insert_employees(connection, employees_data)
        select_employees(connection)

        connection.close()
        print("\n✅ ETL-пайплайн завершен успешно!")


if __name__ == "__main__":
    main()
