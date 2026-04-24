import sqlite3
import os

def cleanup_database(db_path='buses_data.sqlite'):
    # Проверяем, существует ли файл БД
    if not os.path.exists(db_path):
        print(f"Файл базы данных '{db_path}' не найден.")
        return

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Список таблиц для удаления
        tables_to_drop = [
            'user_routes',
            'search_results'
        ]

        print(f"Подключение к {db_path} установлено. Начинаю очистку...")

        for table in tables_to_drop:
            try:
                # DROP TABLE удаляет и структуру, и данные
                cursor.execute(f"DROP TABLE IF EXISTS {table};")
                print(f"✅ Таблица '{table}' успешно удалена.")
            except sqlite3.Error as e:
                print(f"❌ Ошибка при удалении таблицы '{table}': {e}")

        conn.commit()
        
        # VACUUM пересобирает файл базы, уменьшая его размер после удаления данных
        cursor.execute("VACUUM")
        print("\nБаза данных оптимизирована (VACUUM).")

    except sqlite3.Error as e:
        print(f"Критическая ошибка базы данных: {e}")
    finally:
        if conn:
            conn.close()
            print("Соединение закрыто.")

if __name__ == "__main__":
    # Укажи здесь путь к своей БД, если он отличается
    cleanup_database('buses_data.sqlite')