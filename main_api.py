import os
import asyncio
from src.api_db import ParserDBAPI

async def main():
    db_path = 'data/buses_data.sqlite' if os.path.exists('data/buses_data.sqlite') else 'buses_data.sqlite'
    parser = ParserDBAPI(db_path=db_path)
    
    test_cases = [
        ("Станиславского", "Площадь Ленина"),
        ("Учительская", "Метро Заельцовская"),
        ("Метро Студенческая", "Метро Гагаринская"),
        ("Хилокская", "Степная"),
        ("Поселковый совет", "Цветной проезд"),
        ("Цветной проезд", "ГДК"),
        ("Цветной проезд", "Ветлужская")
    ]
    
    for start_stop, end_stop in test_cases:
        print(f"\n{'='*50}")
        print(f"--- Тест: Быстрый поиск маршрута '{start_stop} -> {end_stop}' ---")
        fast_results = parser.find_fast_routes(start_stop, end_stop)
        
        if fast_results:
            for bus, time_mins in fast_results:
                print(f"🚌 {bus} | Примерное время в пути: {time_mins} мин.")
        else:
            print("Маршруты для быстрого поиска не найдены.")

        print(f"\n--- Тест: Точный метод (с использованием API 2GIS) '{start_stop} -> {end_stop}' ---")
        # Обрати внимание: теперь вызов метода асинхронный (await)
        accurate_results = await parser.update_accurate_routes(start_stop, end_stop)
        
        if accurate_results:
            for bus, start_arr, end_arr, route_time in accurate_results:
                print(f"🚌 {bus}")
                print(f"   -> Ожидается на посадочной '{start_stop}' в: {start_arr}")
                print(f"   -> Прибудет на конечную '{end_stop}' в: {end_arr}")
                print(f"   -> Время в пути: {route_time} мин.")
        else:
            print("Записи для уточнения не найдены.")
        print(f"{'='*50}")


if __name__ == "__main__":
    # Запускаем через asyncio, так как мы теперь используем асинхронные методы Playwright и HTTPX
    asyncio.run(main())
