import sqlite3
import datetime
import re
from playwright.sync_api import sync_playwright

def setup_database():
    conn = sqlite3.connect('buses_data.sqlite')
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS routes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        bus_name TEXT,
        route_from TEXT,
        route_to TEXT
    );
    ''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS route_stops (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        route_id INTEGER,
        direction TEXT,
        stop_name TEXT,
        arrival_time TEXT,
        stop_order INTEGER,
        FOREIGN KEY (route_id) REFERENCES routes(id)
    );
    ''')
    conn.commit()
    return conn, cursor

def parse_2gis():
    print("Подключение к базе данных...")
    conn, cursor = setup_database()
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        # URL с результатами поиска
        page.goto("https://2gis.ru/search/%D0%90%D0%B2%D1%82%D0%BE%D0%B1%D1%83%D1%81%D1%8B/geo/141751100637270/82.790222%2C55.039434?m=82.876396%2C55.034909%2F12", timeout=60000)
        
        page.wait_for_selector("._1kf6gff", timeout=15000)

        total_saved_buses = 0
        processed_buses = set()

        while True:
            bus_elements = page.query_selector_all("._1kf6gff")
            
            for index in range(len(bus_elements)):
                try:
                    # Повторно находим элементы, так как DOM мог обновиться
                    current_elements = page.query_selector_all("._1kf6gff")
                    if index >= len(current_elements): break
                    
                    current_bus = current_elements[index]
                    full_card_text = current_bus.inner_text().strip()
                    bus_text = full_card_text.split('\n')[0].replace('\xa0', ' ').strip()
                    
                    if not bus_text.startswith("Автобус") or full_card_text in processed_buses or not re.match(r'^Автобус\s\d+', bus_text):
                        continue
                        
                    print(f"Обработка: {bus_text}")
                    current_bus.click()
                    
                    # Ждем появления контейнера остановок
                    page.wait_for_selector("._1sv3x8qq", timeout=3000)
                    
                    # Это предотвращает получение NULL из-за медленной загрузки скриптов 2GIS
                    try:
                        page.wait_for_selector("._apda8tn, ._psoawlx, ._mgulo2d", timeout=3000)
                    except:
                        print(f"Предупреждение: Время для {bus_text} не подгрузилось вовремя.")

                    to_text = page.locator("._1sv3x8qq").inner_text()
                    out_text = page.locator("._6xulm8t").inner_text() if page.locator("._6xulm8t").count() > 0 else "Не указано"
                    
                    stops = page.evaluate(r"""() => {
                        let elements = document.querySelectorAll('._15nfxwn');
                       let results = [];
                        for (let el of elements) {
                            let timeText = null;
                            let isRelative = false;
                            
                            // Поиск ЧЧ:ММ
                            let timeEl1 = el.querySelector('._apda8tn, ._1g4kbeq');
                            if (timeEl1 && timeEl1.innerText.trim()) {
                                timeText = timeEl1.innerText.trim();
                                isRelative = false;
                            }
                            
                            // Поиск "m мин" (добавили универсальный поиск по атрибутам, если классы сменятся)
                            if (!timeText) {
                                let timeEl2 = el.querySelector('._psoawlx, ._mgulo2d, [class*="time"]');
                                if (timeEl2 && timeEl2.innerText.includes('мин')) {
                                    timeText = timeEl2.innerText.trim();
                                    isRelative = true;
                                }
                            }
                            
                            let nameEl = el.querySelector('._14hj5c4');
                            let name = nameEl ? nameEl.innerText.trim() : "Неизвестная остановка";
                            
                            results.push({ time: timeText, name: name, isRelative: isRelative });
                        }
                        return results;
                    }""")
                    
                    # Сохранение
                    cursor.execute("INSERT INTO routes (bus_name, route_from, route_to) VALUES (?, ?, ?);", (bus_text, out_text, to_text))
                    route_id = cursor.lastrowid
                    
                    direction = "to"
                    norm_to_text = to_text.lower().replace(" ", "").replace("-", "")
                    
                    for stop_idx, stop_data in enumerate(stops):
                        s_name = stop_data['name']
                        s_time = stop_data['time']
                        
                        if s_time:
                            if stop_data['isRelative']:
                                match = re.search(r'(\d+)', s_time)
                                if match:
                                    mins = int(match.group(1))
                                    s_time = (datetime.datetime.now() + datetime.timedelta(minutes=mins)).strftime("%H:%M")
                        
                        cursor.execute(
                            "INSERT INTO route_stops (route_id, direction, stop_name, arrival_time, stop_order) VALUES (?, ?, ?, ?, ?);",
                            (route_id, direction, s_name, s_time, stop_idx)
                        )
                        
                        # Логика смены направления
                        if direction == "to" and norm_to_text != "неизвестно":
                            norm_s_name = s_name.lower().replace(" ", "").replace("-", "")
                            if (norm_s_name in norm_to_text or norm_to_text in norm_s_name) and stop_idx > len(stops) * 0.2:
                                direction = "from"

                    conn.commit()
                    total_saved_buses += 1
                    processed_buses.add(full_card_text)
                    print(f"Успешно: {bus_text} ({len(stops)} ост.)")

                except Exception as e:
                    print(f"Ошибка в цикле: {e}")
                    # Попытка вернуться к списку
                    page.keyboard.press("Escape") 

            # Пагинация
            next_btn = page.locator("._n5hmn94").last
            if next_btn.count() == 0 or "disabled" in (next_btn.get_attribute("class") or ""):
                break
            
            next_btn.click()
            page.wait_for_timeout(3000)

        browser.close()
    conn.close()
    print(f"Завершено. Сохранено автобусов: {total_saved_buses}")

if __name__ == "__main__":
    parse_2gis()