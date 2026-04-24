import sqlite3
import datetime
import re
import httpx
import os
import urllib.parse
from playwright.sync_api import sync_playwright

class ParserDBAPI:
    def __init__(self, db_path='buses_data.sqlite'):
        self.db_path = db_path
        self.setup_database()

    def get_connection(self):
        return sqlite3.connect(self.db_path)

    def is_time_valid(self, calculated_mins, est_travel_time_mins):
        if est_travel_time_mins == 0:
            return True
        return (0.5 * est_travel_time_mins) <= calculated_mins <= (1.5 * est_travel_time_mins)

    def setup_database(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()                   
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS routes (
                id INTEGER PRIMARY KEY,
                bus_name TEXT,
                route_from TEXT,
                route_to TEXT
            );
            ''')
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS route_stops (
                id INTEGER PRIMARY KEY,
                route_id INTEGER,
                direction TEXT,
                stop_name TEXT,
                arrival_time TEXT,
                stop_order INTEGER,
                FOREIGN KEY (route_id) REFERENCES routes(id)
            );
            ''')
            cursor.execute('''
            DROP TABLE IF EXISTS search_results;
            ''')
            cursor.execute('''
            CREATE TABLE search_results (
                route_id INTEGER, start_stop TEXT, end_stop TEXT, bus_name TEXT,
                direction TEXT, est_travel_time_mins INTEGER, arrival_time_start TEXT,
                arrival_time_end TEXT, travel_time_route INTEGER,
                PRIMARY KEY (route_id, start_stop, end_stop)
            );
            ''')
            conn.commit()

    def global_parse(self):
        print("Подключение к базе данных...")
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = context.new_page()

            page.goto("https://2gis.ru/search/%D0%90%D0%B2%D1%82%D0%BE%D0%B1%D1%83%D1%81%D1%8B/geo/141751100637270/82.790222%2C55.039434?m=82.876396%2C55.034909%2F12", timeout=60000)
            page.wait_for_selector("._1kf6gff", timeout=15000)

            total_saved_buses = 0
            processed_buses = set()

            with self.get_connection() as conn:
                cursor = conn.cursor()
                while True:
                    bus_elements = page.query_selector_all("._1kf6gff")
                    
                    for index in range(len(bus_elements)):
                        try:
                            current_elements = page.query_selector_all("._1kf6gff")
                            if index >= len(current_elements): break
                            
                            current_bus = current_elements[index]
                            full_card_text = current_bus.inner_text().strip()
                            bus_text = full_card_text.split('\n')[0].replace('\xa0', ' ').strip()
                            
                            if not bus_text.startswith("Автобус") or full_card_text in processed_buses or not re.match(r'^Автобус\s\d+', bus_text):
                                continue
                                
                            print(f"Обработка: {bus_text}")
                            current_bus.click()
                            
                            page.wait_for_selector("._1sv3x8qq", timeout=3000)
                            
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
                                    let timeAbs = null;
                                    let timeRel = null;
                                    let isRelative = false;
                                    
                                    let timeEl1 = el.querySelector('._apda8tn, ._1g4kbeq');
                                    if (timeEl1 && timeEl1.innerText.trim()) {
                                        timeAbs = timeEl1.innerText.trim();
                                        isRelative = false;
                                    }
                                    
                                    let timeEl2 = el.querySelector('._psoawlx, ._mgulo2d, [class*="time"]');
                                    if (timeEl2 && timeEl2.innerText.includes('мин')) {
                                        timeRel = timeEl2.innerText.trim();
                                        if (!timeAbs) {
                                            isRelative = true;
                                        }
                                    }
                                    let timeText = isRelative ? timeRel : timeAbs;
                                    
                                    let nameEl = el.querySelector('._14hj5c4');
                                    let name = nameEl ? nameEl.innerText.trim() : "Неизвестная остановка";
                                    
                                    results.push({ time: timeText, name: name, isRelative: isRelative });
                                }
                                return results;
                            }""")
                            
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
                            page.keyboard.press("Escape") 

                    next_btn = page.locator("._n5hmn94").last
                    if next_btn.count() == 0 or "disabled" in (next_btn.get_attribute("class") or ""):
                        break
                    
                    next_btn.click()
                    page.wait_for_timeout(3000)

            browser.close()
        print(f"Завершено. Сохранено автобусов: {total_saved_buses}")

    def find_fast_routes(self, start_stop, end_stop):
            with self.get_connection() as conn:
                cursor = conn.cursor()

                query = '''
                SELECT r.id, r.bus_name, s1.arrival_time AS start_time, s2.arrival_time AS end_time, s1.direction
                FROM routes r
                JOIN route_stops s1 ON r.id = s1.route_id
                JOIN route_stops s2 ON r.id = s2.route_id AND s1.direction = s2.direction
                WHERE (s1.stop_name LIKE ? OR s1.stop_name LIKE ?) 
                AND (s2.stop_name LIKE ? OR s2.stop_name LIKE ?) 
                AND s1.stop_order < s2.stop_order
                '''
                cap_start = start_stop[0].upper() + start_stop[1:].lower() if start_stop else ""
                cap_end = end_stop[0].upper() + end_stop[1:].lower() if end_stop else ""
                cursor.execute(query, (f'%{start_stop}%', f'%{cap_start}%', f'%{end_stop}%', f'%{cap_end}%'))
                results = cursor.fetchall()
                
                cursor.execute('DELETE FROM search_results WHERE start_stop = ? AND end_stop = ?', (start_stop, end_stop))
                best_routes = {}
                for row in results:
                    route_id, bus_name, s_time, e_time, direction = row
                    try:
                        est_mins = abs(int(float(e_time)) - int(float(s_time)))
                    except: est_mins = 30
                    if route_id not in best_routes or est_mins < best_routes[route_id]['time']:
                        best_routes[route_id] = {'name': bus_name, 'time': est_mins, 'direction': direction}

                for r_id, data in best_routes.items():
                    cursor.execute('''
                    INSERT INTO search_results (route_id, start_stop, end_stop, bus_name, direction, est_travel_time_mins) 
                    VALUES (?, ?, ?, ?, ?, ?)
                    ''', (r_id, start_stop, end_stop, data['name'], data['direction'], data['time']))
                conn.commit()
                return cursor.execute('SELECT bus_name, est_travel_time_mins FROM search_results WHERE start_stop=? AND end_stop=?', (start_stop, end_stop)).fetchall()


    async def update_accurate_routes(self, start_stop, end_stop):
        import datetime, re, json, httpx
        now = datetime.datetime.now()
        
        # Имитируем мобильный браузер (к ним лояльнее)
        headers = {
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru-ru",
            "Referer": "https://ru.busti.me/novosibirsk/",
        }

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT route_id, bus_name, est_travel_time_mins FROM search_results WHERE start_stop = ? AND end_stop = ?', (start_stop, end_stop))
            routes = cursor.fetchall()
            if not routes: return []

            async with httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=10.0) as client:
                for route_id, bus_name, est_mins in routes:
                    bus_num = "".join(filter(str.isdigit, bus_name))
                    try:
                        # Используем мобильный формат ссылки
                        url = f"https://ru.busti.me/novosibirsk/bus-{bus_num}/"
                        resp = await client.get(url)
                        
                        if resp.status_code != 200:
                            continue

                        # Ищем JSON в тексте страницы
                        # На мобилках данные часто лежат в переменной bs_data или bus_data
                        match = re.search(r'bus_data\s*=\s*(\{.*?\});', resp.text, re.DOTALL)
                        if not match:
                            # Пробуем найти без точки с запятой в конце
                            match = re.search(r'bus_data\s*=\s*(\{.*?\})', resp.text, re.DOTALL)

                        arrival_mins = None
                        if match:
                            data = json.loads(match.group(1))
                            target_clean = re.sub(r'[^а-я0-9]', '', start_stop.lower().replace('метро', ''))
                            
                            for s in data.get('stops', []):
                                api_clean = re.sub(r'[^а-я0-9]', '', s.get('name', '').lower().replace('метро', ''))
                                if target_clean in api_clean or api_clean in target_clean:
                                    t_val = s.get('time')
                                    if t_val is not None and str(t_val).isdigit():
                                        arrival_mins = int(t_val)
                                        break
                        
                        if arrival_mins is not None:
                            start_t = (now + datetime.timedelta(minutes=arrival_mins)).strftime("%H:%M")
                            end_t = (now + datetime.timedelta(minutes=arrival_mins + est_mins)).strftime("%H:%M")
                            cursor.execute('UPDATE search_results SET arrival_time_start=?, arrival_time_end=?, travel_time_route=? WHERE route_id=?', 
                                         (start_t, end_t, arrival_mins, route_id))
                        else:
                            cursor.execute('DELETE FROM search_results WHERE route_id=?', (route_id,))
                            
                    except Exception as e:
                        print(f"DEBUG: Ошибка для {bus_name}: {e}")
            
            conn.commit()
            cursor.execute('SELECT bus_name, arrival_time_start, arrival_time_end, travel_time_route FROM search_results WHERE start_stop=? AND end_stop=?', (start_stop, end_stop))
            return cursor.fetchall()