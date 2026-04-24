import sqlite3
import datetime
import re
import urllib.parse
from playwright.sync_api import sync_playwright

from debug.debug_decorator import debug_time_calc


class ParserDB_Debug:
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


    @debug_time_calc
    def _calculate_route_times(self, bus_name, stops, start_idx, end_idx, start_time_str, start_is_rel, est_mins, now):
        start_dt = None
        arrival_time_start = None
        debug_data = {'applied_method': None, 'method2': None, 'method3': None, 'fallback': None}
        
        # А. Время на старте (база)
        if start_is_rel:
            match = re.search(r'(\d+)', start_time_str)
            start_dt = now + datetime.timedelta(minutes=int(match.group(1)) if match else 0)
        else:
            try:
                t = datetime.datetime.strptime(start_time_str, "%H:%M")
                start_dt = now.replace(hour=t.hour, minute=t.minute, second=0, microsecond=0)
                while start_dt < now - datetime.timedelta(hours=6): start_dt += datetime.timedelta(days=1)
                while start_dt > now + datetime.timedelta(hours=18): start_dt -= datetime.timedelta(days=1)
            except: start_dt = now
        
        arrival_time_start = start_dt.strftime("%H:%M")

        # Б. Поиск последнего анкера (ЧЧ:ММ) между стартом и финишем
        best_anc_dt = None
        last_anchor_idx = -1
        for i in range(start_idx, end_idx + 1):
            if stops[i]['timeAbs']:
                try:
                    t_anc = datetime.datetime.strptime(stops[i]['timeAbs'], "%H:%M")
                    tmp_dt = now.replace(hour=t_anc.hour, minute=t_anc.minute, second=0, microsecond=0)
                    while tmp_dt < start_dt - datetime.timedelta(hours=6): tmp_dt += datetime.timedelta(days=1)
                    best_anc_dt = tmp_dt
                    last_anchor_idx = i
                except: continue

        # В. Расчет конечной (Анкер + минуты на конечной)
        anchor_applied = False
        has_bus_in_between = False
        prev_mins = None
        
        for i in range(max(start_idx, last_anchor_idx), end_idx + 1):
            cur_m = None
            if stops[i]['timeRel']:
                match = re.search(r'(\d+)', stops[i]['timeRel'])
                if match: cur_m = int(match.group(1))
            elif stops[i]['timeAbs']:
                try:
                    t_abs = datetime.datetime.strptime(stops[i]['timeAbs'], "%H:%M")
                    abs_dt = now.replace(hour=t_abs.hour, minute=t_abs.minute, second=0, microsecond=0)
                    while abs_dt < now - datetime.timedelta(hours=6): abs_dt += datetime.timedelta(days=1)
                    cur_m = int((abs_dt - now).total_seconds() / 60)
                except: pass
            
            if cur_m is not None:
                if prev_mins is not None and cur_m < prev_mins:
                    has_bus_in_between = True
                    break
                prev_mins = cur_m

        # Если автобусов между анкером и концом нет, применяем анкерный метод
        arrival_time_end = None
        travel_time_route = 0

        # Метод 2 (Анкер)
        if best_anc_dt and stops[end_idx]['timeRel']:
            match_end = re.search(r'(\d+)', stops[end_idx]['timeRel'])
            if match_end:
                mins_to_end = int(match_end.group(1))
                end_dt_val = best_anc_dt + datetime.timedelta(minutes=mins_to_end)
                travel_mins_val = int((end_dt_val - start_dt).total_seconds() / 60)
                is_val = self.is_time_valid(travel_mins_val, est_mins)
                
                debug_data['method2'] = {
                    'anchor_name': stops[last_anchor_idx]['name'],
                    'anchor_time': best_anc_dt.strftime('%H:%M'),
                    'calculated_mins': travel_mins_val,
                    'is_valid': is_val
                }

                if not has_bus_in_between and is_val:
                    arrival_time_end = end_dt_val.strftime("%H:%M")
                    travel_time_route = travel_mins_val
                    anchor_applied = True
                    debug_data['applied_method'] = 'Метод 2 (Анкер)'

        # Г. Накопление времени, если анкер не сработал (для нескольких автобусов)
        if not anchor_applied:
            accumulated_mins = 0
            prev_mins = None
            debug_acc_stops = []
            
            for i in range(start_idx, end_idx):
                cur_mins = None
                if stops[i]['timeRel']:
                    match = re.search(r'(\d+)', stops[i]['timeRel'])
                    if match: cur_mins = int(match.group(1))
                elif stops[i]['timeAbs']:
                    try:
                        t_abs = datetime.datetime.strptime(stops[i]['timeAbs'], "%H:%M")
                        abs_dt = now.replace(hour=t_abs.hour, minute=t_abs.minute, second=0, microsecond=0)
                        while abs_dt < now - datetime.timedelta(hours=6): abs_dt += datetime.timedelta(days=1)
                        cur_mins = int((abs_dt - now).total_seconds() / 60)
                    except: pass

                if cur_mins is not None:
                    if prev_mins is not None and cur_mins < prev_mins:
                        accumulated_mins += prev_mins
                        debug_acc_stops.append(f"'{stops[i-1]['name']}' ({prev_mins} мин)")
                    prev_mins = cur_mins
            
            end_mins = None
            if stops[end_idx]['timeRel']:
                match = re.search(r'(\d+)', stops[end_idx]['timeRel'])
                if match: end_mins = int(match.group(1))
            elif stops[end_idx]['timeAbs']:
                try:
                    t_abs = datetime.datetime.strptime(stops[end_idx]['timeAbs'], "%H:%M")
                    abs_dt = now.replace(hour=t_abs.hour, minute=t_abs.minute, second=0, microsecond=0)
                    while abs_dt < now - datetime.timedelta(hours=6): abs_dt += datetime.timedelta(days=1)
                    end_mins = int((abs_dt - now).total_seconds() / 60)
                except: pass

            if end_mins is not None:
                # Если перед конечной время было больше, значит автобус от нее уехал
                if prev_mins is not None and end_mins < prev_mins:
                    accumulated_mins += prev_mins
                    debug_acc_stops.append(f"'{stops[end_idx-1]['name']}' ({prev_mins} мин)")
                
                total_calculated_mins = accumulated_mins + end_mins
                debug_acc_stops.append(f"Конечная: '{stops[end_idx]['name']}' ({end_mins} мин)")
                
                end_dt_val = now + datetime.timedelta(minutes=total_calculated_mins)
                travel_mins_val = int((end_dt_val - start_dt).total_seconds() / 60)
                is_val = self.is_time_valid(travel_mins_val, est_mins)
                
                debug_data['method3'] = {
                    'stops': debug_acc_stops,
                    'calculated_mins': travel_mins_val,
                    'is_valid': is_val
                }
                
                if is_val:
                    arrival_time_end = end_dt_val.strftime("%H:%M")
                    travel_time_route = max(0, travel_mins_val)
                    anchor_applied = True
                    debug_data['applied_method'] = 'Метод 3 (Накопление)'

        # Фолбэк
        if not anchor_applied:
            end_dt_val = start_dt + datetime.timedelta(minutes=est_mins)
            travel_mins_val = est_mins
            debug_data['fallback'] = {
                'calculated_mins': travel_mins_val
            }
            arrival_time_end = end_dt_val.strftime("%H:%M")
            travel_time_route = est_mins
            debug_data['applied_method'] = 'Метод Фолбэк (На основе базы)'

        return {
            'arrival_time_start': arrival_time_start,
            'arrival_time_end': arrival_time_end,
            'travel_time_route': travel_time_route,
            'debug_data': debug_data
        }

    def update_accurate_routes(self, start_stop, end_stop):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            # Получаем список маршрутов, которые нужно уточнить
            cursor.execute('''
                SELECT route_id, bus_name, est_travel_time_mins, direction 
                FROM search_results 
                WHERE start_stop = ? AND end_stop = ?
            ''', (start_stop, end_stop))
            buses_to_check = cursor.fetchall()

            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                )
                page = context.new_page()

                for route_id, bus_name, est_mins, direction in buses_to_check:
                    # Кодируем название автобуса для URL
                    encoded_bus = urllib.parse.quote(bus_name)
                    search_url = f"https://2gis.ru/novosibirsk/search/{encoded_bus}"
                    
                    try:
                        page.goto(search_url, timeout=30000)
                        # Ждем появления списка или сразу карточки
                        page.wait_for_selector("._1kf6gff, ._1sv3x8qq", timeout=10000)
                        
                        bus_elements = page.query_selector_all("._1kf6gff")
                        clicked = False
                        for el in bus_elements:
                            card_text = el.inner_text().strip()
                            b_text = card_text.split('\n')[0].replace('\xa0', ' ').strip()
                            if b_text == bus_name:
                                el.click()
                                clicked = True
                                break
                        
                        if not clicked and bus_elements:
                            bus_elements[0].click()

                        # Ждем загрузки данных об остановках и времени
                        page.wait_for_selector("._1sv3x8qq", timeout=10000)
                        # Даем время на подгрузку динамических элементов (время прибытия)
                        page.wait_for_timeout(2000)

                        stops = page.evaluate(r"""() => {
                            let elements = document.querySelectorAll('._15nfxwn');
                            let results = [];
                            for (let el of elements) {
                                let timeAbs = null;
                                let timeRel = null;
                                
                                // Абсолютное время (ЧЧ:ММ)
                                let timeEl1 = el.querySelector('._apda8tn, ._1g4kbeq');
                                if (timeEl1 && timeEl1.innerText.trim() && timeEl1.innerText.includes(':')) {
                                    timeAbs = timeEl1.innerText.trim();
                                }
                                
                                // Относительное время (через X мин)
                                let timeEl2 = el.querySelector('._psoawlx, ._mgulo2d, [class*="time"]');
                                if (timeEl2 && timeEl2.innerText.includes('мин')) {
                                    timeRel = timeEl2.innerText.trim();
                                }
                                
                                let nameEl = el.querySelector('._14hj5c4');
                                let name = nameEl ? nameEl.innerText.trim() : "Неизвестная остановка";
                                
                                results.push({ 
                                    name: name,
                                    timeAbs: timeAbs,
                                    timeRel: timeRel,
                                    // Общее поле для совместимости с логикой старта
                                    time: timeRel || timeAbs 
                                });
                            }
                            return results;
                        }""")

                        start_idx = -1
                        end_idx = -1
                        target_occurrence = 2 if direction == 'from' else 1
                        current_occurrence = 0
                        
                        start_time_str = None
                        start_is_rel = False

                        # 1. СНАЧАЛА НАХОДИМ ИНДЕКСЫ ОСТАНОВОК
                        for i, stop in enumerate(stops):
                            s_name = stop['name'].lower()
                            if start_stop.lower() in s_name and start_idx == -1:
                                current_occurrence += 1
                                if current_occurrence == target_occurrence:
                                    start_idx = i
                                    start_time_str = stop['time']
                                    # Если в timeRel есть данные, считаем время относительным
                                    start_is_rel = True if stop['timeRel'] else False
                            
                            elif end_stop.lower() in s_name and start_idx != -1:
                                end_idx = i
                                break

                        # 2. РАСЧЕТ ЛОГИКИ (если обе остановки найдены)
                        if start_idx != -1 and end_idx != -1 and start_time_str:
                            now = datetime.datetime.now()
                            calc_result = self._calculate_route_times(bus_name, stops, start_idx, end_idx, start_time_str, start_is_rel, est_mins, now)
                            
                            arrival_time_start = calc_result['arrival_time_start']
                            arrival_time_end = calc_result['arrival_time_end']
                            travel_time_route = calc_result['travel_time_route']

                            # 3. ОБНОВЛЯЕМ БАЗУ
                            cursor.execute('''
                                UPDATE search_results 
                                SET arrival_time_start = ?, arrival_time_end = ?, travel_time_route = ?
                                WHERE route_id = ? AND start_stop = ? AND end_stop = ?
                            ''', (arrival_time_start, arrival_time_end, max(0, travel_time_route), route_id, start_stop, end_stop))
                        
                        else:
                            # Удаляем маршрут, если он уже не актуален (автобус проехал остановку)
                            cursor.execute("DELETE FROM search_results WHERE route_id = ? AND start_stop = ? AND end_stop = ?", (route_id, start_stop, end_stop))

                    except Exception as e:
                        print(f"Ошибка при точном парсинге '{bus_name}': {e}")
                        # В случае ошибки очищаем результат для этого маршрута
                        cursor.execute("DELETE FROM search_results WHERE route_id = ? AND start_stop = ? AND end_stop = ?", (route_id, start_stop, end_stop))

                browser.close()
            conn.commit()

            # Возвращаем отсортированные результаты
            cursor.execute('''
                SELECT bus_name, arrival_time_start, arrival_time_end, travel_time_route 
                FROM search_results 
                WHERE start_stop = ? AND end_stop = ?
                ORDER BY travel_time_route ASC
            ''', (start_stop, end_stop))
            return cursor.fetchall()
