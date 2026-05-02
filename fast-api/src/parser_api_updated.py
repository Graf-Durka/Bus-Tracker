import sqlite3
import datetime
import re
import asyncio
import urllib.parse
from playwright.async_api import async_playwright

class AsyncParserService:
    def __init__(self, db_path='data/buses_data.sqlite'):
        self.db_path = db_path
        # Оставляем семафор на случай, если где-то вызовется параллельно, 
        # но внутри методов будем идти циклом
        self.semaphore = asyncio.Semaphore(3)
        self._check_and_update_schema()

    def _get_conn(self):
        return sqlite3.connect(self.db_path)

    def _check_and_update_schema(self):
        with self._get_conn() as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            cursor = conn.cursor()
            try:
                cursor.execute("PRAGMA table_info(search_results)")
                columns = [col[1] for col in cursor.fetchall()]
                if 'travel_time_route' not in columns:
                    cursor.execute("ALTER TABLE search_results ADD COLUMN travel_time_route INTEGER")
                    conn.commit()
            except Exception as e:
                print(f"[DB ERR] {e}")

    def is_time_valid(self, calculated_mins, est_mins):
        if not est_mins or est_mins == 0: return True
        return (0.5 * est_mins) <= calculated_mins <= (1.5 * est_mins)

    def parse_to_minutes(self, stop_data, now):
        if stop_data.get('timeRel'):
            m = re.search(r'(\d+)', stop_data['timeRel'])
            if m: return int(m.group(1))
        if stop_data.get('timeAbs'):
            try:
                t_parts = datetime.datetime.strptime(stop_data['timeAbs'], "%H:%M")
                dt = now.replace(hour=t_parts.hour, minute=t_parts.minute, second=0, microsecond=0)
                if dt < now - datetime.timedelta(hours=6):
                    dt += datetime.timedelta(days=1)
                return int((dt - now).total_seconds() / 60)
            except: pass
        return None

    async def _run_parsing_loop(self, tasks):
        """Внутренний метод для последовательного прохода по списку задач."""
        if not tasks:
            return

        async with async_playwright() as p:
            # Запускаем с небольшими оптимизациями для Docker
            browser = await p.chromium.launch(headless=True, args=['--disable-dev-shm-usage'])
            context = await browser.new_context(
                viewport={'width': 1280, 'height': 800},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            )

            for track_id, bus_name, est_mins, direction, s_stop, e_stop in tasks:
                page = await context.new_page()
                # Тайм-аут на навигацию — 30 сек, на поиск селекторов — 15 сек
                page.set_default_navigation_timeout(30000)
                page.set_default_timeout(15000)
                
                try:
                    # Блокируем ВСЁ лишнее: картинки, шрифты, стили карт
                    await page.route("**/*.{png,jpg,jpeg,svg,woff,woff2,css}", lambda route: route.abort())
                    
                    # КЛЮЧ: ждем только 'domcontentloaded' (структуру), а не 'load' (картинки)
                    await page.goto(
                        f"https://2gis.ru/novosibirsk/search/{urllib.parse.quote(bus_name)}", 
                        wait_until="domcontentloaded" 
                    )
                    
                    # Ждем появления списка ИЛИ карточки
                    try:
                        await page.wait_for_selector("._1kf6gff, ._1sv3x8qq", timeout=10000)
                    except:
                        pass

                    # Проверка на список (если нужно кликнуть)
                    bus_cards = await page.query_selector_all("._1kf6gff")
                    if bus_cards:
                        for card in bus_cards:
                            text = await card.inner_text()
                            if bus_name in text.split('\n')[0]:
                                await card.click()
                                await asyncio.sleep(0.5)
                                break
                    
                    # Даем секундную паузу на прогрузку Live-данных (времени прибытия)
                    await asyncio.sleep(1.5)

                    stops_data = await page.evaluate(r"""() => {
                        return Array.from(document.querySelectorAll('._15nfxwn')).map(el => {
                            let tAbs = el.querySelector('._apda8tn, ._1g4kbeq');
                            let tRel = el.querySelector('._psoawlx, ._mgulo2d, [class*="time"]');
                            return {
                                name: el.querySelector('._14hj5c4')?.innerText.trim() || "???",
                                timeAbs: tAbs?.innerText.includes(':') ? tAbs.innerText.trim() : null,
                                timeRel: tRel?.innerText.includes('мин') ? tRel.innerText.trim() : null
                            };
                        });
                    }""")

                    start_idx, end_idx = -1, -1
                    target_occ = 2 if direction == 'from' else 1
                    curr_occ = 0
                    for i, s in enumerate(stops_data):
                        if s_stop.lower() in s['name'].lower() and start_idx == -1:
                            curr_occ += 1
                            if curr_occ == target_occ: start_idx = i
                        elif e_stop.lower() in s['name'].lower() and start_idx != -1:
                            end_idx = i; break

                    if start_idx != -1 and end_idx != -1:
                        now = datetime.datetime.now()
                        s_mins = self.parse_to_minutes(stops_data[start_idx], now)
                        
                        if s_mins is not None:
                            start_dt = now + datetime.timedelta(minutes=s_mins)
                            arrival_s = start_dt.strftime("%H:%M")
                            arrival_e, t_route, method = None, est_mins, "FB"

                            # --- МЕТОД 0: ПРЯМОЙ (если оба ЧЧ:ММ) ---
                            if stops_data[start_idx]['timeAbs'] and stops_data[end_idx]['timeAbs']:
                                e_mins = self.parse_to_minutes(stops_data[end_idx], now)
                                if e_mins is not None:
                                    calc = e_mins - s_mins
                                    if self.is_time_valid(calc, est_mins):
                                        arrival_e, t_route, method = (now + datetime.timedelta(minutes=e_mins)).strftime("%H:%M"), calc, "M0"

                            # --- МЕТОД 2: АНКЕР (ОРИГИНАЛ) ---
                            if arrival_e is None:
                                last_anc_m = None
                                for i in range(start_idx, end_idx + 1):
                                    if stops_data[i]['timeAbs']:
                                        last_anc_m = self.parse_to_minutes(stops_data[i], now)
                                
                                if last_anc_m is not None:
                                    # Плюс остаток минут до конечной
                                    e_rel_only = self.parse_to_minutes({'timeRel': stops_data[end_idx]['timeRel']}, now) or 0
                                    total_e = last_anc_m + e_rel_only
                                    calc = total_e - s_mins
                                    if self.is_time_valid(calc, est_mins):
                                        arrival_e, t_route, method = (now + datetime.timedelta(minutes=total_e)).strftime("%H:%M"), calc, "M2"

                            # --- МЕТОД 3: НАКОПЛЕНИЕ (ОРИГИНАЛ) ---
                            if arrival_e is None:
                                acc_m, prev_m = 0, None
                                for i in range(start_idx, end_idx):
                                    cur_m = self.parse_to_minutes(stops_data[i], now)
                                    if cur_m is not None:
                                        if prev_m is not None and cur_m < prev_m:
                                            acc_m += prev_m
                                        prev_m = cur_m
                                
                                final_m = self.parse_to_minutes(stops_data[end_idx], now)
                                if final_m is not None:
                                    total_e = acc_m + final_m
                                    calc = total_e - s_mins
                                    if self.is_time_valid(calc, est_mins):
                                        arrival_e, t_route, method = (now + datetime.timedelta(minutes=total_e)).strftime("%H:%M"), calc, "M3"

                            # --- ФОЛБЭК ---
                            if arrival_e is None:
                                arrival_e = (start_dt + datetime.timedelta(minutes=est_mins)).strftime("%H:%M")

                            with self._get_conn() as conn:
                                conn.execute('''UPDATE search_results SET arrival_time_start=?, arrival_time_end=?, 
                                                travel_time_route=?, status='active', last_updated=CURRENT_TIMESTAMP WHERE track_id=?''',
                                             (arrival_s, arrival_e, max(0, t_route), track_id))
                                conn.commit()
                            print(f"  [{method}] {bus_name}: {arrival_s} -> {arrival_e} ({t_route} мин)")
                    else:
                        with self._get_conn() as conn:
                            # Вместо удаления просто помечаем маршрут, чтобы он остался в БД и на дашборде
                            conn.execute('''
                                UPDATE search_results 
                                SET status='passed', 
                                    arrival_time_start='--', 
                                    arrival_time_end='--', 
                                    last_updated=CURRENT_TIMESTAMP 
                                WHERE track_id=?
                            ''', (track_id,))
                            conn.commit()
                except Exception as e:
                    print(f"  [ERR] {bus_name}: {e}")
                finally:
                    await page.close()
            await browser.close()

    async def update_all_live_data(self):
        """Обновление всех автобусов по расписанию."""
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT track_id, bus_name, est_travel_time_mins, direction, start_stop, end_stop 
                FROM search_results 
                WHERE status = 'pending' OR (strftime('%s', 'now') - strftime('%s', last_updated) > 45)
            ''')
            tasks = cursor.fetchall()
        await self._run_parsing_loop(tasks)

    async def update_specific_tracks(self, track_ids: list):
        """Обновление всех маршрутов конкретного пользователя."""
        if not track_ids: return
        with self._get_conn() as conn:
            placeholders = ', '.join(['?'] * len(track_ids))
            cursor = conn.cursor()
            cursor.execute(f'''
                SELECT track_id, bus_name, est_travel_time_mins, direction, start_stop, end_stop 
                FROM search_results 
                WHERE track_id IN ({placeholders})
            ''', track_ids)
            tasks = cursor.fetchall()
        await self._run_parsing_loop(tasks)

    async def update_single_track(self, track_id: int):
        """Обновление одного конкретного маршрута."""
        await self.update_specific_tracks([track_id])