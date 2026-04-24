import sqlite3
import datetime
import re
import asyncio
import urllib.parse
from playwright.async_api import async_playwright

class AsyncParserService:
    def __init__(self, db_path='data/buses_data.sqlite'):
        self.db_path = db_path

    def _get_conn(self):
        return sqlite3.connect(self.db_path)

    def is_time_valid(self, calculated_mins, est_mins):
        if est_mins == 0: return True
        return (0.5 * est_mins) <= calculated_mins <= (1.8 * est_mins)

    async def update_all_live_data(self):
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT track_id, bus_name, est_travel_time_mins, direction, start_stop, end_stop 
                FROM search_results 
                WHERE status = 'pending' OR (strftime('%s', 'now') - strftime('%s', last_updated) > 45)
            ''')
            tasks = cursor.fetchall()

        if not tasks: return

        print(f"\n--- [LOG] Обновление {len(tasks)} активных треков ---")

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                viewport={'width': 1280, 'height': 1024},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            )
            
            for track_id, bus_name, est_mins, direction, s_stop, e_stop in tasks:
                # 1. ПРОВЕРКА ПОДПИСОК (Включая системную "0")
                with self._get_conn() as conn:
                    if not conn.execute("SELECT 1 FROM user_routes WHERE track_id=?", (track_id,)).fetchone():
                        print(f"  [-] Удаление {bus_name}: нет подписчиков.")
                        conn.execute("DELETE FROM search_results WHERE track_id=?", (track_id,))
                        conn.commit()
                        continue

                page = await context.new_page()
                try:
                    # Блокируем картинки для ускорения
                    await page.route("**/*.{png,jpg,jpeg,svg}", lambda route: route.abort())
                    
                    url = f"https://2gis.ru/novosibirsk/search/{urllib.parse.quote(bus_name)}"
                    await page.goto(url, timeout=30000, wait_until="domcontentloaded")
                    
                    # Ждем появления структуры страницы
                    await page.wait_for_selector("header", timeout=15000)

                    # Выбираем автобус из списка, если он открылся
                    bus_els = await page.query_selector_all("._1kf6gff")
                    for el in bus_els:
                        if bus_name.split()[-1] in (await el.inner_text()):
                            await el.click()
                            break
                    
                    # Ждем список остановок (._15nfxwn - элемент остановки)
                    await page.wait_for_selector("._15nfxwn", timeout=15000)
                    
                    # Прокрутка для прогрузки всех данных
                    await page.evaluate("""() => {
                        const s = document.querySelector('._1sv3x8qq') || document.querySelector('._1r89p9e');
                        if (s) s.scrollTop = s.scrollHeight;
                    }""")
                    await asyncio.sleep(1.5)

                    stops = await page.evaluate("""() => {
                        return Array.from(document.querySelectorAll('._15nfxwn')).map(el => ({
                            name: el.querySelector('._14hj5c4')?.innerText.trim() || "",
                            timeAbs: el.querySelector('._apda8tn, ._1g4kbeq')?.innerText.trim() || null,
                            timeRel: el.querySelector('._psoawlx, ._mgulo2d')?.innerText.trim() || null
                        }));
                    }""")

                    # [Логика накопления времени из предыдущих версий]
                    now = datetime.datetime.now()
                    start_idx, end_idx = -1, -1
                    target_occ = 2 if direction == 'from' else 1
                    curr_occ, start_time_str, start_is_rel = 0, None, False

                    for i, stop in enumerate(stops):
                        if s_stop.lower() in stop['name'].lower() and start_idx == -1:
                            curr_occ += 1
                            if curr_occ == target_occ:
                                start_idx = i
                                start_time_str = stop['timeRel'] or stop['timeAbs']
                                start_is_rel = bool(stop['timeRel'])
                        elif e_stop.lower() in stop['name'].lower() and start_idx != -1:
                            end_idx = i
                            break

                    if start_idx != -1 and end_idx != -1 and start_time_str:
                        if start_is_rel:
                            m = re.search(r'(\d+)', start_time_str)
                            start_dt = now + datetime.timedelta(minutes=int(m.group(1)) if m else 0)
                        else:
                            t = datetime.datetime.strptime(start_time_str, "%H:%M")
                            start_dt = now.replace(hour=t.hour, minute=t.minute, second=0, microsecond=0)
                            while start_dt < now - datetime.timedelta(hours=6): start_dt += datetime.timedelta(days=1)

                        arrival_s, arrival_e = start_dt.strftime("%H:%M"), None
                        
                        # Накопление
                        acc_m, prev_m = 0, None
                        for i in range(start_idx, end_idx):
                            cur_m = None
                            time_val = stops[i]['timeRel'] or stops[i]['timeAbs']
                            if time_val:
                                m_val = re.search(r'(\d+)', time_val)
                                if m_val: cur_m = int(m_val.group(1))
                            
                            if cur_m is not None:
                                if prev_m is not None and cur_m < prev_m: acc_m += prev_m
                                prev_m = cur_m

                        # Конечная
                        last_time = stops[end_idx]['timeRel'] or stops[end_idx]['timeAbs']
                        if last_time:
                            m_last = re.search(r'(\d+)', last_time)
                            if m_last:
                                total_m = acc_m + int(m_last.group(1))
                                end_dt = now + datetime.timedelta(minutes=total_m)
                                if self.is_time_valid(int((end_dt - start_dt).total_seconds()/60), est_mins):
                                    arrival_e = end_dt.strftime("%H:%M")

                        if not arrival_e:
                            arrival_e = (start_dt + datetime.timedelta(minutes=est_mins)).strftime("%H:%M")

                        with self._get_conn() as conn:
                            conn.execute('''UPDATE search_results SET arrival_time_start=?, arrival_time_end=?, 
                                            status='active', last_updated=CURRENT_TIMESTAMP WHERE track_id=?''',
                                         (arrival_s, arrival_e, track_id))
                            conn.commit()
                        print(f"  [OK] {bus_name}: {arrival_s} - {arrival_e}")
                    else:
                        print(f"  [!] {bus_name}: Проехал. Удаление.")
                        with self._get_conn() as conn:
                            conn.execute("DELETE FROM search_results WHERE track_id=?", (track_id,))
                            conn.commit()
                except Exception as e:
                    print(f"  [ERR] {bus_name}: {str(e)[:50]}")
                finally:
                    await page.close()
            await browser.close()