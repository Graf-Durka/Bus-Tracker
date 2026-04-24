import sqlite3

class BusManager:
    def __init__(self, db_path='data/buses_data.sqlite'):
        self.db_path = db_path
        self.SYSTEM_USER_ID = "0"  # Тот самый системный пользователь

    def _get_conn(self):
        return sqlite3.connect(self.db_path)

    async def delete_user_data(self, user_id: str):
        """
        Удаляет привязки пользователя и чистит задачи парсера, 
        которые остались совсем без подписчиков.
        """
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM user_routes WHERE user_id = ?", (str(user_id),))
            # Удаляем из search_results только те задачи, которые никто не смотрит (даже "0")
            cursor.execute('''
                DELETE FROM search_results 
                WHERE track_id NOT IN (SELECT DISTINCT track_id FROM user_routes)
            ''')
            conn.commit()
            print(f"[DB] Данные для пользователя {user_id} очищены.")

    async def get_or_create_tracks_by_stops(self, start: str, end: str):
        found = []
        with self._get_conn() as conn:
            cursor = conn.cursor()
            query = '''
                SELECT DISTINCT r.id, r.bus_name, s1.direction, s1.arrival_time, s2.arrival_time
                FROM routes r
                JOIN route_stops s1 ON r.id = s1.route_id
                JOIN route_stops s2 ON r.id = s2.route_id AND s1.direction = s2.direction
                WHERE s1.stop_name LIKE ? AND s2.stop_name LIKE ? AND s1.stop_order < s2.stop_order
            '''
            cursor.execute(query, (f'%{start}%', f'%{end}%'))
            
            for r_id, bus, direct, t1, t2 in cursor.fetchall():
                # Проверяем наличие задачи
                cursor.execute("""
                    SELECT track_id, arrival_time_start, arrival_time_end, status 
                    FROM search_results WHERE route_id=? AND start_stop=? AND end_stop=?
                """, (r_id, start, end))
                
                exists = cursor.fetchone()
                if exists:
                    track_id = exists[0]
                    found.append({
                        "track_id": track_id, "bus": bus, 
                        "arrival_start": exists[1] or "...", "arrival_end": exists[2] or "...", "status": exists[3]
                    })
                else:
                    est = abs(int(float(t2)) - int(float(t1)))
                    cursor.execute("""
                        INSERT INTO search_results 
                        (route_id, start_stop, end_stop, bus_name, direction, est_travel_time_mins, status) 
                        VALUES (?,?,?,?,?,?,?)
                    """, (r_id, start, end, bus, direct, est, 'pending'))
                    track_id = cursor.lastrowid
                    found.append({
                        "track_id": track_id, "bus": bus, 
                        "arrival_start": "Запуск...", "arrival_end": "---", "status": "pending"
                    })
                
                # АВТО-ПРИВЯЗКА К СИСТЕМНОМУ ПОЛЬЗОВАТЕЛЮ "0"
                # Чтобы парсер не удалил маршрут до нажатия кнопки Subscribe
                cursor.execute("INSERT OR IGNORE INTO user_routes (user_id, track_id) VALUES (?,?)", 
                               (self.SYSTEM_USER_ID, track_id))
                               
            conn.commit()
        return found

    async def quick_subscribe(self, user_id: str, track_id: int):
        with self._get_conn() as conn:
            conn.execute("INSERT OR IGNORE INTO user_routes (user_id, track_id) VALUES (?,?)", (str(user_id), track_id))
            conn.commit()
        return True

    async def get_user_dashboard(self, user_id: str):
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT sr.track_id, sr.bus_name, sr.start_stop, sr.end_stop, sr.arrival_time_start, sr.arrival_time_end, sr.status
                FROM user_routes ur JOIN search_results sr ON ur.track_id = sr.track_id
                WHERE ur.user_id = ?
            ''', (str(user_id),))
            return cursor.fetchall()