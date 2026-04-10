import sqlite3
import pandas as pd

# # Подключение к базе
# conn = sqlite3.connect('buses_data.sqlite')

# # Просмотр всех маршрутов
# routes_df = pd.read_sql_query("SELECT * FROM routes", conn)


# # Просмотр остановок
# stops_df = pd.read_sql_query("SELECT * FROM route_stops", conn)

# route_id = []
# stops_id = []
# for idx, i in enumerate(stops_df['arrival_time'].isnull()):
#     if i == True:
#         stops_id.append(int(stops_df['id'][idx]) - 1)
#         if (stops_df['route_id'][idx])-1 not in route_id:
#             route_id.append(int(stops_df['route_id'][idx])-1)


# routes_df.drop(route_id, axis=0, inplace=True)
# routes_df = routes_df.reset_index(drop=True)

# stops_df.drop(stops_id, axis=0, inplace=True)
# stops_df = stops_df.reset_index(drop=True)
# stops_df['arrival_time'] = pd.to_datetime(stops_df['arrival_time'], format='%H:%M')


# route_id = []
# time = 0
# for idx, i in enumerate(stops_df['arrival_time']):
#     if(stops_df['route_id'][idx] not in route_id or (stops_df['direction'][idx] == 'from' and stops_df['direction'][idx-1] == 'to')):
#         time = stops_df['arrival_time'][idx]
#         route_id.append(stops_df['route_id'][idx])
#         stops_df['arrival_time'][idx] = 0
#     else:
#         stops_df['arrival_time'][idx] = (stops_df['arrival_time'][idx] - time).total_seconds() / 60

# routes_df.to_sql('routes', conn, if_exists='replace', index=False)
# stops_df.to_sql('route_stops', conn, if_exists='replace', index=False)

# conn.close()

conn = sqlite3.connect('buses_data.sqlite')
routes_verify = pd.read_sql_query("SELECT * FROM routes", conn)
stops_verify = pd.read_sql_query("SELECT * FROM route_stops", conn)

print(routes_verify)
print(stops_verify)

conn.close()