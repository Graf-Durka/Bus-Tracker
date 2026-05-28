from fastapi import FastAPI, Request
from src.user_service import BusManager
from src.parser_api_updated import AsyncParserService
import asyncio

from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()
app.state.bus_manager = BusManager()
app.state.parser = AsyncParserService()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_event():
    async def loop_parser():
        while True:
            await app.state.parser.update_all_live_data()
            await asyncio.sleep(45)
    asyncio.create_task(loop_parser())

@app.get("/")
def health_check():
    return {"status": "ok", "message": "API works"}

@app.get("/get_buses")
async def get_buses(start: str, end: str, request: Request):

    return await app.state.bus_manager.get_or_create_tracks_by_stops(start, end)

@app.post("/subscribe")
async def subscribe(track_id: int, request: Request, user_id: str = "0"):
    manager: BusManager = request.app.state.bus_manager
    success = await manager.quick_subscribe(user_id, track_id)
    return {"status": "ok" if success else "error"}

@app.delete("/unsubscribe")
async def unsubscribe(track_id: int, request: Request, user_id: str = "0"):
    """
    Удаляет подписку пользователя на конкретный маршрут (track_id).
    Если маршрут больше никто не отслеживает, задача парсера также удаляется.
    """
    manager: BusManager = request.app.state.bus_manager
    await manager.unsubscribe(user_id, track_id)
    return {"status": "ok", "message": f"Вы отписались от маршрута {track_id}"}

@app.delete("/clear_data")
async def clear_data(user_id: str, request: Request):
    """
    Удаляет все маршруты пользователя.
    """
    await app.state.bus_manager.delete_user_data(user_id)
    return {"status": "ok", "message": f"Данные пользователя {user_id} очищены"}

# Метод get_user_id (по IP) полностью удален, так как Android-клиент 
# должен сам генерировать и передавать свой UUID (или Google Advertising ID / Android ID)
# в качестве параметра user_id во все эндпоинты.

@app.get("/dashboard")
async def dashboard(request: Request, user_id: str = "0"):
    manager: BusManager = request.app.state.bus_manager
    data = await manager.get_user_dashboard(user_id)
    return [
        {
            "track_id": r[0],
            "bus": r[1],
            "start_stop": r[2],
            "end_stop": r[3],
            "arrival_start": r[4] if r[4] else None,
            "arrival_end": r[5] if r[5] else None,
            "status": r[6],
            "est_travel_time": r[8] if r[8] is not None else r[7], 
        } for r in data
    ]

@app.get("/stops")
async def get_all_stops(request: Request):
    """Возвращает список всех очищенных остановок."""
    manager: BusManager = request.app.state.bus_manager
    stops = await manager.get_all_stops()
    return {"status": "ok", "total": len(stops), "stops": stops}

@app.post("/refresh_user_buses")
async def refresh_user_buses(user_id: str, request: Request):
    manager: BusManager = request.app.state.bus_manager
    parser: AsyncParserService = request.app.state.parser
    
    # 1. Получаем ID маршрутов пользователя
    track_ids = await manager.get_user_track_ids(user_id)
    
    if not track_ids:
        return {"status": "ok", "message": "У пользователя нет активных маршрутов"}
    
    # 2. Запускаем парсер принудительно (вне очереди основного цикла)
    # Это обновит данные в БД
    await parser.update_specific_tracks(track_ids)
    
    # 3. Возвращаем свежие данные сразу, чтобы фронтенд мог обновиться
    updated_data = await manager.get_user_dashboard(user_id)
    
    return {
        "status": "ok", 
        "message": f"Данные для {user_id} обновлены", 
        "dashboard": updated_data
    }