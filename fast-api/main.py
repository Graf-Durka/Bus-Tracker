from fastapi import FastAPI, Request
from src.user_service import BusManager
from src.parser_api import AsyncParserService
import asyncio

app = FastAPI()
app.state.bus_manager = BusManager()
app.state.parser = AsyncParserService()

@app.on_event("startup")
async def startup_event():
    async def loop_parser():
        while True:
            await app.state.parser.update_all_live_data()
            await asyncio.sleep(45)
    asyncio.create_task(loop_parser())

@app.get("/get_buses")
async def get_buses(start: str, end: str, request: Request):
    # Теперь автоматически крепится к системному ID="0"
    return await app.state.bus_manager.get_or_create_tracks_by_stops(start, end)

@app.post("/subscribe")
async def subscribe(track_id: int, request: Request, user_id: str = "guest"):
    manager: BusManager = request.app.state.bus_manager
    success = await manager.quick_subscribe(user_id, track_id)
    return {"status": "ok" if success else "error"}

@app.delete("/clear_data")
async def clear_data(user_id: str, request: Request):
    """
    Если user_id="0" - удаляет все маршруты, на которые никто не подписан.
    Если user_id="guest" - удаляет подписки гостя.
    """
    await app.state.bus_manager.delete_user_data(user_id)
    return {"status": "ok", "message": f"Данные пользователя {user_id} очищены"}

@app.get("/dashboard")
async def dashboard(request: Request, user_id: str = "guest"):
    manager: BusManager = request.app.state.bus_manager
    data = await manager.get_user_dashboard(user_id)
    return [
        {
            "track_id": r[0],
            "bus": r[1],
            "start_stop": r[2],
            "end_stop": r[3],
            "arrival_start": r[4] if r[4] else "Ищем...",
            "arrival_end": r[5] if r[5] else "---",
            "status": r[6],
        } for r in data
    ]