import asyncio
import json
from playwright.async_api import async_playwright

async def run():
    async with async_playwright() as p:
        # Запускаем браузер (видимым, чтобы ты мог нажать на нужную остановку)
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        captured_requests = []

        # Функция-обработчик каждого запроса
        async def handle_response(response):
            # Нас интересуют только Fetch/XHR запросы (данные)
            if response.request.resource_type in ["fetch", "xhr"]:
                try:
                    # Пробуем достать JSON или текст ответа
                    body = await response.text()
                    try:
                        body = json.loads(body)
                    except:
                        pass
                    
                    data = {
                        "url": response.url,
                        "method": response.request.method,
                        "status": response.status,
                        "request_headers": response.request.headers,
                        "response_body": body
                    }
                    captured_requests.append(data)
                    print(f"Захвачен запрос: {response.url[:60]}...")
                except Exception as e:
                    pass

        # Подписываемся на все ответы сервера
        page.on("response", handle_response)

        # Переходим на страницу Новосибирска
        print("Открываю Bustime... Пожалуйста, выбери любой маршрут или остановку на сайте.")
        await page.goto("https://www.bustime.ru/nsk/", wait_until="networkidle")

        # Даем тебе 40 секунд, чтобы ты потыкал на маршруты/остановки
        print("Слушаю трафик 40 секунд. Покликай на автобусы и остановки!")
        await asyncio.sleep(40)

        # Сохраняем результат
        with open("bustime_traffic.json", "w", encoding="utf-8") as f:
            json.dump(captured_requests, f, ensure_ascii=False, indent=4)

        print(f"\nГотово! Записано запросов: {len(captured_requests)}")
        print("Отправь мне файл 'bustime_traffic.json'")
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(run())