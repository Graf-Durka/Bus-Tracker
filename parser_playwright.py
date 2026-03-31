from playwright.sync_api import sync_playwright
import time

def parse_2gis():
    with sync_playwright() as p:
        # Запускаем браузер. В Playwright headless работает стабильнее
        browser = p.chromium.launch(headless=True)
        
        # Маскируемся под пользователя и задаем большое окно
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        print("Загружаем страницу 2ГИС...")
        page.goto("https://2gis.ru/search/%D0%90%D0%B2%D1%82%D0%BE%D0%B1%D1%83%D1%81%D1%8B/geo/141751100637270/82.790222%2C55.039434?m=82.876396%2C55.034909%2F12", timeout=60000)
        
        # Ждем загрузки первых автобусов
        page.wait_for_selector("._1kf6gff", timeout=15000)

        all_buses_data = []

        while True:
            # Получаем все автобусы на странице
            bus_elements = page.query_selector_all("._1kf6gff")
            
            for index in range(len(bus_elements)):
                try:
                    # Ищем заново, чтобы избежать Stale Element Reference (когда страница перерисовалась)
                    current_bus = page.query_selector_all("._1kf6gff")[index]
                    bus_text = current_bus.inner_text().strip().split('\n')[0]
                    
                    if not bus_text.startswith("Автобус"):
                        continue
                        
                    print(f"Зашли в: {bus_text}")
                    current_bus.click()
                    
                    # Ждем, пока прогрузятся остановки (ждем появления селектора "куда")
                    page.wait_for_selector("._1sv3x8qq", timeout=5000)
                    
                    to_text = page.locator("._1sv3x8qq").inner_text()
                    out_text = page.locator("._6xulm8t").inner_text()
                    
                    # Получаем список всех остановок ОДНИМ действием в браузере (быстро!)
                    stops = page.evaluate("""() => {
                        let elements = document.querySelectorAll('._15nfxwn');
                        let results = [];
                        for(let el of elements) {
                            let time = el.querySelector('._apda8tn');
                            let name = el.querySelector('._14hj5c4');
                            results.push({
                                time: time ? time.innerText.trim() : "",
                                name: name ? name.innerText.trim() : ""
                            });
                        }
                        return results;
                    }""")
                    
                    print(f"[{bus_text}] Собрано {len(stops)} остановок (маршрут {out_text} -> {to_text})")
                    
                    all_buses_data.append({
                        "name": bus_text,
                        "from": out_text,
                        "to": to_text,
                        "stops_count": len(stops)
                    })
                    
                    # Закрываем карточку автобуса ИНАЧЕ
                    # Если кнопка '._1mptg25' не найдена или не кликабельна, 
                    # 2ГИС обычно позволяет вернуться назад, кликнув на кнопку "Крестик" или просто обновив поиск
                    try:
                        # Пытаемся найти кнопку назад по разным селекторам, которые часто использует 2GIS:
                        # ._1mptg25 - стрелочка назад, ._1b1u7 - крестик, ._1oww84x - другая стрелочка
                        back_button_selector = "._1mptg25, ._1b1u7, ._1oww84x, ._1tfwnxl"
                        page.locator(back_button_selector).first.click(timeout=3000)
                        # Возвращаемся в список, ждем пока он снова появится
                        page.wait_for_selector("._1kf6gff", timeout=10000)
                    except:
                        # Если совсем ничего не нашли, просто перезагружаем основной URL 
                        # Это 100% сбросит карточку автобуса
                        print("Кнопка назад не найдена, перезагружаем страницу...")
                        page.goto("https://2gis.ru/search/%D0%90%D0%B2%D1%82%D0%BE%D0%B1%D1%83%D1%81%D1%8B/geo/141751100637270/82.790222%2C55.039434?m=82.876396%2C55.034909%2F12", timeout=30000)
                        page.wait_for_selector("._1kf6gff", timeout=15000)
                        
                        # ВАЖНО: Если мы перезагрузили страницу, список элементов сбросился и индексы стали недействительными.
                        # Чтобы цикл 'for index in range...' не упал с "list index out of range", нужно обновить bus_elements
                        bus_elements = page.query_selector_all("._1kf6gff")

                except Exception as e:
                    print(f"Ошибка при обработке автобуса: {str(e)[:100]}...")
            
            # Пробуем нажать "Следующая страница" (найти активную кнопку вперед)
            # В 2GIS пагинация - кнопка ._n5hmn94
            next_btn = page.locator("._n5hmn94").last
            
            # Если кнопка disabled или ее нет - выходим
            if next_btn.count() == 0 or "disabled" in next_btn.get_attribute("class"):
                print("Достигли последней страницы!")
                break
                
            print("Переходим на следующую страницу...")
            next_btn.click()
            page.wait_for_timeout(2000) # Даем время подгрузить новый список
            page.wait_for_selector("._1kf6gff")

        print("=== ПАРСИНГ ЗАВЕРШЕН ===")
        print(f"Всего собрано автобусов: {len(all_buses_data)}")

        browser.close()

if __name__ == "__main__":
    parse_2gis()
