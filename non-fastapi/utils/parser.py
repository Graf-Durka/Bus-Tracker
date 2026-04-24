import time
import csv
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options


#Подготовка
name = []
go_to = []
stations = []
go_to_stations = []
go_out = []
go_out_stations = []

#Входим в сеть
options = Options()
options.add_argument("--headless=new") # Для запуска без окна раскомментируйте
options.add_argument("--window-size=1920,1080") # Это решает проблему со скроллом в headless режиме
options.add_argument("--disable-blink-features=AutomationControlled") # Обход антибота
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36") # Маскировка под реального пользователя
driver = webdriver.Chrome(options=options)
wait = WebDriverWait(driver, 10)
driver.get("https://2gis.ru/search/%D0%90%D0%B2%D1%82%D0%BE%D0%B1%D1%83%D1%81%D1%8B/geo/141751100637270/82.790222%2C55.039434?m=82.876396%2C55.034909%2F12")
time.sleep(5)

#Перебираем каждую страницу
while True:
    #Собираем все автобусы и перебираем их
    bus_containers = driver.find_elements(By.CSS_SELECTOR, "._1kf6gff")
    for container in bus_containers:
        bus_element = container.find_element(By.TAG_NAME, "span")
        bus_text = bus_element.text.strip()
        print(f"Найден автобус: {bus_text}")

        #Проверка на автобус (там пока страницы листаешь, там есть и не автобусы)
        if(bus_text.split()[0] == "Автобус"):
            #Флаг, чтобы понять какие остановки и время относятся к маршруту туда, а какие обратно
            out_to_flag = 0
            name.append(bus_text)
            
            print(f"Кликаем на {bus_text}...")
            container.click()
            time.sleep(3)
            
            driver.save_screenshot("debug_after_click.png")
            print("Скриншот сохранен. Ищем 'куда'...")

            #Берём название до куда он идёт
            try:
                to = driver.find_element(By.CSS_SELECTOR, "._1sv3x8qq")
                to_text = to.text.strip()
            except Exception as e:
                print(f"Не нашли destination: {e}")
                to_text = "Неизвестно"
                
            go_to.append(to_text)

            #Берём название от куда он идёт
            out = driver.find_element(By.CSS_SELECTOR, "._6xulm8t")
            out_text = out.text.strip()
            go_out.append(out_text)
            print(f"Маршрут: {out_text} -> {to_text}")

            #Перебираем все его остановки
            station_containers = driver.find_elements(By.CSS_SELECTOR, "._15nfxwn")
            print(f"Найдено остановок на экране: {len(station_containers)}")
            
            # Один раз скроллим до самого низа, чтобы подгрузить все остановки, если тут ленивая загрузка
            try:
                driver.execute_script("arguments[0].scrollIntoView(true);", station_containers[-1])
                time.sleep(1) # Даем секунду на подгрузку
                # Обновляем список станций после скролла
                station_containers = driver.find_elements(By.CSS_SELECTOR, "._15nfxwn")
            except:
                pass

            # Извлекаем весь текст разом через JavaScript для бешеной скорости
            # Это мгновенно по сравнению с `find_element` для каждой остановки!
            stop_details = driver.execute_script("""
                let stops = arguments[0];
                let results = [];
                for(let i = 0; i < stops.length; i++) {
                    let timeEl = stops[i].querySelector('._apda8tn');
                    let nameEl = stops[i].querySelector('._14hj5c4');
                    results.push({
                        time: timeEl ? timeEl.innerText.trim() : "",
                        name: nameEl ? nameEl.innerText.trim() : ""
                    });
                }
                return results;
            """, station_containers)

            for index, detail in enumerate(stop_details):
                print(f"Обработка остановки {index + 1}/{len(stop_details)}...", end="\r")
                station_name_text = detail['name']
                time_text = detail['time']

                #Проверяем, к какому пути отнести остановку
                if(len(go_to_stations)!=0 and go_to_stations[-1][0] == station_name_text):
                    out_to_flag+=1
                if(out_to_flag == 0):
                    go_to_stations.append([station_name_text, time_text])
                else:
                    go_out_stations.append([station_name_text, time_text])
                    
            print("") # Перевод строки после прогресс-бара
            
            # ИСПРАВЛЕНИЕ 2: ЗАКРЫВАЕМ КАРТОЧКУ АВТОБУСА ПОСЛЕ ПАРСИНГА ОСТАНОВОК
            try:
                # Ищем кнопку "Назад" в карточке автобуса (стрелочка влево) и кликаем на неё
                back_btn = driver.find_element(By.CSS_SELECTOR, "._1mptg25") # Обратите внимание: класс кнопки назад может быть другим, это нужно будет проверить! Часто это "._1mptg25" или "._1b1u7"
                back_btn.click()
                time.sleep(3) # Ждем, пока вернется список
            except Exception as e:
                print(f"Не смогли закрыть карточку автобуса ловим ошибку: {e}")
                # Если не нашли кнопку назад, можно жестко перезагрузить урл:
                # driver.get("https://2gis.ru/search/Автобусы/...") 
                # Но это сбросит пагинацию. 
        
        #Двигаем вниз ползунок автобусов
        driver.execute_script("""
            arguments[0].scrollIntoView({
            behavior: 'smooth',
            block: 'center',
            inline: 'center'
            });
        """, container)
    stations.append([go_to_stations,go_out_stations])

    #Проверка, можно ли перейти на седующую страницу с автобусами и парсить на ней
    try:
        next_btn = driver.find_element(By.CSS_SELECTOR, "._n5hmn94")
        if "disabled" in next_btn.get_attribute("class"): break
        next_btn.click()
        time.sleep(3)
    except:
        break

print(name)



