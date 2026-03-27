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
driver = webdriver.Chrome()
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
            container.click()
            time.sleep(5)

            #Берём название до куда он идёт
            to = driver.find_element(By.CSS_SELECTOR, "._1sv3x8qq")
            to_text = to.text.strip()
            go_to.append(to_text)

            #Берём название от куда он идёт
            out = driver.find_element(By.CSS_SELECTOR, "._6xulm8t")
            out_text = out.text.strip()
            go_out.append(out_text)

            #Перебираем все его остановки
            station_containers = driver.find_elements(By.CSS_SELECTOR, "._15nfxwn")
            for station in station_containers:

                #Парсим название и время
                time_station = station.find_element(By.CSS_SELECTOR, "._apda8tn")
                time_text = time_station.text.strip()
                station_name = station.find_element(By.CSS_SELECTOR, "._14hj5c4")
                station_name_text = station_name.text.strip()

                #Проверяем, к какому пути отнести остановку
                if(len(go_to_stations)!=0 and go_to_stations[-1][0] == station_name_text):
                    out_to_flag+=1
                if(out_to_flag == 0):
                    go_to_stations.append([station_name_text, time_text])
                else:
                    go_out_stations.append([station_name_text, time_text])

                #Двигаем вниз ползунок остановок
                driver.execute_script("""
                    arguments[0].scrollIntoView({
                    behavior: 'smooth',
                    block: 'center',
                    inline: 'center'
                    });
                """, container)
                time.sleep(1)
        
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



