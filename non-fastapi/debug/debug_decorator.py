import functools

def debug_time_calc(func):
    @functools.wraps(func)
    def wrapper(self, bus_name, stops, start_idx, end_idx, start_time_str, start_is_rel, est_mins, now, *args, **kwargs):
        print(f"\n================ [DEBUG {bus_name}] ================")
        print(f"Старт: '{stops[start_idx]['name']}' ({start_time_str})")
        print(f"Конец: '{stops[end_idx]['name']}'")
        print(f"Расчетное время по базе 2GIS: {est_mins} мин")
        print("-------------------------------------------------")
        
        result = func(self, bus_name, stops, start_idx, end_idx, start_time_str, start_is_rel, est_mins, now, *args, **kwargs)
        
        if not result:
            print(">>> Нет данных для расчета.")
            return None
            
        debug_data = result.get('debug_data', {})
        arr_start = result.get('arrival_time_start')
        arr_end = result.get('arrival_time_end')
        t_route = result.get('travel_time_route')
        
        # Метод 2 (Анкер)
        m2 = debug_data.get('method2')
        if m2:
            print(f"Метод 2 (Анкер):")
            print(f"  Анкерная остановка: '{m2['anchor_name']}' в {m2['anchor_time']}")
            print(f"  Расчетное время: {m2['calculated_mins']} мин (валидно: {m2['is_valid']})")
        
        # Метод 3 (Накопление)
        m3 = debug_data.get('method3')
        if m3:
            print(f"Метод 3 (Накопление):")
            print(f"  Слагаемые: {', '.join(m3['stops'])}")
            print(f"  Сумма: {m3['calculated_mins']} мин (валидно: {m3['is_valid']})")
            
        # Метод Фолбэк
        m_fb = debug_data.get('fallback')
        if m_fb:
            print(f"Метод Фолбэк:")
            print(f"  Применено время из базы: {m_fb['calculated_mins']} мин")
            
        print(f"ИТОГ ({debug_data.get('applied_method', 'Неизвестно')}): Прибытие на старт: {arr_start}, Прибытие на конец: {arr_end}, В пути: {t_route} мин")
        print("=================================================\n")
        
        return result
    return wrapper
