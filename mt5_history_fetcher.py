import json
import logging
from datetime import datetime
import MetaTrader5 as mt5
import argparse
import configparser


def initialize_mt5():
    """Инициализация подключения к MetaTrader 5"""
    if not mt5.initialize():
        print(f"Ошибка инициализации MetaTrader 5: {mt5.last_error()}")
        return False
    return True


def get_timeframe_from_config(timeframe_str):
    """Преобразование строкового значения таймфрейма в константу MT5"""
    timeframe_map = {
        'M1': mt5.TIMEFRAME_M1,
        'M2': mt5.TIMEFRAME_M2,
        'M3': mt5.TIMEFRAME_M3,
        'M4': mt5.TIMEFRAME_M4,
        'M5': mt5.TIMEFRAME_M5,
        'M10': mt5.TIMEFRAME_M10,
        'M15': mt5.TIMEFRAME_M15,
        'M30': mt5.TIMEFRAME_M30,
        'H1': mt5.TIMEFRAME_H1,
        'H2': mt5.TIMEFRAME_H2,
        'H3': mt5.TIMEFRAME_H3,
        'H4': mt5.TIMEFRAME_H4,
        'H6': mt5.TIMEFRAME_H6,
        'H8': mt5.TIMEFRAME_H8,
        'H12': mt5.TIMEFRAME_H12,
        'D1': mt5.TIMEFRAME_D1,
        'W1': mt5.TIMEFRAME_W1,
        'MN1': mt5.TIMEFRAME_MN1
    }
    
    timeframe_str = timeframe_str.upper()
    if timeframe_str not in timeframe_map:
        raise ValueError(f"Неизвестный таймфрейм: {timeframe_str}. Доступные значения: {list(timeframe_map.keys())}")
    
    return timeframe_map[timeframe_str]


def fetch_history(symbol, timeframe, start_date, end_date):
    """Получение исторических данных из MetaTrader 5"""
    # Преобразуем строки дат в объекты datetime
    start_dt = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
    
    print(f"Получение истории для {symbol} с {start_dt} по {end_dt}, таймфрейм {timeframe}")
    
    # Получаем бары
    rates = mt5.copy_rates_range(symbol, timeframe, start_dt, end_dt)
    
    if rates is None:
        print(f"Ошибка получения данных: {mt5.last_error()}")
        return None
    
    if len(rates) == 0:
        print("Нет данных за указанный период")
        return []
    
    # Преобразуем numpy массив в список словарей для лучшей читаемости
    history_data = []
    for rate in rates:
        bar = {
            'time': datetime.fromtimestamp(rate[0]).strftime('%Y-%m-%d %H:%M:%S'),
            'open': float(rate[1]),
            'high': float(rate[2]),
            'low': float(rate[3]),
            'close': float(rate[4]),
            'volume': int(rate[5])
        }
        history_data.append(bar)
    
    return history_data


def save_to_file(data, filename):
    """Сохранение данных в файл"""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    print(f"Данные сохранены в файл: {filename}")


def load_config(config_path):
    """Загрузка конфигурации из файла"""
    config = configparser.ConfigParser()
    config.read(config_path, encoding='utf-8')
    
    # Получаем параметры
    symbol = config.get('data', 'symbol')
    start_date = config.get('data', 'start_date')
    end_date = config.get('data', 'end_date')
    timeframe_str = config.get('data', 'timeframe', fallback='M1')
    
    return symbol, start_date, end_date, timeframe_str


def main():
    parser = argparse.ArgumentParser(description='Получение исторических данных из MetaTrader 5')
    parser.add_argument('-c', '--config', type=str, default='config.ini',
                        help='Путь к файлу конфигурации (по умолчанию: config.ini)')
    parser.add_argument('-o', '--output', type=str, default='history.json',
                        help='Файл для сохранения результатов (по умолчанию: history.json)')
    
    args = parser.parse_args()
    
    try:
        # Загружаем конфигурацию
        symbol, start_date, end_date, timeframe_str = load_config(args.config)
        
        # Инициализируем MT5
        if not initialize_mt5():
            return
        
        # Преобразуем таймфрейм
        timeframe = get_timeframe_from_config(timeframe_str)
        
        # Получаем исторические данные
        history_data = fetch_history(symbol, timeframe, start_date, end_date)
        
        if history_data is not None:
            # Сохраняем данные в файл
            save_to_file(history_data, args.output)
            
            print(f"Успешно получено {len(history_data)} бар(ов)")
        else:
            print("Не удалось получить исторические данные")
        
        # Закрываем соединение с MT5
        mt5.shutdown()
        
    except Exception as e:
        print(f"Ошибка выполнения программы: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()