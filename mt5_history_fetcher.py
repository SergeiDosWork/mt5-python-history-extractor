import logging
from datetime import datetime
import argparse
import configparser
import pandas as pd

# Попытка импорта MetaTrader5 с обработкой ошибки
try:
    import MetaTrader5 as mt5
    MT5_AVAILABLE = True
except ImportError:
    print("Предупреждение: MetaTrader5 недоступен. Будет использован режим генерации тестовых данных.")
    MT5_AVAILABLE = False
    import random


def initialize_mt5():
    """Инициализация подключения к MetaTrader 5"""
    if not MT5_AVAILABLE:
        print("Режим генерации тестовых данных - пропуск инициализации MT5")
        return True
    
    if not mt5.initialize():
        print(f"Ошибка инициализации MetaTrader 5: {mt5.last_error()}")
        return False
    # request connection status and parameters
    print(mt5.terminal_info())
    # get data on MetaTrader 5 version
    print(mt5.version())
    # symbols = mt5.symbols_get()
    # df = pd.DataFrame(symbols)
    # df.to_csv("symbols.csv", index=False, encoding="utf-8")
    return True


def get_timeframe_from_config(timeframe_str):
    """Преобразование строкового значения таймфрейма в константу MT5"""
    if MT5_AVAILABLE:
        # Режим с MT5
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
    else:
        # Режим без MT5 - используем числовые значения для справочника
        timeframe_map = {
            'M1': 1,
            'M2': 2,
            'M3': 3,
            'M4': 4,
            'M5': 5,
            'M10': 10,
            'M15': 15,
            'M30': 30,
            'H1': 60,
            'H2': 120,
            'H3': 180,
            'H4': 240,
            'H6': 360,
            'H8': 480,
            'H12': 720,
            'D1': 1440,
            'W1': 10080,
            'MN1': 43200
        }
    
    timeframe_str = timeframe_str.upper()
    if timeframe_str not in timeframe_map:
        raise ValueError(f"Неизвестный таймфрейм: {timeframe_str}. Доступные значения: {list(timeframe_map.keys())}")
    
    return timeframe_map[timeframe_str]

def format_time(time):
    """Преобразование unix time в формат строки"""
    time_format = "%Y-%m-%d %H:%M:%S"
    if time > 0:
        exp_dt = datetime.fromtimestamp(time)
        exp_str = exp_dt.strftime(time_format)
    else:
        exp_str = "Нет"
    return exp_str

def get_full_history(symbol, timeframe):
    all_rates = []
    last_time = datetime.now()
    count = 10000  # максимальное количество свечей за раз

    while True:
        # Получаем данные от last_time назад
        rates = mt5.copy_rates_from(symbol, timeframe, last_time, count)
        
        if rates is None or len(rates) == 0:
            break

        # Преобразуем в DataFrame для удобства
        df = pd.DataFrame(rates)
        df['time'] = pd.to_datetime(df['time'], unit='s')
        
        # Добавляем в общий список
        all_rates.append(df)
        
        # Обновляем last_time — самая ранняя свеча в этом блоке
        last_time = df['time'].min()
        
        # Если получено меньше, чем запрашивали — достигли начала
        if len(rates) < count:
            break
    
    if not all_rates:
        return pd.DataFrame()
    
    # Объединяем и сортируем по времени
    full_df = pd.concat(all_rates).sort_values('time').reset_index(drop=True)
    return full_df

def fetch_history(symbol, timeframe, start_date, end_date):
    """Получение исторических данных из MetaTrader 5"""
    # Преобразуем строки дат в объекты datetime
    start_dt = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
    
    print(f"Получение истории для {symbol} с {start_dt} по {end_dt}, таймфрейм {timeframe}")
    
    if MT5_AVAILABLE:
        # Режим с MT5
        if not mt5.symbol_select(symbol, True):
            print(f"Символ {symbol} не найден")
            return
        symbol_info = mt5.symbol_info(symbol)
        print(f"Дата символа: {format_time(symbol_info.time)}")
        print(f"Дата начала для символа: {format_time(symbol_info.start_time)}")
        print(f"Дата экспирации для символа: {format_time(symbol_info.expiration_time)}")
        tick_value = symbol_info.trade_tick_value  # Стоимость одного тика в валюте счёта
        tick_size = symbol_info.trade_tick_size    # Размер тика в цене
        curr = symbol_info.currency_base    # Размер тика в цене
        print(f"Стоимость 1 пункта: {tick_value} {symbol_info.currency_profit} {curr}")
        

        # Получаем бары
        #rates = mt5.copy_rates_range(symbol, timeframe, start_dt, end_dt)
        rates = get_full_history(symbol, timeframe)
        
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
    else:
        # Режим без MT5 - генерация тестовых данных
        import random
        from datetime import timedelta
        
        # Генерируем фиктивные данные для тестирования
        # Вычисляем количество минут между датами
        diff = end_dt - start_dt
        minutes_diff = int(diff.total_seconds() / 60)
        
        history_data = []
        current_time = start_dt
        
        for i in range(min(minutes_diff + 1, 100)):  # Ограничиваем до 100 записей для примера
            bar = {
                'time': current_time.strftime('%Y-%m-%d %H:%M:%S'),
                'open': round(random.uniform(1.0, 2.0), 5),
                'high': round(random.uniform(1.0, 2.0), 5),
                'low': round(random.uniform(1.0, 2.0), 5),
                'close': round(random.uniform(1.0, 2.0), 5),
                'volume': random.randint(0, 1000)
            }
            
            # Убедимся, что high >= max(open, close), low <= min(open, close)
            high_val = max(bar['open'], bar['close'])
            low_val = min(bar['open'], bar['close'])
            
            if bar['high'] < high_val:
                bar['high'] = round(high_val + random.uniform(0.0001, 0.01), 5)
                
            if bar['low'] > low_val:
                bar['low'] = round(low_val - random.uniform(0.0001, 0.01), 5)
            
            history_data.append(bar)
            current_time += timedelta(minutes=1)
        
        return history_data


def save_to_parquet(df, filename):
    """Сохранение DataFrame в файл Parquet"""
    df.to_parquet(filename, engine='pyarrow', index=False)
    
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
    parser.add_argument('-o', '--output', type=str, default=None,
                        help='Файл для сохранения результатов (по умолчанию: тикер_дата_от_дата_до.parquet)')
    
    args = parser.parse_args()
    
    try:
        # Загружаем конфигурацию
        symbol, start_date, end_date, timeframe_str = load_config(args.config)
        
        # Если не указан output, формируем имя файла автоматически
        if args.output is None:
            # Форматируем даты для использования в имени файла
            formatted_start = start_date.replace(" ", "_").replace(":", "-")
            formatted_end = end_date.replace(" ", "_").replace(":", "-")
            args.output = f"{symbol}_{formatted_start}_{formatted_end}.parquet"
        
        # Инициализируем MT5
        if not initialize_mt5():
            return
        
        # Преобразуем таймфрейм
        timeframe = get_timeframe_from_config(timeframe_str)
        
        # Получаем исторические данные
        history_data = fetch_history(symbol, timeframe, start_date, end_date)
        
        if history_data is not None:
            # Преобразуем данные в pandas DataFrame
            df = pd.DataFrame(history_data)
            
            # Сохраняем DataFrame в файл Parquet
            save_to_parquet(df, args.output)
            
            print(f"Успешно получено {len(history_data)} бар(ов)")
        else:
            print("Не удалось получить исторические данные")
        
        # Закрываем соединение с MT5
        if MT5_AVAILABLE:
            mt5.shutdown()
        
    except Exception as e:
        print(f"Ошибка выполнения программы: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
