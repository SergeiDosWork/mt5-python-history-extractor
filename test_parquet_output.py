#!/usr/bin/env python3
"""
Тестовая версия приложения для демонстрации функциональности
преобразования данных в pandas DataFrame и сохранения в формат Parquet
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import argparse
import random


def generate_mock_mt5_data(symbol="EURUSD", start_date="2023-07-01 00:00:00", end_date="2023-07-02 23:59:59", count=100):
    """Генерация фиктивных данных, имитирующих получение из MT5"""
    start_dt = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
    
    # Создаем временные метки
    time_diff = (end_dt - start_dt) / count
    timestamps = [start_dt + i * time_diff for i in range(count)]
    
    # Генерируем OHLCV данные
    data = []
    base_price = 1.0500  # базовая цена для EURUSD
    
    for i, timestamp in enumerate(timestamps):
        price_change = random.uniform(-0.005, 0.005)  # изменение цены до ±50 пунктов
        open_price = base_price + price_change
        
        # Генерируем OHLC
        high = open_price + abs(random.uniform(0, 0.002))  # максимум выше открытия
        low = open_price - abs(random.uniform(0, 0.002))   # минимум ниже открытия
        close = low + random.random() * (high - low)       # закрытие между мин и макс
        volume = random.randint(100, 10000)                # объем
        
        bar = {
            'time': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'open': round(open_price, 5),
            'high': round(high, 5),
            'low': round(low, 5),
            'close': round(close, 5),
            'volume': volume
        }
        data.append(bar)
        
        base_price = close  # следующая цена основана на предыдущей цене закрытия
    
    return data


def save_to_parquet(df, filename):
    """Сохранение DataFrame в файл Parquet"""
    df.to_parquet(filename, engine='pyarrow', index=False)
    
    print(f"Данные сохранены в файл: {filename}")


def main():
    parser = argparse.ArgumentParser(description='Тестовое приложение для сохранения данных в формат Parquet')
    parser.add_argument('-o', '--output', type=str, default='test_history.parquet',
                        help='Файл для сохранения результатов (по умолчанию: test_history.parquet)')
    parser.add_argument('--symbol', type=str, default='EURUSD',
                        help='Символ для генерации тестовых данных (по умолчанию: EURUSD)')
    parser.add_argument('--start-date', type=str, default='2023-07-01 00:00:00',
                        help='Начальная дата (по умолчанию: 2023-07-01 00:00:00)')
    parser.add_argument('--end-date', type=str, default='2023-07-02 23:59:59',
                        help='Конечная дата (по умолчанию: 2023-07-02 23:59:59)')
    parser.add_argument('--count', type=int, default=1000,
                        help='Количество баров для генерации (по умолчанию: 1000)')
    
    args = parser.parse_args()
    
    try:
        print(f"Генерация тестовых данных для {args.symbol} с {args.start_date} по {args.end_date}")
        
        # Генерируем тестовые данные
        mock_data = generate_mock_mt5_data(
            symbol=args.symbol,
            start_date=args.start_date,
            end_date=args.end_date,
            count=args.count
        )
        
        print(f"Сгенерировано {len(mock_data)} бар(ов)")
        
        # Преобразуем данные в pandas DataFrame
        df = pd.DataFrame(mock_data)
        
        # Проверяем типы данных
        print(f"Типы данных в DataFrame:")
        print(df.dtypes)
        print(f"\nПервые 5 строк:")
        print(df.head())
        
        # Сохраняем DataFrame в файл Parquet
        save_to_parquet(df, args.output)
        
        print(f"Успешно завершено! Файл {args.output} создан.")
        
    except Exception as e:
        print(f"Ошибка выполнения программы: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()