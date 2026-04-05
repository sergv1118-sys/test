import requests
import json
import os
from datetime import datetime

# Конфигурация
OUTPUT_DIR = "data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "tickers_db.json")

# Источники данных (официальный API скринера NASDAQ)
SOURCES = [
    {
        "name": "NASDAQ",
        "url": "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=10000&exchange=nasdaq&download=true"
    },
    {
        "name": "NYSE",
        "url": "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=10000&exchange=nyse&download=true"
    },
    {
        "name": "AMEX",
        "url": "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=10000&exchange=amex&download=true"
    }
]

# Заголовки для имитации браузера (важно для доступа к API)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nasdaq.com/",
    "Connection": "keep-alive"
}

def fetch_exchange_data(source):
    """Загружает данные по конкретной бирже."""
    print(f"Загрузка данных с {source['name']}...")
    try:
        response = requests.get(source['url'], headers=HEADERS, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Сохраним полный ответ в файл для анализа структуры
        debug_file = f"data/debug_{source['name']}_raw.json"
        with open(debug_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"  -> Полный ответ сохранен в {debug_file} для анализа.")
        
        # Печатаем ключи верхнего уровня
        print(f"  -> Ключи верхнего уровня: {list(data.keys())}")
        
        # Пробуем найти данные в разных возможных местах
        rows = []
        if 'data' in data:
            inner_data = data['data']
            print(f"  -> Ключи внутри 'data': {list(inner_data.keys()) if isinstance(inner_data, dict) else 'Not a dict'}")
            
            # Вариант 1: data -> data -> rows (старый формат)
            if isinstance(inner_data, dict) and 'data' in inner_data and 'rows' in inner_data['data']:
                rows = inner_data['data']['rows']
                print(f"  -> Найдено через data.data.rows: {len(rows)} записей.")
            
            # Вариант 2: data -> rows (новый формат?)
            elif isinstance(inner_data, dict) and 'rows' in inner_data:
                rows = inner_data['rows']
                print(f"  -> Найдено через data.rows: {len(rows)} записей.")
                
            # Вариант 3: data - это сразу список?
            elif isinstance(inner_data, list):
                rows = inner_data
                print(f"  -> data является списком: {len(rows)} записей.")
        
        # Если ничего не нашли, пробуем искать 'rows' на верхнем уровне или в других местах
        if not rows and 'rows' in data:
            rows = data['rows']
            print(f"  -> Найдено через root.rows: {len(rows)} записей.")
            
        if not rows:
            print(f"  -> WARNING: Не удалось найти массив с данными в ответе от {source['name']}.")
            return []
            
        print(f"  -> Успешно извлечено {len(rows)} записей с {source['name']}.")
        return rows
            
    except requests.exceptions.RequestException as e:
        print(f"  -> ERROR при запросе к {source['name']}: {e}")
        return []
    except json.JSONDecodeError:
        print(f"  -> ERROR: Не удалось распарсить JSON от {source['name']}.")
        return []

def normalize_company_name(name):
    """Приводит название компании к стандартному виду для лучшего сопоставления."""
    if not name:
        return ""
    # Убираем лишние пробелы, приводим к нижнему регистру для сравнения later, но храним оригинал
    return name.strip()

def main():
    print("="*40)
    print("Запуск загрузки базы тикеров")
    print("="*40)
    
    # Создаем папку для данных, если нет
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    
    all_tickers = []
    seen_symbols = set()
    
    # Сбор данных со всех источников
    for source in SOURCES:
        rows = fetch_exchange_data(source)
        for row in rows:
            # row - это словарь с данными о компании
            symbol = row.get('symbol', '')
            
            # Проверка на дубликаты (иногда одни и те же компании могут быть в разных списках или ошибки API)
            if symbol and symbol not in seen_symbols:
                seen_symbols.add(symbol)
                
                # Формируем чистую запись
                ticker_entry = {
                    "symbol": symbol,
                    "name": row.get('name', ''),
                    "exchange": source['name'],
                    # Сохраняем все остальные поля, которые пришли с API, для анализа
                    "raw_data": row 
                }
                all_tickers.append(ticker_entry)
    
    print("-" * 40)
    print(f"Всего уникальных тикеров собрано: {len(all_tickers)}")
    
    # Сохранение результата
    output_data = {
        "last_updated": datetime.now().isoformat(),
        "total_count": len(all_tickers),
        "tickers": all_tickers
    }
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    
    print(f"Данные успешно сохранены в файл: {OUTPUT_FILE}")
    print("="*40)
    
    # Анализ полей
    if all_tickers:
        print("\nАнализ структуры данных (поля из raw_data первой компании):")
        sample_keys = list(all_tickers[0]['raw_data'].keys())
        print(f"Доступные поля: {sample_keys}")
        print("\nПример записи (первая компания):")
        print(json.dumps(all_tickers[0]['raw_data'], indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
