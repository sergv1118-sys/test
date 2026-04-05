#!/usr/bin/env python3
"""
Скрипт для загрузки полной базы тикеров с NASDAQ, NYSE и AMEX.
Сохраняет данные в data/tickers_db.json
"""

import json
import os
import time
import random
from datetime import datetime
import requests
from pathlib import Path

# Константы
DATA_DIR = Path("data")
OUTPUT_FILE = DATA_DIR / "tickers_db.json"
LAST_UPDATE_FILE = DATA_DIR / "last_ticker_update.txt"

# Источники данных (официальные API скринеров)
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

# Заголовки для имитации браузера
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nasdaq.com/",
    "Connection": "keep-alive"
}


def load_existing_data():
    """Загружает существующую базу, если она есть."""
    if OUTPUT_FILE.exists():
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_data(data):
    """Сохраняет базу тикеров в JSON."""
    DATA_DIR.mkdir(exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"✓ Сохранено {len(data)} тикеров в {OUTPUT_FILE}")


def save_last_update():
    """Сохраняет время последнего обновления."""
    DATA_DIR.mkdir(exist_ok=True)
    with open(LAST_UPDATE_FILE, "w", encoding="utf-8") as f:
        f.write(datetime.now().isoformat())


def fetch_exchange_data(source):
    """Загружает данные по одной бирже с защитой от ошибок."""
    print(f"Загрузка данных с {source['name']}...")
    
    try:
        # Случайная задержка перед запросом (0.5-1.5 сек)
        time.sleep(random.uniform(0.5, 1.5))
        
        response = requests.get(
            source["url"],
            headers=HEADERS,
            timeout=30
        )
        
        if response.status_code == 429:
            print(f"⚠ Получен код 429 (Too Many Requests) от {source['name']}. Пропускаем.")
            return []
        
        response.raise_for_status()
        data = response.json()
        
        # Парсинг структуры ответа NASDAQ API
        rows = data.get("data", {}).get("rows", [])
        
        print(f"  → Получено {len(rows)} записей")
        return rows
        
    except requests.exceptions.RequestException as e:
        print(f"✗ Ошибка при загрузке {source['name']}: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"✗ Ошибка парсинга JSON от {source['name']}: {e}")
        return []


def normalize_ticker_data(raw_data, exchange_name):
    """Приводит данные к единому формату."""
    normalized = {}
    
    for item in raw_data:
        symbol = item.get("symbol", "").strip()
        if not symbol:
            continue
            
        normalized[symbol] = {
            "symbol": symbol,
            "name": item.get("name", "").strip(),
            "exchange": exchange_name,
            "lastsale": item.get("lastsale"),
            "netchange": item.get("netchange"),
            "pctchange": item.get("pctchange"),
            "volume": item.get("volume"),
            "marketCap": item.get("marketCap"),
            "country": item.get("country"),
            "ipoyear": item.get("ipoyear"),
            "industry": item.get("industry"),
            "sector": item.get("sector"),
            "updated_at": datetime.now().isoformat()
        }
    
    return normalized


def main():
    print("=" * 60)
    print("Загрузка базы тикеров с NASDAQ, NYSE, AMEX")
    print("=" * 60)
    
    # Проверяем, когда было последнее обновление
    last_update = None
    if LAST_UPDATE_FILE.exists():
        with open(LAST_UPDATE_FILE, "r", encoding="utf-8") as f:
            last_update_str = f.read().strip()
            try:
                last_update = datetime.fromisoformat(last_update_str)
                hours_since = (datetime.now() - last_update).total_seconds() / 3600
                
                if hours_since < 24:
                    print(f"Последнее обновление: {hours_since:.1f} ч. назад")
                    choice = input("Обновить базу сейчас? (y/n): ").strip().lower()
                    if choice != 'y':
                        print("Отменено пользователем.")
                        return
            except:
                pass
    
    # Загружаем существующие данные
    all_tickers = load_existing_data()
    print(f"Загружено {len(all_tickers)} существующих тикеров")
    
    total_new = 0
    
    # Проходим по всем источникам
    for source in SOURCES:
        print(f"\n--- {source['name']} ---")
        raw_data = fetch_exchange_data(source)
        
        if raw_data:
            normalized = normalize_ticker_data(raw_data, source["name"])
            
            # Объединяем с существующими данными
            new_count = 0
            for symbol, ticker_info in normalized.items():
                if symbol not in all_tickers:
                    new_count += 1
                all_tickers[symbol] = ticker_info
            
            print(f"  → Добавлено/обновлено: {len(normalized)} (новых: {new_count})")
            total_new += len(normalized)
        
        # Небольшая пауза между запросами к разным биржам
        if source != SOURCES[-1]:
            time.sleep(random.uniform(1.0, 2.0))
    
    # Сохраняем результат
    print("\n" + "=" * 60)
    save_data(all_tickers)
    save_last_update()
    
    print(f"\nИтого уникальных тикеров: {len(all_tickers)}")
    print(f"Распределение по биржам:")
    
    exchange_counts = {}
    for ticker in all_tickers.values():
        exch = ticker.get("exchange", "Unknown")
        exchange_counts[exch] = exchange_counts.get(exch, 0) + 1
    
    for exch, count in sorted(exchange_counts.items()):
        print(f"  {exch}: {count}")
    
    print("\n✓ Готово!")


if __name__ == "__main__":
    main()
