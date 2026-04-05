#!/usr/bin/env python3
"""
Скрипт для загрузки полной базы тикеров и данных о компаниях
с официальных источников NASDAQ, NYSE, AMEX.
Сохраняет результат в data/tickers_db.json
"""

import requests
import json
import os
from datetime import datetime
from typing import List, Dict, Any

# Конфигурация
DATA_DIR = "data"
DB_FILE = os.path.join(DATA_DIR, "tickers_db.json")

# Источники данных (официальные API скринера NASDAQ)
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

# Заголовки для имитации браузера (защита от простых блокировок)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nasdaq.com/",
    "Connection": "keep-alive"
}


def fetch_exchange_data(source: Dict[str, str]) -> List[Dict[str, Any]]:
    """Загружает данные по одной бирже."""
    print(f"Загрузка данных с {source['name']}...")
    try:
        response = requests.get(source['url'], headers=HEADERS, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # Проверка структуры ответа
        if 'data' not in data or 'rows' not in data['data']:
            print(f"  ⚠️ Неожиданная структура ответа от {source['name']}")
            return []
        
        rows = data['data']['rows']
        print(f"  ✅ Получено {len(rows)} записей")
        return rows
        
    except requests.exceptions.RequestException as e:
        print(f"  ❌ Ошибка при загрузке {source['name']}: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"  ❌ Ошибка парсинга JSON от {source['name']}: {e}")
        return []


def normalize_ticker_data(raw_data: List[Dict[str, Any]], exchange_name: str) -> List[Dict[str, Any]]:
    """Приводит данные к единому формату и добавляет имя биржи."""
    normalized = []
    
    # Сопоставление ключей (может отличаться в разных ответах)
    # Обычно NASDAQ API возвращает ключи в нижнем регистре или camelCase
    for item in raw_data:
        # Базовая структура, которую мы хотим получить
        ticker_info = {
            "symbol": item.get("symbol", ""),
            "name": item.get("name", "") or item.get("companyName", ""),
            "lastsale": item.get("lastsale", ""),
            "netchange": item.get("netchange", ""),
            "pctchange": item.get("pctchange", ""),
            "volume": item.get("volume", ""),
            "marketCap": item.get("marketCap", ""),
            "country": item.get("country", ""),
            "ipoyear": item.get("ipoyear", ""),
            "industry": item.get("industry", ""),
            "sector": item.get("sector", ""),
            "exchange": exchange_name,
            "updated_at": datetime.now().isoformat()
        }
        
        # Пропускаем записи без тикера
        if not ticker_info["symbol"]:
            continue
            
        normalized.append(ticker_info)
    
    return normalized


def load_tickers_database() -> Dict[str, Dict[str, Any]]:
    """Основная функция загрузки и объединения данных."""
    print("="*50)
    print("Запуск загрузки базы тикеров")
    print("="*50)
    
    all_tickers = {}
    
    # Проходим по всем источникам
    for source in SOURCES:
        raw_rows = fetch_exchange_data(source)
        if raw_rows:
            normalized = normalize_ticker_data(raw_rows, source['name'])
            for item in normalized:
                symbol = item['symbol']
                # Если тикер уже есть (редко, но бывает на стыке бирж), оставляем первое вхождение
                if symbol not in all_tickers:
                    all_tickers[symbol] = item
    
    return all_tickers


def save_database(tickers: Dict[str, Dict[str, Any]]):
    """Сохраняет базу в JSON файл."""
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # Структура для сохранения
    output_data = {
        "meta": {
            "total_count": len(tickers),
            "generated_at": datetime.now().isoformat(),
            "sources": [s['name'] for s in SOURCES]
        },
        "tickers": tickers
    }
    
    with open(DB_FILE, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print(f"\n💾 База сохранена в {DB_FILE}")


def print_statistics(tickers: Dict[str, Dict[str, Any]]):
    """Выводит краткую статистику."""
    exchanges = {}
    sectors = {}
    countries = {}
    
    for data in tickers.values():
        exch = data.get('exchange', 'Unknown')
        exchanges[exch] = exchanges.get(exch, 0) + 1
        
        sector = data.get('sector', 'Unknown')
        if sector:
            sectors[sector] = sectors.get(sector, 0) + 1
            
        country = data.get('country', 'Unknown')
        if country:
            countries[country] = countries.get(country, 0) + 1

    print("\n" + "="*50)
    print("СТАТИСТИКА")
    print("="*50)
    print(f"Всего уникальных тикеров: {len(tickers)}")
    print("\nПо биржам:")
    for exch, count in sorted(exchanges.items(), key=lambda x: x[1], reverse=True):
        print(f"  {exch}: {count}")
    
    print("\nТоп-5 секторов:")
    top_sectors = sorted(sectors.items(), key=lambda x: x[1], reverse=True)[:5]
    for sector, count in top_sectors:
        print(f"  {sector}: {count}")
        
    print("\nТоп-5 стран:")
    top_countries = sorted(countries.items(), key=lambda x: x[1], reverse=True)[:5]
    for country, count in top_countries:
        print(f"  {country}: {count}")
    print("="*50)


def main():
    # Загрузка
    tickers = load_tickers_database()
    
    if not tickers:
        print("❌ Не удалось загрузить ни одного тикера. Проверьте соединение.")
        return
    
    # Сохранение
    save_database(tickers)
    
    # Статистика
    print_statistics(tickers)
    
    print("\n✅ Готово! База тикеров обновлена.")


if __name__ == "__main__":
    main()
