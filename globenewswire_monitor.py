#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GlobeNewswire Real-Time Monitor with Finviz Integration
Архитектура:
- Умный поллинг с алгоритмом "дожима" (burst detection)
- Защита от блокировок (User-Agent ротация, задержки, обработка ошибок)
- Хранение новостей в папках по тикерам (news/TICKER/news_ID.json)
- Профиль компании с Finviz обновляется 1 раз в сутки
- База тикеров NASDAQ/NYSE/AMEX обновляется 1 раз в сутки
"""

import os
import sys
import json
import time
import random
import logging
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Set

import requests
from bs4 import BeautifulSoup

# --- КОНФИГУРАЦИЯ ---
CONFIG = {
    "dirs": {
        "data": "data",
        "news_root": "news",
        "cache": "cache"
    },
    "files": {
        "tickers_db": "tickers_db.json",
        "processed_news": "processed_news.json",
        "last_ticker_update": "last_ticker_update.txt"
    },
    "limits": {
        "ticker_cache_hours": 24,
        "profile_cache_hours": 24,
        "max_burst_checks": 5,       # Сколько раз проверить "на всякий случай" после находки
        "burst_delay_sec": 1.5,      # Пауза между проверками при burst
        "request_timeout": 10,
        "retry_attempts": 3
    },
    "urls": {
        "nasdaq": "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=10000&exchange=nasdaq&download=true",
        "nyse": "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=10000&exchange=nyse&download=true",
        "amex": "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=10000&exchange=amex&download=true",
        "categories": [
            "/news/consumer-products-services",
            "/news/banks-financial-services",
            "/news/energy",
            "/news/heathcare",
            "/news/industrials-utilities",
            "/news/technology-telecom"
        ]
    },
    "headers_base": {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Cache-Control": "max-age=0"
    }
}

# Пользователи для ротации (эмуляция разных браузеров/ОС)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("monitor.log", encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

class TickerDatabase:
    """Управление базой тикеров и сопоставление названий."""
    
    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.data_dir / CONFIG["files"]["tickers_db"]
        self.timestamp_path = self.data_dir / CONFIG["files"]["last_ticker_update"]
        self.tickers: Dict[str, str] = {} # Name -> Ticker
        self.ticker_info: Dict[str, dict] = {} # Ticker -> Info
        
    def is_stale(self) -> bool:
        """Проверка актуальности базы."""
        if not self.timestamp_path.exists():
            return True
        try:
            last_update = datetime.fromisoformat(self.timestamp_path.read_text())
            return datetime.now() - last_update > timedelta(hours=CONFIG["limits"]["ticker_cache_hours"])
        except Exception:
            return True

    def load(self) -> bool:
        """Загрузка базы из файла."""
        if not self.db_path.exists():
            return False
        try:
            data = json.loads(self.db_path.read_text(encoding='utf-8'))
            self.tickers = data.get("name_to_ticker", {})
            self.ticker_info = data.get("ticker_info", {})
            logger.info(f"Loaded {len(self.tickers)} tickers from cache.")
            return True
        except Exception as e:
            logger.error(f"Error loading ticker DB: {e}")
            return False

    def save(self):
        """Сохранение базы в файл."""
        data = {
            "name_to_ticker": self.tickers,
            "ticker_info": self.ticker_info,
            "updated_at": datetime.now().isoformat()
        }
        with open(self.db_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        self.timestamp_path.write_text(datetime.now().isoformat())
        logger.info(f"Saved {len(self.tickers)} tickers to cache.")

    def fetch_from_nasdaq_api(self) -> bool:
        """Скачивание данных с API NASDAQ."""
        session = self._create_session()
        all_rows = []
        
        exchanges = [
            ("NASDAQ", CONFIG["urls"]["nasdaq"]),
            ("NYSE", CONFIG["urls"]["nyse"]),
            ("AMEX", CONFIG["urls"]["amex"])
        ]
        
        for ex_name, url in exchanges:
            try:
                logger.info(f"Fetching {ex_name} data...")
                headers = session.headers.copy()
                headers.update({
                    "Referer": "https://www.nasdaq.com/",
                    "Origin": "https://www.nasdaq.com"
                })
                resp = session.get(url, headers=headers, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                
                if "data" in data and "rows" in data["data"]:
                    rows = data["data"]["rows"]
                    logger.info(f"Got {len(rows)} rows from {ex_name}")
                    all_rows.extend(rows)
                else:
                    logger.warning(f"No data found in response from {ex_name}")
                    
            except Exception as e:
                logger.error(f"Failed to fetch {ex_name}: {e}")
        
        if not all_rows:
            return False
            
        # Обработка данных
        for row in all_rows:
            symbol = row.get("symbol")
            name = row.get("name")
            if symbol and name:
                clean_name = " ".join(name.split()) 
                self.tickers[clean_name] = symbol
                self.ticker_info[symbol] = {
                    "name": name,
                    "exchange": row.get("exchange", ""),
                    "sector": row.get("sector", ""),
                    "industry": row.get("industry", "")
                }
                
        self.save()
        return True

    def find_ticker(self, source_name: str) -> Optional[str]:
        """Поиск тикера по названию компании из Source."""
        if not source_name:
            return None
            
        clean_source = " ".join(source_name.split())
        
        # Прямое совпадение
        if clean_source in self.tickers:
            return self.tickers[clean_source]
            
        # Пробуем убрать суффиксы типа Ltd, Inc, Corp, PLC, SA, AG и т.д.
        suffixes = [
            " Ltd.", " Ltd", " Inc.", " Inc", " Corp.", " Corp", " Corporation",
            " PLC", " Plc", " S.A.", " SA", " AG", " GmbH", " Holdings", 
            " Group", " The", " Co.", " Co", " L.P.", " LP", " N.V.", " NV"
        ]
        
        base_name = clean_source
        for suf in suffixes:
            if base_name.endswith(suf):
                base_name = base_name[:-len(suf)].strip()
                if base_name in self.tickers:
                    return self.tickers[base_name]
        
        return None

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        ua = random.choice(USER_AGENTS)
        session.headers.update(CONFIG["headers_base"])
        session.headers["User-Agent"] = ua
        return session


class NewsHistory:
    """Хранение истории обработанных новостей в JSON."""
    
    def __init__(self, data_dir: str):
        self.file_path = Path(data_dir) / CONFIG["files"]["processed_news"]
        self.urls: Set[str] = set()
        self.load()
        
    def load(self):
        if self.file_path.exists():
            try:
                data = json.loads(self.file_path.read_text(encoding='utf-8'))
                self.urls = set(data.get("urls", []))
                logger.info(f"Loaded {len(self.urls)} processed URLs from history.")
            except Exception as e:
                logger.error(f"Error loading history: {e}")
                self.urls = set()
        else:
            logger.info("No history file found. Starting fresh.")
            
    def save(self):
        data = {"urls": list(self.urls), "count": len(self.urls), "updated_at": datetime.now().isoformat()}
        with open(self.file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
            
    def is_processed(self, url: str) -> bool:
        return url in self.urls
    
    def add(self, url: str):
        self.urls.add(url)
        
    def cleanup_old(self, max_keep: int = 50000):
        """Ограничение размера файла истории."""
        if len(self.urls) > max_keep:
            lst = list(self.urls)
            self.urls = set(lst[-max_keep:])
            self.save()
            logger.info(f"Cleaned up history to {max_keep} entries.")


class FinvizScraper:
    """Скрапинг данных с Finviz с защитой."""
    
    def __init__(self, news_root: str):
        self.news_root = Path(news_root)
        
    def get_profile_path(self, ticker: str) -> Path:
        # Профиль храним в папке тикера: news/TICKER/profile.json
        ticker_dir = self.news_root / ticker
        return ticker_dir / "profile.json"

    def should_update(self, ticker: str) -> bool:
        path = self.get_profile_path(ticker)
        if not path.exists():
            return True
        try:
            mtime = datetime.fromtimestamp(path.stat().st_mtime)
            return datetime.now() - mtime > timedelta(hours=CONFIG["limits"]["profile_cache_hours"])
        except Exception:
            return True

    def fetch(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Парсинг страницы Finviz."""
        url = f"https://finviz.com/quote.ashx?t={ticker}"
        session = requests.Session()
        
        ua = random.choice(USER_AGENTS)
        session.headers.update(CONFIG["headers_base"])
        session.headers["User-Agent"] = ua
        session.headers["Referer"] = "https://www.google.com/"
        
        try:
            logger.info(f"Fetching Finviz profile for {ticker}...")
            resp = session.get(url, timeout=15)
            
            if resp.status_code == 403:
                logger.warning(f"Finviz returned 403 for {ticker}. Possible block.")
                return None
            resp.raise_for_status()
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            tables = soup.find_all('table', {'class': 'snapshot-table2'})
            if not tables:
                tables = soup.find_all('table', class_='snapshot-table2')
                
            if not tables:
                logger.warning(f"No profile table found for {ticker} on Finviz.")
                return None
                
            table = tables[0]
            rows = table.find_all('tr')
            
            profile_data = {"ticker": ticker, "fetched_at": datetime.now().isoformat(), "fields": {}}
            
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 2:
                    for i in range(0, len(cols), 2):
                        label = cols[i].text.strip()
                        value = cols[i+1].text.strip()
                        if label and value != '-':
                            profile_data["fields"][label] = value
                            
            target_path = self.get_profile_path(ticker)
            target_path.parent.mkdir(parents=True, exist_ok=True)
            with open(target_path, 'w', encoding='utf-8') as f:
                json.dump(profile_data, f, indent=2, ensure_ascii=False)
                
            logger.info(f"Saved Finviz profile for {ticker} ({len(profile_data['fields'])} fields).")
            return profile_data
            
        except Exception as e:
            logger.error(f"Error fetching Finviz for {ticker}: {e}")
            return None


class GlobeNewswireMonitor:
    """Основной класс монитора."""
    
    def __init__(self):
        self.base_url = "https://www.globenewswire.com"
        self.session = self._create_session()
        
        self.ticker_db = TickerDatabase(CONFIG["dirs"]["data"])
        self.history = NewsHistory(CONFIG["dirs"]["data"])
        self.finviz = FinvizScraper(CONFIG["dirs"]["news_root"])
        
        Path(CONFIG["dirs"]["news_root"]).mkdir(exist_ok=True)
        
    def _create_session(self) -> requests.Session:
        session = requests.Session()
        ua = random.choice(USER_AGENTS)
        session.headers.update(CONFIG["headers_base"])
        session.headers["User-Agent"] = ua
        return session

    def refresh_tickers_if_needed(self):
        """Обновление базы тикеров если устарела."""
        if self.ticker_db.is_stale():
            logger.info("Ticker database is stale or missing. Updating...")
            if self.ticker_db.fetch_from_nasdaq_api():
                logger.info("Ticker database updated successfully.")
            else:
                if self.ticker_db.load():
                    logger.warning("Failed to update tickers, using cached version.")
                else:
                    logger.error("CRITICAL: No ticker database available!")
                    raise RuntimeError("No ticker database")
        else:
            if not self.ticker_db.load():
                logger.info("Ticker cache file missing. Fetching now...")
                self.ticker_db.fetch_from_nasdaq_api()

    def get_category_urls(self) -> List[str]:
        return [f"{self.base_url}{cat}" for cat in CONFIG["urls"]["categories"]]

    def fetch_article_links(self, category_url: str) -> List[str]:
        links = []
        try:
            self.session.headers["User-Agent"] = random.choice(USER_AGENTS)
            
            resp = self.session.get(category_url, timeout=CONFIG["limits"]["request_timeout"])
            if resp.status_code == 403:
                logger.warning(f"403 Forbidden from {category_url}")
                return links
            resp.raise_for_status()
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            for link in soup.find_all('a', href=True):
                href = link['href']
                if '/news-release/' in href or '/release/' in href:
                    full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                    links.append(full_url)
                    
            logger.debug(f"Found {len(links)} links in {category_url}")
            
        except Exception as e:
            logger.error(f"Error fetching links from {category_url}: {e}")
            
        return list(set(links))

    def scrape_article(self, url: str) -> Optional[Dict[str, Any]]:
        try:
            self.session.headers["User-Agent"] = random.choice(USER_AGENTS)
            resp = self.session.get(url, timeout=CONFIG["limits"]["request_timeout"])
            resp.raise_for_status()
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # Заголовок
            title_tag = soup.find('h1')
            title = title_tag.text.strip() if title_tag else "No Title"
            
            source_company = None
            
            # Метод 1: Ищем в JSON-LD структурированных данных (самый надежный способ)
            for script in soup.find_all('script', type='application/ld+json'):
                try:
                    import json as json_mod
                    data = json_mod.loads(script.string)
                    # Проверяем author или publisher
                    if isinstance(data, dict):
                        author = data.get('author', {})
                        if isinstance(author, dict) and 'name' in author:
                            source_company = author['name']
                            break
                        # Проверяем sourceOrganization
                        source_org = data.get('sourceOrganization', [])
                        if isinstance(source_org, list) and len(source_org) > 0:
                            if isinstance(source_org[0], dict) and 'name' in source_org[0]:
                                source_company = source_org[0]['name']
                                break
                except Exception:
                    continue
            
            # Метод 2: Ищем в meta теге twitter:site или article:author
            if not source_company:
                meta_author = soup.find('meta', {'name': 'twitter:site'})
                if meta_author and meta_author.get('content'):
                    source_company = meta_author['content']
                    
            # Метод 3: Ищем в блоке .article-source или .gn-body-notes
            if not source_company:
                article_source = soup.find('span', class_='article-source')
                if article_source:
                    text = article_source.get_text()
                    if 'Source:' in text:
                        parts = text.split('Source:')
                        if len(parts) > 1:
                            raw_source = parts[1].strip()
                            # Берем первую строку
                            source_company = raw_source.split('\n')[0].strip()
            
            # Метод 4: Ищем в gn-body-notes
            if not source_company:
                body_notes = soup.find('div', class_='gn-body-notes')
                if body_notes:
                    text = body_notes.get_text()
                    if "Source:" in text:
                        parts = text.split("Source:")
                        if len(parts) > 1:
                            raw_source = parts[1].split('\n')[0].strip()
                            source_company = raw_source.strip()
            
            # Метод 5: Ищем в meta DCSext.Cluster
            if not source_company:
                meta = soup.find('meta', {'name': 'DCSext.Cluster'})
                if meta and meta.get('content'):
                    source_company = meta['content']
            
            # Текст новости
            content_div = soup.find('div', class_='gn-article-content') or soup.find('div', id='bodyText')
            content = ""
            if content_div:
                paragraphs = content_div.find_all('p')
                content = "\n".join([p.get_text(strip=True) for p in paragraphs])
            
            if not source_company:
                logger.warning(f"Could not find Source company for {url}")
                return None
                
            return {
                "url": url,
                "title": title,
                "source_company": source_company,
                "content": content,
                "scraped_at": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error scraping article {url}: {e}")
            return None

    def save_news(self, ticker: str, article: Dict[str, Any]):
        ticker_dir = Path(CONFIG["dirs"]["news_root"]) / ticker
        ticker_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        url_hash = hashlib.md5(article['url'].encode()).hexdigest()[:6]
        filename = f"news_{timestamp}_{url_hash}.json"
        filepath = ticker_dir / filename
        
        data = {
            "ticker": ticker,
            "company": article['source_company'],
            "title": article['title'],
            "url": article['url'],
            "content": article['content'],
            "received_at": datetime.now().isoformat()
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            
        logger.info(f"Saved news to {filepath}")
        
        if self.finviz.should_update(ticker):
            self.finviz.fetch(ticker)
        else:
            logger.debug(f"Finviz profile for {ticker} is up-to-date (cached).")

    def run_burst_check(self):
        self.refresh_tickers_if_needed()
        
        categories = self.get_category_urls()
        total_new_found = 0
        
        logger.info("Starting news scan...")
        new_links = set()
        
        for cat_url in categories:
            links = self.fetch_article_links(cat_url)
            for link in links:
                if not self.history.is_processed(link):
                    new_links.add(link)
        
        if not new_links:
            logger.info("No new news found.")
            return 0
            
        logger.info(f"Found {len(new_links)} new articles. Processing...")
        
        burst_rounds = 0
        while burst_rounds < CONFIG["limits"]["max_burst_checks"]:
            processed_count = 0
            for url in list(new_links):
                article = self.scrape_article(url)
                if article:
                    ticker = self.ticker_db.find_ticker(article['source_company'])
                    if ticker:
                        logger.info(f"Matched '{article['source_company']}' -> {ticker}")
                        self.save_news(ticker, article)
                        self.history.add(url)
                        processed_count += 1
                    else:
                        logger.info(f"Company '{article['source_company']}' not in DB. Ignoring.")
                        self.history.add(url)
                else:
                    self.history.add(url)
            
            total_new_found += processed_count
            new_links.clear()
            
            if processed_count > 0 or burst_rounds == 0:
                burst_rounds += 1
                if burst_rounds < CONFIG["limits"]["max_burst_checks"]:
                    wait_time = CONFIG["limits"]["burst_delay_sec"] + random.uniform(0.2, 0.5)
                    logger.info(f"Burst mode: waiting {wait_time:.2f}s before next check (Round {burst_rounds})...")
                    time.sleep(wait_time)
                    
                    for cat_url in categories:
                        links = self.fetch_article_links(cat_url)
                        for link in links:
                            if not self.history.is_processed(link):
                                new_links.add(link)
                    
                    if not new_links:
                        logger.info("Burst check: no more new links.")
                        break
                else:
                    break
            else:
                break
                
        self.history.save()
        logger.info(f"Run finished. Processed {total_new_found} new articles.")
        return total_new_found

def main():
    try:
        monitor = GlobeNewswireMonitor()
        monitor.run_burst_check()
    except KeyboardInterrupt:
        logger.info("Stopped by user.")
    except Exception as e:
        logger.exception(f"Critical error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
