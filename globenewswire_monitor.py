#!/usr/bin/env python3
"""
GlobeNewswire Real-Time News Monitor

Мониторит новости с GlobeNewswire, извлекает компанию из поля "Source",
сверяет с базой тикеров (NASDAQ/NYSE) и сохраняет новости в файлы <TICKER>.txt
"""

import os
import re
import time
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Set, Dict, List
from dataclasses import dataclass

import requests
from bs4 import BeautifulSoup
import pandas as pd
import schedule

# Конфигурация
CONFIG = {
    'base_url': 'https://www.globenewswire.com',
    'news_categories': [
        '/news/consumer-products-services',
        '/news/energy',
        '/news/banks-financial-services',
        '/news/heathcare',
        '/news/industrials-utilities',
        '/news/technology-telecom',
    ],
    'output_dir': './news_output',
    'check_interval_minutes': 5,
    'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'max_news_per_run': 50,
    'ticker_db_file': 'ticker_database.csv',
}


@dataclass
class NewsArticle:
    """Представляет новость"""
    title: str
    url: str
    published_date: str
    content: str
    source_company: str  # Компания из поля Source
    ticker: Optional[str] = None
    scraped_at: str = None
    
    def __post_init__(self):
        if self.scraped_at is None:
            self.scraped_at = datetime.utcnow().isoformat()


class TickerDatabase:
    """База данных тикеров и компаний"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.df: Optional[pd.DataFrame] = None
        self.company_to_ticker: Dict[str, str] = {}
        
    def download_database(self) -> bool:
        """Скачивает актуальные списки тикеров NASDAQ и NYSE"""
        logging.info("Downloading ticker database from official sources...")
        
        nasdaq_url = "http://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
        nyse_url = "http://www.nasdaqtrader.com/dynamic/symdir/nyseotherlisted.txt"
        
        all_data = []
        
        try:
            # Скачиваем NASDAQ
            logging.info("Fetching NASDAQ list...")
            df_nasdaq = pd.read_csv(nasdaq_url, sep='|', skiprows=lambda x: x > 0 and not str(x).isdigit(), on_bad_lines='skip')
            # Читаем как простой текст с разделителем |
            try:
                df_nasdaq = pd.read_csv(nasdaq_url, sep='|')
            except:
                # Если не получается, читаем построчно
                import io
                response = requests.get(nasdaq_url, timeout=10)
                lines = response.text.split('\n')
                valid_lines = []
                for line in lines:
                    parts = line.split('|')
                    if len(parts) >= 2:
                        valid_lines.append(line)
                if valid_lines:
                    df_nasdaq = pd.read_csv(io.StringIO('\n'.join(valid_lines)), sep='|')
                else:
                    df_nasdaq = None
            
            if df_nasdaq is not None and 'Symbol' in df_nasdaq.columns and 'Security Name' in df_nasdaq.columns:
                df_nasdaq = df_nasdaq[['Symbol', 'Security Name']].copy()
                df_nasdaq['Exchange'] = 'NASDAQ'
                all_data.append(df_nasdaq)
                
            # Скачиваем NYSE
            logging.info("Fetching NYSE list...")
            try:
                df_nyse = pd.read_csv(nyse_url, sep='|')
            except:
                import io
                response = requests.get(nyse_url, timeout=10)
                lines = response.text.split('\n')
                valid_lines = []
                for line in lines:
                    parts = line.split('|')
                    if len(parts) >= 2:
                        valid_lines.append(line)
                if valid_lines:
                    df_nyse = pd.read_csv(io.StringIO('\n'.join(valid_lines)), sep='|')
                else:
                    df_nyse = None
                    
            if df_nyse is not None and 'ACT Symbol' in df_nyse.columns and 'Security Name' in df_nyse.columns:
                df_nyse = df_nyse.rename(columns={'ACT Symbol': 'Symbol'})
                df_nyse = df_nyse[['Symbol', 'Security Name']].copy()
                df_nyse['Exchange'] = 'NYSE'
                all_data.append(df_nyse)
            
            if all_data:
                self.df = pd.concat(all_data, ignore_index=True)
                self._build_index()
                self.df.to_csv(self.db_path, index=False)
                logging.info(f"Database saved to {self.db_path}. Total records: {len(self.df)}")
                return True
            else:
                logging.error("Failed to parse ticker data from source.")
                return False
                
        except Exception as e:
            logging.error(f"Error downloading ticker database: {e}")
            if os.path.exists(self.db_path):
                logging.info("Loading existing local database...")
                self.load_local()
                return True
            return False

    def load_local(self) -> bool:
        """Загружает базу из локального файла"""
        if not os.path.exists(self.db_path):
            logging.warning("Local ticker database not found.")
            return False
        
        try:
            self.df = pd.read_csv(self.db_path)
            self._build_index()
            logging.info(f"Loaded {len(self.df)} tickers from local database.")
            return True
        except Exception as e:
            logging.error(f"Error loading local database: {e}")
            return False

    def _normalize_name(self, name: str) -> str:
        """Нормализует название компании для поиска"""
        if not isinstance(name, str):
            return ""
        
        suffixes = [
            " inc", " corp", " ltd", " llc", " co", 
            " company", " corporation", " limited", 
            " plc", " gmbh", " ag", " sa", " nv"
        ]
        
        clean = name.lower().strip()
        clean = re.sub(r'[^\w\s]', '', clean)
        
        for suffix in suffixes:
            if clean.endswith(suffix):
                clean = clean[:-len(suffix)].strip()
        
        return clean

    def _build_index(self):
        """Строит индекс для быстрого поиска"""
        self.company_to_ticker = {}
        if self.df is None:
            return

        for _, row in self.df.iterrows():
            symbol = row['Symbol']
            name = row['Security Name']
            
            clean_name = self._normalize_name(name)
            self.company_to_ticker[clean_name] = symbol
            
            # Добавляем частичные совпадения
            parts = clean_name.split()
            for i in range(1, len(parts) + 1):
                partial = " ".join(parts[:i])
                if partial not in self.company_to_ticker:
                    self.company_to_ticker[partial] = symbol

    def find_ticker(self, source_name: str) -> Optional[str]:
        """Ищет тикер по названию компании из Source"""
        if not source_name or not self.company_to_ticker:
            return None
            
        clean_source = self._normalize_name(source_name)
        
        # Точное совпадение
        if clean_source in self.company_to_ticker:
            return self.company_to_ticker[clean_source]
            
        # Поиск по подстроке (самое длинное совпадение)
        best_match = None
        max_len = 0
        
        for db_name, ticker in self.company_to_ticker.items():
            if db_name in clean_source or clean_source in db_name:
                if len(db_name) > max_len:
                    max_len = len(db_name)
                    best_match = ticker
        
        return best_match


class GlobeNewswireMonitor:
    """Монитор новостей GlobeNewswire"""
    
    def __init__(self, config: Dict = None):
        self.config = config or CONFIG
        self.output_dir = Path(self.config['output_dir'])
        self.processed_urls: Set[str] = set()
        self.ticker_db = TickerDatabase(self.config['ticker_db_file'])
        
        # Настройка логирования
        self._setup_logging()
        
        # Создаем директорию
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Загружаем кэш
        self._load_processed_urls()
        
        # Сессия
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.config['user_agent'],
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        })
    
    def _setup_logging(self):
        """Настройка логирования"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('news_monitor.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def _load_processed_urls(self):
        """Загрузка обработанных URL"""
        cache_file = self.output_dir / 'processed_urls.json'
        if cache_file.exists():
            try:
                with open(cache_file, 'r') as f:
                    data = json.load(f)
                    self.processed_urls = set(data.get('urls', []))
                    self.logger.info(f"Loaded {len(self.processed_urls)} previously processed URLs")
            except Exception as e:
                self.logger.warning(f"Could not load processed URLs cache: {e}")
    
    def _save_processed_urls(self):
        """Сохранение обработанных URL"""
        cache_file = self.output_dir / 'processed_urls.json'
        try:
            with open(cache_file, 'w') as f:
                json.dump({'urls': list(self.processed_urls)}, f)
        except Exception as e:
            self.logger.error(f"Could not save processed URLs cache: {e}")
    
    def get_news_links(self, category_url: str) -> List[Dict[str, str]]:
        """Получение ссылок на новости"""
        articles = []
        
        try:
            url = f"{self.config['base_url']}{category_url}"
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'lxml')
            
            links = soup.find_all('a', href=re.compile(r'/news-release/\d{4}/\d{2}/\d{2}/'))
            
            for link in links[:self.config['max_news_per_run'] // len(self.config['news_categories'])]:
                href = link.get('href', '')
                if href and href.startswith('/'):
                    full_url = f"{self.config['base_url']}{href}"
                    title = link.get_text(strip=True)
                    
                    if title and full_url not in self.processed_urls:
                        articles.append({
                            'url': full_url,
                            'title': title
                        })
            
            self.logger.info(f"Found {len(articles)} new articles in {category_url}")
            
        except Exception as e:
            self.logger.error(f"Error fetching news from {category_url}: {e}")
        
        return articles
    
    def scrape_article(self, url: str) -> Optional[NewsArticle]:
        """Парсинг статьи"""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'lxml')
            
            # Заголовок
            title_tag = soup.find('h1', class_=re.compile(r'article-title|headline', re.I))
            if not title_tag:
                title_tag = soup.find('h1')
            title = title_tag.get_text(strip=True) if title_tag else "Unknown Title"
            
            # Дата
            date_tag = soup.find('div', class_=re.compile(r'date|time|published', re.I))
            if not date_tag:
                date_tag = soup.find('time')
            published_date = date_tag.get_text(strip=True) if date_tag else datetime.utcnow().strftime('%Y-%m-%d')
            
            # Источник (КЛЮЧЕВОЙ МОМЕНТ!)
            source_company = None
            source_label = soup.find(string=re.compile(r"Source:", re.IGNORECASE))
            
            if source_label:
                parent = source_label.parent
                next_sibling = parent.next_sibling
                if next_sibling:
                    source_company = next_sibling.strip()
                else:
                    full_text = parent.get_text()
                    if ":" in full_text:
                        source_company = full_text.split(":", 1)[1].strip()
            
            if not source_company:
                meta_source = soup.find("meta", attrs={"name": "author"})
                if meta_source:
                    source_company = meta_source.get("content")
            
            # Контент
            content_parts = []
            content_div = soup.find('div', class_=re.compile(r'article-body|content|release-content', re.I))
            if not content_div:
                content_div = soup.find('div', id=re.compile(r'article|content|body', re.I))
            
            if content_div:
                paragraphs = content_div.find_all('p')
                for p in paragraphs:
                    text = p.get_text(strip=True)
                    if text:
                        content_parts.append(text)
            
            content = '\n\n'.join(content_parts)
            
            if not content:
                content = soup.get_text(separator='\n', strip=True)
            
            article = NewsArticle(
                title=title,
                url=url,
                published_date=published_date,
                content=content,
                source_company=source_company or "Unknown"
            )
            
            self.logger.info(f"Scraped: {title[:50]}... | Source: {article.source_company}")
            return article
            
        except Exception as e:
            self.logger.error(f"Error scraping article {url}: {e}")
            return None
    
    def process_article(self, article: NewsArticle) -> bool:
        """Обработка статьи: поиск тикера и сохранение"""
        if not article.source_company or article.source_company == "Unknown":
            self.logger.debug(f"No source found, skipping.")
            return False
        
        # Поиск тикера в базе
        ticker = self.ticker_db.find_ticker(article.source_company)
        
        if not ticker:
            self.logger.info(f"Company '{article.source_company}' NOT found in database. Ignoring.")
            return False
        
        article.ticker = ticker
        self.logger.info(f"Matched '{article.source_company}' -> Ticker: {ticker}")
        
        # Сохранение в файл <TICKER>.txt
        filename = f"{ticker}.txt"
        filepath = self.output_dir / filename
        self._append_to_file(filepath, article)
        self.logger.info(f"Saved article to {filename}")
        
        return True
    
    def _append_to_file(self, filepath: Path, article: NewsArticle):
        """Добавление статьи в файл"""
        separator = "=" * 80
        
        with open(filepath, 'a', encoding='utf-8') as f:
            f.write(f"\n{separator}\n")
            f.write(f"DATE: {article.published_date}\n")
            f.write(f"SOURCE: {article.source_company}\n")
            f.write(f"TICKER: {article.ticker}\n")
            f.write(f"TITLE: {article.title}\n")
            f.write(f"URL: {article.url}\n")
            f.write(f"SCRAPED AT: {article.scraped_at}\n")
            f.write(f"{separator}\n\n")
            f.write(f"{article.content}\n")
            f.write(f"\n{separator}\n\n")
    
    def run_once(self):
        """Одиночный запуск"""
        self.logger.info("Starting news monitoring run...")
        
        # Проверка базы тикеров
        if not self.ticker_df or not self.ticker_db.company_to_ticker:
            if not self.ticker_db.load_local():
                self.logger.warning("Ticker database empty. Attempting to download...")
                if not self.ticker_db.download_database():
                    self.logger.error("Could not initialize ticker database. Exiting.")
                    return 0
        
        all_articles = []
        
        for category in self.config['news_categories']:
            articles = self.get_news_links(category)
            all_articles.extend(articles)
        
        self.logger.info(f"Total articles to process: {len(all_articles)}")
        
        processed_count = 0
        matched_count = 0
        
        for article_info in all_articles:
            if processed_count >= self.config['max_news_per_run']:
                break
            
            url = article_info['url']
            
            if url in self.processed_urls:
                continue
            
            article = self.scrape_article(url)
            
            if article:
                processed_count += 1
                if self.process_article(article):
                    matched_count += 1
                
                self.processed_urls.add(url)
        
        self._save_processed_urls()
        
        self.logger.info(f"Completed run. Processed {processed_count} articles, matched {matched_count} companies.")
        return matched_count
    
    def run_continuous(self):
        """Непрерывный мониторинг"""
        self.logger.info(f"Starting continuous monitoring (every {self.config['check_interval_minutes']} min)")
        
        schedule.every(self.config['check_interval_minutes']).minutes.do(self.run_once)
        
        self.run_once()
        
        while True:
            schedule.run_pending()
            time.sleep(1)
    
    @property
    def ticker_df(self):
        return self.ticker_db.df


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='GlobeNewswire News Monitor (Source-based)')
    parser.add_argument('--once', action='store_true', help='Run once and exit')
    parser.add_argument('--interval', type=int, default=5, help='Check interval in minutes')
    parser.add_argument('--output-dir', type=str, default='./news_output', help='Output directory')
    parser.add_argument('--update-tickers', action='store_true', help='Force update ticker database')
    parser.add_argument('--categories', nargs='+', help='News categories to monitor')
    
    args = parser.parse_args()
    
    config = CONFIG.copy()
    config['check_interval_minutes'] = args.interval
    config['output_dir'] = args.output_dir
    
    if args.categories:
        config['news_categories'] = args.categories
    
    monitor = GlobeNewswireMonitor(config)
    
    if args.update_tickers:
        monitor.ticker_db.download_database()
        return
    
    if args.once:
        count = monitor.run_once()
        print(f"\nProcessed. Matched {count} companies. Check {args.output_dir} for files.")
    else:
        print(f"Starting continuous monitoring...")
        print(f"Output directory: {args.output_dir}")
        print(f"Check interval: {args.interval} minutes")
        print("Press Ctrl+C to stop\n")
        monitor.run_continuous()


if __name__ == '__main__':
    main()
