#!/usr/bin/env python3
"""
GlobeNewswire Real-Time News Monitor

This script monitors GlobeNewswire for press releases and news about publicly traded companies,
extracts company tickers, and saves news to files named by ticker symbol.
"""

import os
import re
import time
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Set, Dict, List
from dataclasses import dataclass, asdict

import requests
from bs4 import BeautifulSoup
import schedule

# Configuration
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
}

# Ticker patterns for different exchanges
TICKER_PATTERNS = [
    # NYSE, NASDAQ, Nasdaq-GS, Nasdaq-CM, Nasdaq-NMS
    r'(?:NYSE|NASDAQ|Nasdaq|Nasdaq-GS|Nasdaq-CM|Nasdaq-NMS)[:\s]*([A-Z]{1,5}(?:\.[A-Z]{1,2})?)',
    # Stock exchange in parentheses
    r'\((?:NYSE|NASDAQ|Nasdaq)[:\s]*([A-Z]{1,5}(?:\.[A-Z]{1,2})?)\)',
]

@dataclass
class NewsArticle:
    """Represents a news article"""
    title: str
    url: str
    published_date: str
    content: str
    tickers: List[str]
    source: str = 'GlobeNewswire'
    scraped_at: str = None
    
    def __post_init__(self):
        if self.scraped_at is None:
            self.scraped_at = datetime.utcnow().isoformat()


class GlobeNewswireMonitor:
    """Monitor GlobeNewswire for company news"""
    
    def __init__(self, config: Dict = None):
        self.config = config or CONFIG
        self.output_dir = Path(self.config['output_dir'])
        self.processed_urls: Set[str] = set()
        self.ticker_cache: Dict[str, List[NewsArticle]] = {}
        
        # Setup logging
        self._setup_logging()
        
        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Load previously processed URLs
        self._load_processed_urls()
        
        # Session for efficient requests
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.config['user_agent'],
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        })
    
    def _setup_logging(self):
        """Configure logging"""
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
        """Load previously processed URLs from cache file"""
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
        """Save processed URLs to cache file"""
        cache_file = self.output_dir / 'processed_urls.json'
        try:
            with open(cache_file, 'w') as f:
                json.dump({'urls': list(self.processed_urls)}, f)
        except Exception as e:
            self.logger.error(f"Could not save processed URLs cache: {e}")
    
    def extract_tickers(self, text: str) -> List[str]:
        """Extract stock tickers from text"""
        tickers = set()
        
        for pattern in TICKER_PATTERNS:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                # Clean up ticker symbol
                ticker = match.strip().upper()
                if ticker and len(ticker) <= 6:  # Valid ticker length
                    tickers.add(ticker)
        
        return sorted(list(tickers))
    
    def get_news_links(self, category_url: str) -> List[Dict[str, str]]:
        """Get news article links from a category page"""
        articles = []
        
        try:
            url = f"{self.config['base_url']}{category_url}"
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'lxml')
            
            # Find all news release links
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
        """Scrape a single news article"""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'lxml')
            
            # Extract title
            title_tag = soup.find('h1', class_=re.compile(r'article-title|headline', re.I))
            if not title_tag:
                title_tag = soup.find('h1')
            title = title_tag.get_text(strip=True) if title_tag else "Unknown Title"
            
            # Extract date
            date_tag = soup.find('div', class_=re.compile(r'date|time|published', re.I))
            if not date_tag:
                date_tag = soup.find('time')
            published_date = date_tag.get_text(strip=True) if date_tag else datetime.utcnow().strftime('%Y-%m-%d')
            
            # Extract main content
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
            
            # If no content found, use full page text
            if not content:
                content = soup.get_text(separator='\n', strip=True)
            
            # Extract tickers from content
            tickers = self.extract_tickers(content)
            
            # Also check title for tickers
            title_tickers = self.extract_tickers(title)
            tickers = sorted(list(set(tickers + title_tickers)))
            
            article = NewsArticle(
                title=title,
                url=url,
                published_date=published_date,
                content=content,
                tickers=tickers
            )
            
            self.logger.info(f"Scraped article: {title[:50]}... - Found tickers: {tickers}")
            return article
            
        except Exception as e:
            self.logger.error(f"Error scraping article {url}: {e}")
            return None
    
    def save_article_by_ticker(self, article: NewsArticle):
        """Save article to files named by ticker symbols"""
        if not article.tickers:
            # If no tickers found, save to a general file
            general_file = self.output_dir / 'UNKNOWN.txt'
            self._append_to_file(general_file, article)
            self.logger.info(f"Saved article without tickers to UNKNOWN.txt")
            return
        
        for ticker in article.tickers:
            filename = f"{ticker}.txt"
            filepath = self.output_dir / filename
            self._append_to_file(filepath, article)
            self.logger.info(f"Saved article to {filename}")
    
    def _append_to_file(self, filepath: Path, article: NewsArticle):
        """Append article to a file"""
        separator = "=" * 80
        
        with open(filepath, 'a', encoding='utf-8') as f:
            f.write(f"\n{separator}\n")
            f.write(f"DATE: {article.published_date}\n")
            f.write(f"TITLE: {article.title}\n")
            f.write(f"URL: {article.url}\n")
            f.write(f"TICKERS: {', '.join(article.tickers)}\n")
            f.write(f"SCRAPED AT: {article.scraped_at}\n")
            f.write(f"{separator}\n\n")
            f.write(f"{article.content}\n")
            f.write(f"\n{separator}\n\n")
    
    def run_once(self):
        """Run one iteration of news monitoring"""
        self.logger.info("Starting news monitoring run...")
        
        all_articles = []
        
        # Get articles from all categories
        for category in self.config['news_categories']:
            articles = self.get_news_links(category)
            all_articles.extend(articles)
        
        self.logger.info(f"Total articles to process: {len(all_articles)}")
        
        # Process each article
        processed_count = 0
        for article_info in all_articles:
            if processed_count >= self.config['max_news_per_run']:
                break
            
            url = article_info['url']
            
            # Skip if already processed
            if url in self.processed_urls:
                continue
            
            # Scrape the article
            article = self.scrape_article(url)
            
            if article:
                # Save article by ticker
                self.save_article_by_ticker(article)
                
                # Mark as processed
                self.processed_urls.add(url)
                processed_count += 1
        
        # Save processed URLs cache
        self._save_processed_urls()
        
        self.logger.info(f"Completed run. Processed {processed_count} new articles.")
        return processed_count
    
    def run_continuous(self):
        """Run continuous monitoring"""
        self.logger.info(f"Starting continuous monitoring (checking every {self.config['check_interval_minutes']} minutes)")
        
        # Schedule the job
        schedule.every(self.config['check_interval_minutes']).minutes.do(self.run_once)
        
        # Run immediately on start
        self.run_once()
        
        # Keep running
        while True:
            schedule.run_pending()
            time.sleep(1)
    
    def search_company_news(self, company_name: str, max_articles: int = 10) -> List[NewsArticle]:
        """Search for news about a specific company"""
        self.logger.info(f"Searching for news about: {company_name}")
        
        search_url = f"{self.config['base_url']}/search?q={company_name.replace(' ', '+')}"
        articles = []
        
        try:
            response = self.session.get(search_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'lxml')
            links = soup.find_all('a', href=re.compile(r'/news-release/'))
            
            for link in links[:max_articles]:
                href = link.get('href', '')
                if href and href.startswith('/'):
                    full_url = f"{self.config['base_url']}{href}"
                    article = self.scrape_article(full_url)
                    if article:
                        articles.append(article)
            
        except Exception as e:
            self.logger.error(f"Error searching for {company_name}: {e}")
        
        return articles


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='GlobeNewswire Real-Time News Monitor')
    parser.add_argument('--once', action='store_true', help='Run once and exit')
    parser.add_argument('--interval', type=int, default=5, help='Check interval in minutes')
    parser.add_argument('--output-dir', type=str, default='./news_output', help='Output directory for news files')
    parser.add_argument('--search', type=str, help='Search for news about a specific company')
    parser.add_argument('--categories', nargs='+', help='News categories to monitor')
    
    args = parser.parse_args()
    
    # Update config with command line args
    config = CONFIG.copy()
    config['check_interval_minutes'] = args.interval
    config['output_dir'] = args.output_dir
    
    if args.categories:
        config['news_categories'] = args.categories
    
    # Create monitor
    monitor = GlobeNewswireMonitor(config)
    
    if args.search:
        # Search mode
        articles = monitor.search_company_news(args.search)
        print(f"\nFound {len(articles)} articles:")
        for article in articles:
            print(f"  - {article.title} ({', '.join(article.tickers)})")
    elif args.once:
        # Single run mode
        count = monitor.run_once()
        print(f"\nProcessed {count} articles. Check {args.output_dir} for output files.")
    else:
        # Continuous monitoring mode
        print(f"Starting continuous monitoring...")
        print(f"Output directory: {args.output_dir}")
        print(f"Check interval: {args.interval} minutes")
        print("Press Ctrl+C to stop\n")
        monitor.run_continuous()


if __name__ == '__main__':
    main()
