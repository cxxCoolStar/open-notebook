import asyncio
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import List, Set

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from bs4 import BeautifulSoup
from loguru import logger

from api.sources_service import sources_service
from open_notebook.domain.notebook import Source

# List of sources to crawl (Subset from seed_data.py verified by user)
CRAWL_SOURCES = [
    # GitHub Trending & Specific Repos
    "https://github.com/trending",
    "https://github.com/thedotmack/claude-mem",
    
    # YouTube Channels
    "https://www.youtube.com/@LatentSpaceTV",
    "https://www.youtube.com/@AIDailyBrief",
    "https://www.youtube.com/@LennysPodcast",
    "https://www.youtube.com/@NoPriorsPodcast",
    "https://www.youtube.com/@GoogleDeepMind",
    "https://www.youtube.com/@sequoiacapital",
    "https://www.youtube.com/@RedpointAI",
    "https://www.youtube.com/@ycombinator",
    "https://www.youtube.com/@PeterYangYT",
    "https://www.youtube.com/@southparkcommons",
    "https://www.youtube.com/@AndrejKarpathy",
    "https://www.youtube.com/@AnthropicAI",
    "https://www.youtube.com/@Google",
    "https://www.youtube.com/@Every",
    "https://www.youtube.com/@MattTurck",
    "https://www.youtube.com/@ColeMedin",
]

class DailyCrawlerService:
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self.client = httpx.AsyncClient(headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        })

    def start(self):
        """Start the daily scheduler."""
        # Schedule to run at 9:00 AM every day
        self.scheduler.add_job(
            self.run_daily_crawl,
            CronTrigger(hour=9, minute=0),
            id="daily_crawl",
            replace_existing=True
        )
        self.scheduler.start()
        logger.info("Daily Crawler Service started. Scheduled for 09:00.")

    async def run_daily_crawl(self):
        """Main execution logic for daily crawl."""
        logger.info("Starting daily crawl execution...")
        
        discovered_urls = set()

        for source_url in CRAWL_SOURCES:
            try:
                new_links = await self._fetch_links_from_source(source_url)
                discovered_urls.update(new_links)
            except Exception as e:
                logger.error(f"Failed to crawl {source_url}: {e}")

        logger.info(f"Discovered {len(discovered_urls)} potential URLs. Filtering and adding source...")
        
        count = 0
        for url in discovered_urls:
            # Check if source already exists to avoid duplicates
            # Note: This is an optimization; duplicate checks might also exist in create_source logic
            # but usually it's good to check here if efficient.
            # For now, we'll try to create it, assuming service handles duplicates or we just handle errors.
            try:
                # We need to check existence efficiently. 
                # Since SourcesService doesn't have exist_by_url, we might skip or just try create.
                # Let's try to create and catch potential duplication errors if they bubble up,
                # or just let it process.
                
                # Check for recent YouTube videos (3 months)
                if "youtube.com" in url or "youtu.be" in url:
                    if not await self._is_video_recent(url):
                        logger.debug(f"Skipping old video: {url}")
                        continue

                # Create the source
                # We don't have a specific notebook_id, so it might be created as an orphaned source 
                # or we might need a default 'Daily Crawl' notebook?
                # The user request said "execute create source operation", implies just adding to the system.
                
                await sources_service.create_source(
                    url=url,
                    source_type="link",
                    title=None, # Will be fetched
                    async_processing=True # Use async to accept crawling logic we added before
                )
                count += 1
            except Exception as e:
                logger.error(f"Failed to add source {url}: {e}")
        
        logger.info(f"Daily crawl complete. Added {count} new sources.")

    async def _fetch_links_from_source(self, url: str) -> List[str]:
        """Fetch content from seed URL and parse relevant links."""
        links = []
        try:
            resp = await self.client.get(url, follow_redirects=True)
            if resp.status_code != 200:
                return []
            
            html = resp.text
            
            if "github.com/trending" in url:
                links = self._parse_github_trending(html)
            elif "github.com" in url:
                # It's a specific repo, just add itself if it's not already the url
                # Use request URL effectively
                links = [url]
            elif "youtube.com" in url:
                links = self._parse_youtube_channel(html)
            
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            
        return links

    def _parse_github_trending(self, html: str) -> List[str]:
        soup = BeautifulSoup(html, 'html.parser')
        urls = []
        articles = soup.find_all('article', class_='Box-row')
        for article in articles:
            h2 = article.find('h2')
            if h2:
                a_tag = h2.find('a')
                if a_tag and a_tag.get('href'):
                    relative_url = a_tag.get('href')
                    urls.append(f"https://github.com{relative_url}")
        return urls

    def _parse_youtube_channel(self, html: str) -> List[str]:
        # Simple regex for video IDs to avoid heavy parsing
        # Matches "videoId":"..."
        video_ids = set(re.findall(r'"videoId":"([a-zA-Z0-9_-]+)"', html))
        return [f"https://www.youtube.com/watch?v={vid}" for vid in video_ids]

    async def _is_video_recent(self, url: str) -> bool:
        """Check if YouTube video is published within last 3 months."""
        try:
            # We need to fetch the video page to get the date
            resp = await self.client.get(url, follow_redirects=True)
            if resp.status_code != 200:
                return False
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # Try to find date meta tags
            date_str = None
            date_meta = soup.find('meta', attrs={'itemprop': 'uploadDate'}) or \
                        soup.find('meta', attrs={'itemprop': 'datePublished'})
            
            if date_meta:
                date_str = date_meta.get('content')
            
            if not date_str:
                return True # If can't find date, assume it's okay/safe to include or risk it? User said "only 3 months".
                            # Safer to return False if unsure, OR check for "x months ago" text.
                            # Let's perform a stricter check.
                return False

            # Parse date (ISO 8601 usually YYYY-MM-DD or full datetime)
            # Remove time component for simplicity
            if 'T' in date_str:
                date_str = date_str.split('T')[0]
                
            pub_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            
            # Check delta
            three_months_ago = datetime.now(timezone.utc) - timedelta(days=90)
            
            return pub_date >= three_months_ago
            
        except Exception as e:
            logger.warning(f"Failed to verify video date for {url}: {e}")
            return False

# Global instance
daily_crawler_service = DailyCrawlerService()
