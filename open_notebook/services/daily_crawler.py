import asyncio
import logging
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Set

# Add the current directory to Python path so internal imports work
current_dir = Path(__file__).parent.parent.parent
if str(current_dir) not in sys.path:
    sys.path.insert(0, str(current_dir))

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from bs4 import BeautifulSoup
from loguru import logger

from api.sources_service import sources_service
from api.notebook_service import notebook_service
from open_notebook.domain.notebook import Source

# List of sources to crawl (Subset from seed_data.py verified by user)
CRAWL_SOURCES = [
    # GitHub Trending & Specific Repos
    "https://github.com/trending",
    
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
        # Schedule to run at 9:00 AM Beijing Time (UTC+8)
        # Note: Docker containers usually run in UTC. 
        # Using explicit timezone ensures it runs at the user's expected local time.
        beijing_tz = timezone(timedelta(hours=8))
        
        self.scheduler.add_job(
            self.run_daily_crawl,
            CronTrigger(hour=9, minute=0, timezone=beijing_tz),
            id="daily_crawl",
            replace_existing=True
        )
        self.scheduler.start()
        
        next_run = self.scheduler.get_job("daily_crawl").next_run_time
        logger.info(f"Daily Crawler Service started. Scheduled for 09:00 Beijing Time. Next run: {next_run}")

    async def run_daily_crawl(self):
        """Main execution logic for daily crawl."""
        logger.info("Starting daily crawl execution...")
        
        # Ensure we have a notebook for daily crawls
        today_str = datetime.now().strftime("%Y-%m-%d")
        notebook_name = f"Daily Crawl {today_str}"
        
        notebooks = notebook_service.get_all_notebooks()
        crawl_notebook = next((nb for nb in notebooks if nb.name == notebook_name), None)
        
        if not crawl_notebook:
            logger.info(f"Creating '{notebook_name}' notebook...")
            crawl_notebook = notebook_service.create_notebook(
                name=notebook_name,
                description=f"Notebook for automatically crawled sources on {today_str}."
            )
        
        notebook_id = crawl_notebook.id
        logger.info(f"Using notebook: {crawl_notebook.name} ({notebook_id})")

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
            try:
                # Check for recent YouTube videos (1 week)
                if "youtube.com" in url or "youtu.be" in url:
                    if not await self._is_video_recent(url):
                        logger.debug(f"Skipping old video: {url}")
                        continue

                # Create the source
                sources_service.create_source(
                    url=url,
                    source_type="link",
                    title=None, # Will be fetched
                    notebook_id=notebook_id,
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
        """Check if YouTube video is published within last week."""
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
                return False

            # Parse date (ISO 8601 usually YYYY-MM-DD or full datetime)
            if 'T' in date_str:
                date_str = date_str.split('T')[0]
                
            pub_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            
            # Check delta
            one_week_ago = datetime.now(timezone.utc) - timedelta(days=7)
            
            return pub_date >= one_week_ago
            
        except Exception as e:
            logger.warning(f"Failed to verify video date for {url}: {e}")
            return False

# Global instance
daily_crawler_service = DailyCrawlerService()

if __name__ == "__main__":
    async def main():
        service = DailyCrawlerService()
        await service.run_daily_crawl()
    
    asyncio.run(main())
