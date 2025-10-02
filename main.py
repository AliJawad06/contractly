import asyncio
import json
import logging
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
from typing import List, Dict, Any, Optional
import concurrent.futures
from pathlib import Path
import re
from dateutil import parser
from bs4 import BeautifulSoup
import time


# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SimplifiedJobScraper:
    def __init__(self, base_url: str, headless: bool = True, slow_mo: int = 0, date_cutoff_days: int = 30):
        self.base_url = base_url
        self.headless = headless
        self.slow_mo = slow_mo
        self.job_ids = []
        self.job_details = []
        
        # Date cutoff setup
        self.date_cutoff_days = date_cutoff_days
        self.cutoff_date = datetime.now() - timedelta(days=date_cutoff_days)
        self.cutoff_timestamp = int(self.cutoff_date.timestamp() * 1000)  # Convert to milliseconds
        
        logger.info(f"Date cutoff set to: {self.cutoff_date.strftime('%Y-%m-%d')} ({date_cutoff_days} days ago)")
        logger.info(f"Cutoff timestamp: {self.cutoff_timestamp}")
        
    def is_job_too_old(self, post_date) -> bool:
        """Check if a job's post date is before the cutoff date"""
        if not post_date:
            return False  # If no date, don't filter out
            
        try:
            # Handle timestamp in milliseconds
            if isinstance(post_date, (int, float)) and post_date > 0:
                return post_date < self.cutoff_timestamp
            
            # Handle string dates if needed
            if isinstance(post_date, str):
                # Add parsing logic for string dates if your API returns them
                pass
                
        except Exception as e:
            logger.error(f"Error checking date for job: {e}")
            return False  # Don't filter if we can't parse the date
            
        return False

    async def setup_network_interception_for_ids(self, page):
        """Set up network interception to capture job IDs from listing pages"""
        
        async def handle_response(response):
            is_job_request = (
                'searchjobsportal' in response.url.lower() or 
                'searchjobs' in response.url.lower() or
                'getmore' in response.url.lower()
            )
            
            if is_job_request and response.status == 200:
                try:
                    content_type = response.headers.get('content-type', '')
                    if 'application/json' in content_type:
                        json_data = await response.json()
                        should_continue = self.extract_job_ids_from_response(json_data)
                        # Store whether we should continue pagination
                        self._should_continue_pagination = should_continue
                except Exception as e:
                    logger.error(f"Error processing listing response: {e}")
        
        page.on('response', handle_response)
        self._should_continue_pagination = True  # Initialize

    def extract_job_ids_from_response(self, data) -> bool:
        """Extract job IDs from the listing response and check date cutoff"""
        should_continue = True
        jobs_on_page = 0
        old_jobs_count = 0
        
        try:
            # Handle your specific API format
            if isinstance(data, dict) and 'data' in data:
                jobs = data['data']
                if isinstance(jobs, list):
                    jobs_on_page = len(jobs)
                    
                    for job in jobs:
                        if isinstance(job, dict) and 'id' in job:
                            job_id = job['id']
                            post_date = job.get('postDate')
                            
                            # Check if job is too old
                            if self.is_job_too_old(post_date):
                                old_jobs_count += 1
                                post_date_readable = datetime.fromtimestamp(post_date / 1000).strftime('%Y-%m-%d') if post_date else 'Unknown'
                                logger.info(f"Skipping old job ID {job_id} (posted: {post_date_readable})")
                                continue
                            
                            # Add job ID if it's recent enough
                            if job_id not in self.job_ids:
                                self.job_ids.append(job_id)
                                post_date_readable = datetime.fromtimestamp(post_date / 1000).strftime('%Y-%m-%d') if post_date else 'Unknown'
                                logger.info(f"Found recent job ID: {job_id} (posted: {post_date_readable})")
                    
                    # Decision logic: stop if majority of jobs on this page are too old
                    old_job_ratio = old_jobs_count / jobs_on_page if jobs_on_page > 0 else 0
                    
                    if old_job_ratio >= 1.0:  # If 70% or more jobs are too old
                        logger.warning(f"Stopping pagination: {old_jobs_count}/{jobs_on_page} jobs on this page are older than cutoff date")
                        should_continue = False
                    elif old_jobs_count > 0:
                        logger.info(f"Found {old_jobs_count}/{jobs_on_page} old jobs on this page, continuing to next page")
                    
        except Exception as e:
            logger.error(f"Error extracting job IDs: {e}")
            
        return should_continue

    async def get_job_ids_from_listing(self, company: str):
        """Get all job IDs from the job listing pages with date cutoff"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless, slow_mo=self.slow_mo)
            context = await browser.new_context()
            page = await context.new_page()
            
            try:
                await self.setup_network_interception_for_ids(page)
                
                logger.info(f"Navigating to {self.base_url}")
                await page.goto(self.base_url)
                await page.wait_for_load_state('networkidle', timeout=15000)
                
                current_page = 1
                has_next_page = True
                
                while has_next_page:
                    logger.info(f"Processing listing page {current_page}...")
                    await page.wait_for_timeout(3000)
                    
                    # Check if we should stop due to date cutoff
                    if not self._should_continue_pagination:
                        logger.info(f"Stopping pagination for {company} due to date cutoff")
                        break
                    
                    # Check for next page
                    next_button = await page.query_selector('button[aria-label="Next Page"]:not([disabled])')
                    
                    if next_button:
                        try:
                            logger.info("Clicking next page button...")
                            await next_button.click()
                            await page.wait_for_load_state('networkidle', timeout=15000)
                            current_page += 1
                            await page.wait_for_timeout(2000)
                        except Exception as e:
                            logger.error(f"Error navigating to next page: {e}")
                            has_next_page = False
                    else:
                        logger.info("No more pages found")
                        has_next_page = False
                    
                    if current_page > 50:  # Safety limit
                        logger.warning("Reached maximum page limit")
                        break
                        
            except Exception as e:
                logger.error(f"Error getting job IDs: {e}")
            finally:
                await browser.close()
        
        logger.info(f"Found {len(self.job_ids)} recent job IDs for {company} (cutoff: {self.cutoff_date.strftime('%Y-%m-%d')})")
        return self.job_ids

    async def setup_network_interception_for_job_detail(self, page, job_id):
        """Set up network interception for individual job detail requests"""
        job_data = None
        
        async def handle_response(response):
            nonlocal job_data
            # The request name should match the job ID
            if str(job_id) in response.url and response.status == 200:
                try:
                    content_type = response.headers.get('content-type', '')
                    if 'application/json' in content_type:
                        json_data = await response.json()
                        job_data = self.extract_job_details(json_data, page.url)
                        logger.info(f"Captured job details for ID: {job_id}")
                except Exception as e:
                    logger.error(f"Error processing job detail response for {job_id}: {e}")
        
        page.on('response', handle_response)
        return lambda: job_data

    def extract_job_details(self, data, url):
        """Extract only the required fields from job detail response"""
        try:
            # Handle the nested structure from your example
            if isinstance(data, dict):
                job_info = data.get('job', {})

                jobJSON = {
                    'id': job_info.get('id','N/A'),
                    'title': job_info.get('title','N/A'),
                    'location': (f"{job_info['mainLocation']['state']}, {job_info['mainLocation']['city']}" if job_info.get('mainLocation') else "N/A"),
                    'experience': {
                        'experience': job_info.get('experience', 'N/A')
                    },
                    'posted': {'date_postedStr':job_info.get('postDateStr','N/A'), 'date_posted': job_info.get('postDate', 'N/A') },
                    'start': {'start_dateStr': job_info.get('startDateStr','N/A'), 'start_date': job_info.get('startDate','N/A')},
                    'end': {'end_dateStr': job_info.get('endDateStr','N/A'), 'end_date': job_info.get('endDate','N/A'),},
                    'salary': job_info.get('payRate','N/A'),
                    'remote': job_info.get('workingRemote','N/A'),
                    'recruiter': [job_info.get('primaryRecruiterName'), job_info.get('primaryRecruiterEmail'), job_info.get('primaryRecruiterPhone','N/A')],
                    'url':url,
                    'time_scraped': int(time.time() * 1000)
                }
                # Add readable date field
    
                return jobJSON
                
        except Exception as e:
            logger.error(f"Error extracting job details: {e}")
        return None

    async def scrape_individual_job(self, job_id):
        """Scrape details for a single job"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless, slow_mo=self.slow_mo)
            context = await browser.new_context()
            page = await context.new_page()
            
            try:
                # Set up network interception
                get_job_data = await self.setup_network_interception_for_job_detail(page, job_id)
                
                # Navigate to individual job URL
                job_url = f"{self.base_url}&SearchString=#/jobs/{job_id}"
                logger.info(f"Navigating to job: {job_url}")
                
                await page.goto(job_url)
                await page.wait_for_load_state('networkidle', timeout=15000)
                await page.wait_for_timeout(2000)  # Give time for API calls
                
                # Get the captured job data
                job_data = get_job_data()
                if job_data:
                    logger.info(f"Successfully scraped job {job_id}: {job_data.get('title', 'Unknown Title')}")
                    return job_data
                else:
                    logger.warning(f"No job data captured for job {job_id}")
                    return None
                    
            except Exception as e:
                logger.error(f"Error scraping job {job_id}: {e}")
                return None
            finally:
                await browser.close()

    async def scrape_jobs_batch(self, job_ids_batch):
        """Scrape a batch of jobs concurrently"""
        tasks = [self.scrape_individual_job(job_id) for job_id in job_ids_batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        successful_jobs = []
        for result in results:
            if isinstance(result, dict) and result is not None:
                successful_jobs.append(result)
            elif isinstance(result, Exception):
                logger.error(f"Exception in batch: {result}")
        
        return successful_jobs

    async def scrape_all_job_details(self, company: str, max_concurrent: int = 9):
        """Scrape details for all jobs with controlled concurrency"""
        if not self.job_ids:
            logger.warning("No job IDs to process")
            return []

        # Split job IDs into batches for controlled concurrency
        batch_size = max_concurrent
        batches = [self.job_ids[i:i + batch_size] for i in range(0, len(self.job_ids), batch_size)]
        
        all_job_details = []
        
        for i, batch in enumerate(batches, 1):
            logger.info(f"Processing batch {i}/{len(batches)} ({len(batch)} jobs)")
            batch_results = await self.scrape_jobs_batch(batch)
            all_job_details.extend(batch_results)
            
            # Add delay between batches to be respectful
            if i < len(batches):
                await asyncio.sleep(2)
        
        self.job_details = all_job_details
        logger.info(f"Successfully scraped {len(all_job_details)} job details out of {len(self.job_ids)} total jobs")
        return all_job_details

    async def save_to_json(self, company: str):
        """Save job details to JSON file"""
        if not self.job_details:
            logger.warning(f"No job details to save for {company}")
            return

        # Create directory if it doesn't exist
        Path(f'json/jobdiva/{company}').mkdir(parents=True, exist_ok=True)
        filename = f'json/jobdiva/{company}/{company}.json'
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.job_details, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Saved {len(self.job_details)} job details to {filename}")
        except Exception as e:
            logger.error(f"Error saving JSON for {company}: {e}")

def run_company_scraper(company_data, date_cutoff_days=15):
    """Run scraper for a single company - to be used in thread pool"""
    url, company = company_data
    
    async def scrape_company():
        scraper = SimplifiedJobScraper(
            base_url=url,
            headless=True,  # Set to True for production
            slow_mo=500,
            date_cutoff_days=date_cutoff_days  # Pass the date cutoff
        )
        
        try:
            # Step 1: Get all job IDs from listing pages (with date filtering)
            logger.info(f"Getting recent job IDs for {company} (last {date_cutoff_days} days)...")
            await scraper.get_job_ids_from_listing(company)
            
            if not scraper.job_ids:
                logger.warning(f"No recent job IDs found for {company}")
                return []
            
            # Step 2: Get detailed information for each job
            logger.info(f"Scraping details for {len(scraper.job_ids)} recent jobs from {company}...")
            job_details = await scraper.scrape_all_job_details(company, max_concurrent=5)
            
            # Add company info to each job
            for job in job_details:
                job['source_company'] = company
                job['source_url'] = url
                job['date_cutoff_days'] = date_cutoff_days
            
            # Step 3: Save to JSON
            await scraper.save_to_json(company)
            
            return job_details
            
        except Exception as e:
            logger.error(f"Error scraping {company}: {e}")
            return []
    
    # Run the async function
    return asyncio.run(scrape_company())

async def main_multithreaded(date_cutoff_days=30):
    """Main function with multithreading for companies"""
    logger.info(f"Starting job scraping with {date_cutoff_days} day cutoff")
    
    # Load companies from JSON
    companies_data = []
    try:
        with open('companies.json', 'r') as f:
            companies = json.load(f)
            for company in companies:
                if isinstance(company, dict) and 'url' in company and 'company' in company:
                    companies_data.append((company['url'], company['company']))
                elif isinstance(company, list) and len(company) >= 2:
                    companies_data.append((company[0], company[1]))
    except FileNotFoundError:
        logger.error("companies.json file not found")
        return
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing companies.json: {e}")
        return
    
    if not companies_data:
        logger.error("No company data found")
        return
    
    logger.info(f"Starting scraping for {len(companies_data)} companies")
    
    # Use ThreadPoolExecutor to run multiple companies in parallel
    all_job_details = []
    max_workers = min(3, len(companies_data))  # Limit concurrent companies
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all company scraping tasks
        future_to_company = {
            executor.submit(run_company_scraper, company_data, date_cutoff_days): company_data[0] 
            for company_data in companies_data
        }
        
        # Process completed tasks
        for future in concurrent.futures.as_completed(future_to_company):
            company = future_to_company[future]
            try:
                company_jobs = future.result()
                if company_jobs:
                    all_job_details.extend(company_jobs)
                    logger.info(f"Completed {company}: {len(company_jobs)} recent jobs")
                else:
                    logger.warning(f"No recent jobs retrieved for {company}")
            except Exception as e:
                logger.error(f"Company {company} generated an exception: {e}")
    
    # Save combined results
    if all_job_details:
        await save_combined_json(all_job_details, date_cutoff_days)
        logger.info(f"Total recent jobs scraped across all companies: {len(all_job_details)}")
    else:
        logger.warning("No recent job details were scraped from any company")

async def save_combined_json(all_job_details, date_cutoff_days):
    """Save all job details to a combined JSON file"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'all_companies_job_details_last_{date_cutoff_days}_days_{timestamp}.json'
    
    try:
        # Create a structured output with metadata
        output = {
            'metadata': {
                'total_jobs': len(all_job_details),
                'date_cutoff_days': date_cutoff_days,
                'scraped_at': datetime.now().isoformat(),
                'timestamp': timestamp
            },
            'jobs': all_job_details
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved {len(all_job_details)} total job details to {filename}")
    except Exception as e:
        logger.error(f"Error saving combined JSON: {e}")

if __name__ == "__main__":
    # You can modify the date cutoff here (default is 30 days)
    DATE_CUTOFF_DAYS = 15  # Only scrape jobs from the last 15 days
    asyncio.run(main_multithreaded(date_cutoff_days=DATE_CUTOFF_DAYS))