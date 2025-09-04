import asyncio
import json
import logging
import csv 
from playwright.async_api import async_playwright
from typing import List, Dict, Any
import concurrent.futures
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SimplifiedJobScraper:
    def __init__(self, base_url: str, headless: bool = True, slow_mo: int = 0):
        self.base_url = base_url
        self.headless = headless
        self.slow_mo = slow_mo
        self.job_ids = []
        self.job_details = []
        
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
                        self.extract_job_ids_from_response(json_data)
                except Exception as e:
                    logger.error(f"Error processing listing response: {e}")
        
        page.on('response', handle_response)

    def extract_job_ids_from_response(self, data):
        """Extract job IDs from the listing response"""
        try:
            # Handle your specific API format
            if isinstance(data, dict) and 'data' in data:
                jobs = data['data']
                if isinstance(jobs, list):
                    for job in jobs:
                        if isinstance(job, dict) and 'id' in job:
                            job_id = job['id']
                            if job_id not in self.job_ids:
                                self.job_ids.append(job_id)
                                logger.info(f"Found job ID: {job_id}")
        except Exception as e:
            logger.error(f"Error extracting job IDs: {e}")

    async def get_job_ids_from_listing(self, company: str):
        """Get all job IDs from the job listing pages"""
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
        
        logger.info(f"Found {len(self.job_ids)} job IDs for {company}")
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
                        job_data = self.extract_job_details(json_data)
                        logger.info(f"Captured job details for ID: {job_id}")
                except Exception as e:
                    logger.error(f"Error processing job detail response for {job_id}: {e}")
        
        page.on('response', handle_response)
        return lambda: job_data

    def extract_job_details(self, data):
        """Extract only the required fields from job detail response"""
        try:
            # Handle the nested structure from your example
            if isinstance(data, dict):
                job_info = data.get('job', {})
                
                return {
                    'id': job_info.get('id'),
                    'title': job_info.get('title'),
                    'ref_no': job_info.get('refNo'),
                    'job_description': job_info.get('jobDescription'),
                    'start_date': job_info.get('startDate'),
                    'end_date': job_info.get('endDate'),
                    'working_remote': job_info.get('workingRemote'),
                    'primary_recruiter_name': job_info.get('primaryRecruiterName'),
                    'primary_recruiter_email': job_info.get('primaryRecruiterEmail'),
                    'primary_recruiter_phone': job_info.get('primaryRecruiterPhone')
                }
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
                job_url = f"{self.base_url}jobs/{job_id}"
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

    async def save_to_csv(self, company: str):
        """Save job details to CSV file"""
        if not self.job_details:
            logger.warning(f"No job details to save for {company}")
            return

        filename = f'csv/{company}/{company}.csv'
        
        try:
            fieldnames = [
                'id', 'title', 'ref_no', 'job_description', 'start_date', 
                'end_date', 'working_remote', 'primary_recruiter_name', 
                'primary_recruiter_email', 'primary_recruiter_phone'
            ]
            
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(self.job_details)
            
            logger.info(f"Saved {len(self.job_details)} job details to {filename}")
        except Exception as e:
            logger.error(f"Error saving CSV for {company}: {e}")

def run_company_scraper(company_data):
    """Run scraper for a single company - to be used in thread pool"""
    company, url = company_data
    
    async def scrape_company():
        scraper = SimplifiedJobScraper(
            base_url=url,
            headless=True,  # Set to True for production
            slow_mo=500
        )
        
        try:
            # Step 1: Get all job IDs from listing pages
            logger.info(f"Getting job IDs for {company}...")
            await scraper.get_job_ids_from_listing(company)
            
            if not scraper.job_ids:
                logger.warning(f"No job IDs found for {company}")
                return []
            
            # Step 2: Get detailed information for each job
            logger.info(f"Scraping details for {len(scraper.job_ids)} jobs from {company}...")
            job_details = await scraper.scrape_all_job_details(company, max_concurrent=5)
            
            # Add company info to each job
            for job in job_details:
                job['source_company'] = company
                job['source_url'] = url
            
            # Step 3: Save to CSV
            await scraper.save_to_csv(company)
            
            return job_details
            
        except Exception as e:
            logger.error(f"Error scraping {company}: {e}")
            return []
    
    # Run the async function
    return asyncio.run(scrape_company())

async def main_multithreaded():
    """Main function with multithreading for companies"""
    # Load companies
    companies_data = []
    try:
        with open('companies.csv', 'r') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if len(row) >= 2:
                    companies_data.append((row[0], row[1]))
    except FileNotFoundError:
        logger.error("companies.csv file not found")
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
            executor.submit(run_company_scraper, company_data): company_data[0] 
            for company_data in companies_data
        }
        
        # Process completed tasks
        for future in concurrent.futures.as_completed(future_to_company):
            company = future_to_company[future]
            try:
                company_jobs = future.result()
                if company_jobs:
                    all_job_details.extend(company_jobs)
                    logger.info(f"Completed {company}: {len(company_jobs)} jobs")
                else:
                    logger.warning(f"No jobs retrieved for {company}")
            except Exception as e:
                logger.error(f"Company {company} generated an exception: {e}")
    
    # Save combined results
    if all_job_details:
        await save_combined_csv(all_job_details)
        logger.info(f"Total jobs scraped across all companies: {len(all_job_details)}")
    else:
        logger.warning("No job details were scraped from any company")

async def save_combined_csv(all_job_details):
    """Save all job details to a combined CSV"""
    filename = 'all_companies_job_details.csv'
    
    try:
        fieldnames = [
            'id', 'title', 'ref_no', 'job_description', 'start_date', 
            'end_date', 'working_remote', 'primary_recruiter_name', 
            'primary_recruiter_email', 'primary_recruiter_phone',
            'source_company', 'source_url'
        ]
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(all_job_details)
        
        logger.info(f"Saved {len(all_job_details)} total job details to {filename}")
    except Exception as e:
        logger.error(f"Error saving combined CSV: {e}")



if __name__ == "__main__":
    asyncio.run(main_multithreaded())