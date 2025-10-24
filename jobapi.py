import asyncio
import csv
import time
from urllib.parse import urlparse, parse_qs
from playwright.async_api import async_playwright
import logging
from bs4 import BeautifulSoup
from datetime import datetime
from dateutil import parser


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CeipalJobScraper:
    def __init__(self, company_name, base_url, options=None):
        self.company_name = company_name
        self.base_url = base_url
        self.options = {
            'headless': True,
            'timeout': 30000,
            'delay': 2,
            'max_retries': 3
        }
        if options:
            self.options.update(options)
        
        self.all_jobs = []
        self.browser = None
        self.page = None
        self.page_number = 1

    async def initialize(self):
        """Initialize the browser and page"""
        playwright = await async_playwright().start()
        self.browser = await playwright.chromium.launch(
            headless=self.options['headless'],
            args=['--no-sandbox', '--disable-setuid-sandbox']
        )
        self.page = await self.browser.new_page()
        self.page.set_default_timeout(self.options['timeout'])

    async def close(self):
        """Close the browser"""
        if self.browser:
            await self.browser.close()

    def extract_base_url(self, full_url):
        """Extract base URL up to index.html"""
        if 'index.html' in full_url:
            return full_url.split('index.html')[0] + 'index.html'
        return full_url

    def build_job_url(self, base_url, job_id):
        """Build individual job URL"""
        base = self.extract_base_url(base_url)
        # Parse existing query params to maintain api_key, etc.
        parsed = urlparse(base_url)
        query_params = parse_qs(parsed.query)
        
        # Build new URL with job_id
        if '?' in base:
            return f"{base}&job_id={job_id}"
        else:
            return f"{base}?job_id={job_id}"

    async def wait_for_network_response(self, url_pattern, timeout=30):
        """Wait for a specific network response"""
        response_data = None
        
        async def handle_response(response):
            nonlocal response_data
            if url_pattern in response.url:
                try:
                    response_data = await response.json()
                except Exception as e:
                    logger.error(f"Error parsing JSON from response: {e}")
        
        # Set up response listener
        self.page.on('response', handle_response)
        
        # Wait for response or timeout
        start_time = time.time()
        while response_data is None and time.time() - start_time < timeout:
            await asyncio.sleep(0.1)
        
        # Remove listener
        self.page.remove_listener('response', handle_response)
        
        return response_data

    async def get_next_page_number(self):
        """Get the next page number from pagination"""
        try:
            # Look for the next page in pagination
            next_page_number = self.page_number + 1
            
            # Check if next page exists in pagination
            next_page_selector = f"div.colPagnation ul li a[onclick*='loadJobPostings({next_page_number})']"
            next_page_elem = await self.page.query_selector(next_page_selector)
            
            if next_page_elem:
                return next_page_number
            else:
                # Try alternative pagination selectors
                next_btn_selectors = [
                    "a:has-text('Next')",
                    ".pagination .next",
                    "[aria-label='Next page']",
                    ".page-next"
                ]
                
                for selector in next_btn_selectors:
                    next_btn = await self.page.query_selector(selector)
                    if next_btn:
                        return next_page_number
                
                return None
                
        except Exception as e:
            logger.error(f"Error getting next page number: {e}")
            return None
    
    async def navigate_to_next_page(self, next_page_number):
        """Navigate to the next page and return jobs data"""
        try:
            self.page_number = next_page_number
            li_selector = f"div.colPagnation ul li:has-text('{self.page_number}')"
            li_elem = await self.page.query_selector(li_selector)
            li_elem = li_elem if li_elem else await self.page.query_selector("div.colPagnation a:has-text('Next >')")
            if li_elem:
                # Set up listener for new jobs data before clicking
                jobs_data_task = asyncio.create_task(
                    self.wait_for_network_response('CareerPortalJobPostings')
                )
                
                await li_elem.click()
                logger.info(f"Navigated to page: {self.page_number}")
                
                # Wait for page to load
                await self.page.wait_for_load_state('networkidle')
                
                # Get the new jobs data
                jobs_data = await jobs_data_task
                return jobs_data
            else:
                logger.warning(f"Could not find pagination element for page {self.page_number}")
                return None
                
        except Exception as e:
            logger.error(f"Error navigating to page {self.page_number}: {e}")
            return None

    async def scrape_jobs_list(self, page_url):
        """Scrape the jobs list from CareerPortalJobPostings response"""
        logger.info(f"Navigating to: {page_url}")
        
        try:
            # Set up listener for CareerPortalJobPostings before navigation
            jobs_data_task = asyncio.create_task(
                self.wait_for_network_response('CareerPortalJobPostings')
            )
            
            # Navigate to the page
            await self.page.goto(page_url, wait_until='networkidle')
            
            # Wait for the jobs data
            jobs_data = await jobs_data_task
            
            if not jobs_data:
                logger.warning("No CareerPortalJobPostings response found")
                return None
            
            logger.info(f"Found {jobs_data.get('count', 0)} jobs")
            return jobs_data
            
        except Exception as e:
            logger.error(f"Error scraping jobs list: {e}")
            return None
        
    def date_to_time(self, date):
        try:
            dt = parser.parse(date)
            return int(dt.timestamp() * 1000)
        except Exception:
            return date



    async def scrape_job_details(self, base_url, job_basic_info):
        """Scrape detailed job information by clicking on job div"""
        job_id = job_basic_info.get('id')
        if not job_id:
            logger.warning("No job ID found")
            return None
        
        logger.info(f"Scraping job details for ID: {job_id}")
        
        try:
            # Find the job div by data-job-id attribute
            job_selector = f'div[data-job-id="{job_id}"]'
            
            # Wait for the job div to be present
            await self.page.wait_for_selector(job_selector, timeout=10000)
            
            # Set up listener for job details response before clicking
            job_details_task = asyncio.create_task(
                self.wait_for_network_response(str(job_id), timeout=15)
            )
            
            # Click on the job div
            await self.page.click(job_selector)
            logger.info(f"Clicked on job div for ID: {job_id}")
            
            # Wait for navigation or job details to load
            await self.page.wait_for_load_state('networkidle')
            
            # Wait for job details
            job_details = await job_details_task
            
            if job_details:
                # Extract required fields
                extracted_data = self.extract_job_fields(job_details)
                logger.info(f"Successfully scraped job: {extracted_data.get('title', 'Unknown')}")
                return extracted_data
            else:
                # Fallback to basic info if detailed scraping fails
                logger.warning(f"Could not get detailed info for job {job_id}, using basic info")
                return self.extract_job_fields(job_basic_info)
                
        except Exception as e:
            logger.error(f"Error scraping job details for {job_id}: {e}")
            # Fallback to basic info
            return self.extract_job_fields(job_basic_info)

                          #c means exclusively ceipal field 
                
                #id x
                #title
                #experience: experience, min_experiencec
                #date_postedStr
                #salary
                #date_posted 
                #job description 
                #skills
                #dates: posted: date_postedStr, date_posted start: start_dateStr, start_date end: end_dateStr, end_date closingc: closing_dateStr, closing_date
                #remote
                #recruiter/contact: name, email, phone, assigned_recruiterc, contactc
                #clientc 
                #urls: url, easy_applyc 
                #client 
                # 



    def extract_job_fields(self, job_data):
        """Extract required fields from job data"""

        
        url = self.page.url
        pay_rate = ""
        date_postedStr = job_data.get('date_postedStr', 'N/A')
        closing_date = job_data.get('closing_date', 'N/A')
        start_date = job_data.get('job_start_date', 'N/A')
        end_date = job_data.get('job_end_date','N/A')

        if job_data.get('pay_rates') and len(job_data['pay_rates']) > 0:
            pay_rate = job_data['pay_rates'][0].get('pay_rate', 'N/A')
        return {      
            'id': job_data.get('id', job_data.get('job_id', 'N/A')),
            'title': job_data.get('position_title', job_data.get('public_job_title', 'N/A')),
            'location': job_data.get('multpile_job_location', 'N/A'),
            'experience': job_data.get('experience', 'N/A'),
            'min_experience': job_data.get('min_experience', 'N/A'),
            'skills': next((o["value"] for o in job_data.get('display_job_view_fields', []) if o.get("label") == "Primary Skills"), None),
            'date_postedStr': date_postedStr,
            'date_posted': self.date_to_time(date_postedStr),
            'job_description': job_data.get('public_job_desc', None),
            'closing_date': closing_date,
            'closing_dateStr': self.date_to_time(date_postedStr),
            'start_date': start_date,
            'start_dateStr': self.date_to_time(start_date),
            'end_dateStr': end_date,
            'end_date': self.date_to_time(end_date),
            'salary': pay_rate,
            'remote': job_data.get('remote_opportunities', 0),
            'assigned_recruiter': job_data.get('assigned_recruiter', 'N/A'),
            'contact': BeautifulSoup(job_data.get('contact_person', 'N/A'),"html.parser").text or "N/A",
            'easy_apply_job_login_hc': job_data.get('easy_apply_job_login_hc', ''),
            'client': job_data.get('client', 'N/A'),
            'url': url
        }

    async def go_back_to_jobs_list(self):
        """Click the 'Back to all jobs' link"""
        try:
            # Try multiple selectors for the back link
            back_selectors = [
                'text="<< Back to all jobs"',
                'text="Back to all jobs"',
                'a:has-text("Back")',
                '.back-link',
                '[title*="back"]'
            ]
            
            for selector in back_selectors:
                back_link = await self.page.query_selector(selector)
                if back_link:
                    await back_link.click()
                    await self.page.wait_for_load_state('networkidle')
                    await asyncio.sleep(self.options['delay'])
                    return True
                    
            logger.warning("Back to all jobs link not found with any selector")
            return False
            
        except Exception as e:
            logger.error(f"Error going back to jobs list: {e}")
            return False

    async def scrape_url(self):
        """Scrape all jobs from the base URL with HTML pagination"""
        logger.info(f"Starting scraping for {self.company_name}: {self.base_url}")
        self.page_number = 1

        # Start with the initial page
        jobs_data = await self.scrape_jobs_list(self.base_url)
        
        while jobs_data and jobs_data.get('results'):
            logger.info(f"Scraping page {self.page_number}...")
            
            try:
                jobs_list = jobs_data['results']
                logger.info(f"Processing {len(jobs_list)} jobs on page {self.page_number}")
                
                # Process each job
                for i, job in enumerate(jobs_list, 1):
                    logger.info(f"Processing job {i}/{len(jobs_list)}")
                    
                    # Get detailed job info by clicking on job div
                    detailed_job = await self.scrape_job_details(self.base_url, job)
                    if detailed_job:
                        self.all_jobs.append(detailed_job)
                    
                    # Go back to jobs list if not the last job
                    await self.go_back_to_jobs_list()
                    
                    # Add delay between jobs
                    await asyncio.sleep(self.options['delay'])
                
                # Check for next page using HTML pagination
                next_page_number = await self.get_next_page_number()
                if next_page_number:
                    logger.info(f"Moving to next page: {next_page_number}")
                    jobs_data = await self.navigate_to_next_page(next_page_number)
                    
                    # Add delay between pages
                    await asyncio.sleep(self.options['delay'] * 2)
                else:
                    logger.info("No more pages to scrape")
                    break
                    
            except Exception as e:
                logger.error(f"Error scraping page {self.page_number}: {e}")
                break

    def save_to_csv(self):
        """Save results to CSV file with company name"""
        if not self.all_jobs:
            logger.warning(f"No jobs to save for {self.company_name}")
            return
        
        filename = f"csv/ceipal/{self.company_name}/{self.company_name}.csv"
        
        try:
            # Define CSV fieldnames based on the extracted fields
            fieldnames = [
                'title', 'date_posted', 'requisition_description', 'job_description',
                'closing_date', 'start_date', 'pay_rate', 'assigned_recruiter',
                'contact', 'easy_apply_job_login_hc', 'remote', 'modified_date',
                'job_id', 'client', 'location', 'job_code', 'skills'
            ]
            
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                # Write header
                writer.writeheader()
                
                # Write job data
                for job in self.all_jobs:
                    # Clean up job description and requisition description for CSV
                    if 'job_description' in job:
                        job['job_description'] = BeautifulSoup(job['job_description'], "html.parser").text.replace('\n', ' ').replace('\r', ' ')
                    if 'requisition_description' in job:
                        job['requisition_description'] = str(job['requisition_description']).replace('\n', ' ').replace('\r', ' ')
                    
                    writer.writerow(job)
            
            logger.info(f"Results saved to {filename}")
            logger.info(f"Total jobs saved: {len(self.all_jobs)}")
            
        except Exception as e:
            logger.error(f"Error saving CSV: {e}")

    async def run(self):
        """Main method to run the scraper"""
        logger.info(f"Starting Ceipal job scraper for {self.company_name}...")
        await self.initialize()
        
        try:
            await self.scrape_url()
            self.save_to_csv()
            logger.info(f"Scraping completed for {self.company_name}. Total jobs scraped: {len(self.all_jobs)}")
        except Exception as e:
            logger.error(f"Error during scraping: {e}")
        finally:
            await self.close()
        
        return self.all_jobs


async def main():
    # List of companies and their URLs
    companies = [
        {
            "company": "StaffingSols",
            "url": "https://jobsapi.ceipal.com/APISource/v2/index.html?bgcolor=1ba1ff&api_key=M1dVMTh0Tmk4NWFYdnE3eE5zd0RhZz09&cp_id=Z3RkUkt2OXZJVld2MjFpOVRSTXoxZz09&job_id="
        },
        # Add more companies here as needed
        # {
        #     "company": "AnotherCompany",
        #     "url": "https://jobsapi.ceipal.com/..."
        # }
    ]
    
    # Configuration options
    options = {
        'headless': False,  # Set to True for production
        'delay': 2,  # Delay between requests in seconds
        'timeout': 30000
    }
    
    # Scrape jobs for each company
    for company_info in companies:
        scraper = CeipalJobScraper(
            company_name=company_info["company"],
            base_url=company_info["url"],
            options=options
        )
        jobs = await scraper.run()
        
        print(f"Scraped {len(jobs)} jobs for {company_info['company']}!")
        
        # Add delay between companies
        if len(companies) > 1:
            await asyncio.sleep(5)

if __name__ == "__main__":
    # Install required packages first:
    # pip install playwright beautifulsoup4
    # playwright install chromium
    
    asyncio.run(main())