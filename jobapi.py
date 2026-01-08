import asyncio
import json
import time
import logging
import concurrent.futures
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from datetime import datetime
from dateutil import parser
from tests.findlastJob import findLast
# Set up logging (using Code A format)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CeipalJobScraper:
    def __init__(self, maxT, company_name, base_url, options=None):
        self.company_name = company_name
        self.maxT = maxT
        self.base_url = base_url
        self.options = {
            'headless': True,
            'timeout': 30000,
            'delay': 2,
            'max_retries': 3,
            'min_timestamp': 1761177600000  # Unix timestamp in milliseconds - stop scraping if job is older
        }
        if options:
            self.options.update(options)

        self.all_jobs = []
        self.browser = None
        self.page = None
        self.page_number = 1
        self.should_stop_scraping = False  # Flag to stop scraping early

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
        parsed = urlparse(base_url)
        query_params = parse_qs(parsed.query)
        if '?' in base:
            return f"{base}&job_id={job_id}"
        else:
            return f"{base}?job_id={job_id}"

    async def wait_for_network_response(self, url_pattern, timeout=30):
        """Wait for a specific network response"""
        response_data = None

        async def handle_response(response):
            nonlocal response_data
            try:
                if url_pattern in response.url:
                    response_data = await response.json()
            except Exception as e:
                logger.debug(f"Error parsing JSON from response: {e}")

        self.page.on('response', handle_response)
        start_time = time.time()
        while response_data is None and time.time() - start_time < timeout:
            await asyncio.sleep(0.1)

        try:
            self.page.remove_listener('response', handle_response)
        except Exception:
            pass

        return response_data

    async def get_next_page_number(self):
        """Get the next page number from pagination"""
        try:
            next_page_number = self.page_number + 1
            next_page_selector = f"div.colPagnation ul li a[onclick*='loadJobPostings({next_page_number})']"
            next_page_elem = await self.page.query_selector(next_page_selector)
            if next_page_elem:
                return next_page_number
            else:
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
                jobs_data_task = asyncio.create_task(
                    self.wait_for_network_response('CareerPortalJobPostings')
                )
                await li_elem.click()
                logger.info(f"Navigated to page: {self.page_number}")
                await self.page.wait_for_load_state('networkidle')
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
            jobs_data_task = asyncio.create_task(
                self.wait_for_network_response('CareerPortalJobPostings')
            )
            await self.page.goto(page_url, wait_until='networkidle')
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

    def check_if_job_too_old(self, job_timestamp):
        """Check if a job is older than the minimum timestamp"""
        if self.options['min_timestamp'] is None:
            return False
        
        if not isinstance(job_timestamp, (int, float)):
            return False
        
        return job_timestamp < self.maxT

    async def scrape_job_details(self, base_url, job_basic_info):
        job_id = job_basic_info.get('id')
        if not job_id:
            logger.warning("No job ID found")
            return None

        logger.info(f"Scraping job details for ID: {job_id}")
        try:
            job_selector = f'a[data-job-id="{job_id}"]' if "v1" in self.base_url else f'div[data-job-id="{job_id}"]'
            await self.page.wait_for_selector(job_selector, timeout=10000)
            job_details_task = asyncio.create_task(
                self.wait_for_network_response(str(job_id), timeout=15)
            )
            await self.page.click(job_selector)
            logger.info(f"Clicked on job div for ID: {job_id}")
            await self.page.wait_for_load_state('networkidle')
            job_details = await job_details_task
            if job_details:
                extracted_data = self.extract_job_fields(job_details)
                logger.info(f"Successfully scraped job: {extracted_data.get('title', 'Unknown')}")
                return extracted_data
            else:
                logger.warning(f"Could not get detailed info for job {job_id}, using basic info")
                return self.extract_job_fields(job_basic_info)
        except Exception as e:
            logger.error(f"Error scraping job details for {job_id}: {e}")
            return self.extract_job_fields(job_basic_info)

    def extract_job_fields(self, job_data):
        url = self.page.url
        pay_rate = ""
        date_postedStr = job_data.get('modified', 'N/A')
        closing_date = job_data.get('closing_date', 'N/A')
        start_date = job_data.get('job_start_date', 'N/A')
        end_date = job_data.get('job_end_date', 'N/A')
        if job_data.get('pay_rates') and len(job_data['pay_rates']) > 0:
            pay_rate = job_data['pay_rates'][0].get('min_pay_rate', 'N/A')

        return {
            'id': job_data.get('id', job_data.get('job_id', 'N/A')),
            'title': job_data.get('position_title', job_data.get('public_job_title', 'N/A')),
            'location': job_data.get('address', 'multpile_job_location') or "",
            'experience': {
                "experience": job_data.get('experience', 'N/A'),
                'min_experience': job_data.get('min_experience', 'N/A')
            },
            'skills': next((o["value"] for o in job_data.get('display_job_view_fields', []) if o.get("label") == "Primary Skills"), None),
            "posted": {
                'date_postedStr': date_postedStr,
                'date_posted': self.date_to_time(date_postedStr)
            },
            'description': job_data.get('public_job_desc', None),
            "closing": {
                'closing_date': closing_date,
                'closing_dateStr': self.date_to_time(date_postedStr)
            },
            "start": {
                'start_date': start_date,
                'start_dateStr': self.date_to_time(start_date)
            },
            "end": {
                'end_dateStr': end_date,
                'end_date': self.date_to_time(end_date)
            },
            'salary': pay_rate,
            'remote': job_data.get('remote_opportunities', 0),
            'assigned_recruiter': job_data.get('assigned_recruiter', 'N/A'),
            "recruiter": {
                'contact': BeautifulSoup(job_data.get('contact_person', 'N/A'), "html.parser").text or "N/A",
                'client': job_data.get('client', 'N/A')
            },
            'apply': job_data.get('easy_apply_job_login_hc', ''),
            'url': url
        }

    async def go_back_to_jobs_list(self):
        try:
            await asyncio.sleep(0.8)
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
                    await asyncio.sleep(0.8)
                    return True
            logger.warning("Back to all jobs link not found with any selector")
            return False
        except Exception as e:
            logger.error(f"Error going back to jobs list: {e}")
            return False

    async def scrape_url(self):
        logger.info(f"Starting scraping for {self.company_name}: {self.base_url}")
        self.page_number = 1
        jobs_data = await self.scrape_jobs_list(self.base_url)
        
        while jobs_data and jobs_data.get('results') and not self.should_stop_scraping:
            logger.info(f"Scraping page {self.page_number}...")
            try:
                jobs_list = jobs_data['results']
                logger.info(f"Processing {len(jobs_list)} jobs on page {self.page_number}")
                
                for i, job in enumerate(jobs_list, 1):
                    if self.should_stop_scraping:
                        logger.info(f"Stopping scraping for {self.company_name} - found old job")
                        break
                    
                    logger.info(f"Processing job {i}/{len(jobs_list)}")
                    detailed_job = await self.scrape_job_details(self.base_url, job)
                    
                    if detailed_job:
                        job_timestamp = detailed_job.get('posted', {}).get('date_posted')
                        
                        # Check if job is too old
                        if int(job_timestamp) < findLast("/Users/aj/Downloads/DhikrUp/contractJobs/day/jobsapi_day.json"):
                            logger.info(f"Job {detailed_job.get('id')} is older than cutoff timestamp. Stopping scraping for {self.company_name}.")
                            self.should_stop_scraping = True
                            break
                        
                        # Job is recent enough, add it
                        self.all_jobs.append(detailed_job)
                        logger.info(f"Added job {detailed_job.get('id')} - posted at {job_timestamp}")
                    
                    await self.go_back_to_jobs_list()
                    await asyncio.sleep(self.options['delay'])
                
                # If we should stop, don't go to next page
                if self.should_stop_scraping:
                    logger.info(f"Early stop triggered for {self.company_name}. Total jobs scraped: {len(self.all_jobs)}")
                    break
                
                # Try to go to next page
                next_page_number = await self.get_next_page_number()
                if next_page_number:
                    logger.info(f"Moving to next page: {next_page_number}")
                    jobs_data = await self.navigate_to_next_page(next_page_number)
                    await asyncio.sleep(self.options['delay'] * 2)
                else:
                    logger.info("No more pages to scrape")
                    break
                    
            except Exception as e:
                logger.error(f"Error scraping page {self.page_number}: {e}")
                break

    def save_to_json(self):
        if not self.all_jobs:
            logger.warning(f"No jobs to save for {self.company_name}")
            return
        filename = f"json/ceipal/{self.company_name}/{self.company_name}.json"
        Path(f"json/ceipal/{self.company_name}").mkdir(parents=True, exist_ok=True)
        try:
            for job in self.all_jobs:
                if 'description' in job and job['description']:
                    job['description'] = BeautifulSoup(job['description'], "html.parser").text
            output = {
                'company': self.company_name,
                'scrape_date': datetime.now().isoformat(),
                'total_jobs': len(self.all_jobs),
                'jobs': self.all_jobs
            }
            with open(filename, 'w', encoding='utf-8') as jsonfile:
                json.dump(output, jsonfile, indent=2, ensure_ascii=False)
            logger.info(f"Results saved to {filename}")
            logger.info(f"Total jobs saved: {len(self.all_jobs)}")
        except Exception as e:
            logger.error(f"Error saving JSON: {e}")

    async def run(self):
        logger.info(f"Starting Ceipal job scraper for {self.company_name}...")
        await self.initialize()
        try:
            await self.scrape_url()
            self.save_to_json()
            logger.info(f"Scraping completed for {self.company_name}. Total jobs scraped: {len(self.all_jobs)}")
        except Exception as e:
            logger.error(f"Error during scraping: {e}")
        finally:
            await self.close()
        return self.all_jobs


def run_company_scraper(company_data, options=None):
    if isinstance(company_data, (tuple, list)):
        url, company = company_data[0], company_data[1]
    elif isinstance(company_data, dict):
        url, company = company_data.get('url'), company_data.get('company')
    else:
        raise ValueError("company_data must be a tuple/list or dict")

    async def _scrape():

        max = 1762637911000
        scraper = CeipalJobScraper(maxT=max,company_name=company, base_url=url, options=options or {})
        try:
            jobs = await scraper.run()
            for j in jobs:
                j['company'] = company
            return jobs
        except Exception as e:
            logger.error(f"Error scraping {company}: {e}")
            return []

    return asyncio.run(_scrape())


async def save_combined_json(all_job_details, label="combined", date_cutoff_days=None):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    date_suffix = f"_last_{date_cutoff_days}_days" if date_cutoff_days is not None else ""
    filename = f"all_companies_job_details{date_suffix}_{timestamp}.json"
    Path("json/combined").mkdir(parents=True, exist_ok=True)
    filepath = Path("json/combined") / filename
    try:
        output = {
            'metadata': {
                'total_jobs': len(all_job_details),
                'date_cutoff_days': date_cutoff_days,
                'scraped_at': datetime.now().isoformat(),
                'timestamp': timestamp
            },
            'jobs': all_job_details
        }
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

        with open("/Users/aj/Downloads/DhikrUp/contractJobs/day/jobsapi_day.json", 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

        logger.info(f"Saved {len(all_job_details)} total job details to {filepath}")
    except Exception as e:
        logger.error(f"Error saving combined JSON: {e}")


async def main_multithreaded(companies_data, max_workers: int = None, options: dict = None,
                             date_cutoff_days: int = None, date_cutoff_timestamp: int = None):
    if not companies_data:
        logger.error("No company data provided to main_multithreaded")
        return

    logger.info(f"Starting multithreaded scraping for {len(companies_data)} companies")
    all_job_details = []
    max_workers = max_workers or min(1, len(companies_data))
    max_workers = min(max_workers, len(companies_data))

    # Add min_timestamp to options if date_cutoff_timestamp is provided
    if options is None:
        options = {}
    if date_cutoff_timestamp:
        options['min_timestamp'] = date_cutoff_timestamp

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_company = {
            executor.submit(run_company_scraper, company_data, options): (
                company_data.get('company') if isinstance(company_data, dict) else company_data[1]
            )
            for company_data in companies_data
        }

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

    if all_job_details:
        await save_combined_json(all_job_details, date_cutoff_days=date_cutoff_days)
        logger.info(f"Total jobs scraped across all companies: {len(all_job_details)}")
    else:
        logger.warning("No job details were scraped from any company")


if __name__ == "__main__":
    companies = []
    with open("/Users/aj/Downloads/DhikrUp/contractJobs/jacompanies.json", "r") as r:
        companies = json.load(r)

    options = {
        'headless': False,
        'delay': 2,
        'timeout': 30000
    }

    # 🔹 Stop scraping each company once a job older than this timestamp is found
    # Example: Nov 1, 2024 00:00:00 UTC = 1730419200000
    DATE_CUTOFF_TIMESTAMP = 1730419200000

    MAX_WORKERS = None

    asyncio.run(main_multithreaded(
        companies,
        max_workers=MAX_WORKERS,
        options=options,
        date_cutoff_timestamp=DATE_CUTOFF_TIMESTAMP
    ))