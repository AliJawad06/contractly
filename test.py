import asyncio
import json
import logging
import csv 
from playwright.async_api import async_playwright
from typing import List, Dict, Any


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NetworkJobScraper:
    def __init__(self, base_url: str, headless: bool = True, slow_mo: int = 0):
        self.base_url = base_url
        self.headless = headless
        self.slow_mo = slow_mo
        self.all_job_data = []
        self.network_responses = []
        
    async def setup_network_interception(self, page):
        """Set up network request/response interception"""
        
        async def handle_response(response):
            # Check if this is the searchjobsportal or getmore request
            is_job_request = (
                'searchjobsportal' in response.url.lower() or 
                'searchjobs' in response.url.lower() or
                'getmore' in response.url.lower()
            )
            
            if is_job_request:
                try:
                    # Only process successful responses
                    if response.status == 200:
                        content_type = response.headers.get('content-type', '')
                        if 'application/json' in content_type:
                            json_data = await response.json()
                            
                            # Determine request type for logging
                            request_type = 'getmore' if 'getmore' in response.url.lower() else 'searchjobsportal'
                            logger.info(f"Captured {request_type} response from: {response.url}")
                            
                            self.network_responses.append({
                                'url': response.url,
                                'status': response.status,
                                'data': json_data,
                                'headers': dict(response.headers),
                                'request_type': request_type
                            })
                        else:
                            # Handle non-JSON responses
                            text_data = await response.text()
                            request_type = 'getmore' if 'getmore' in response.url.lower() else 'searchjobsportal'
                            logger.info(f"Captured non-JSON {request_type} response from: {response.url}")
                            self.network_responses.append({
                                'url': response.url,
                                'status': response.status,
                                'data': text_data,
                                'headers': dict(response.headers),
                                'request_type': request_type
                            })
                except Exception as e:
                    logger.error(f"Error processing network response: {e}")
        
        # Set up the response handler
        page.on('response', handle_response)
        
        async def handle_request(request):
            # Log all requests to help identify the correct endpoint
            is_job_request = any(keyword in request.url.lower() for keyword in ['job', 'search', 'portal', 'getmore'])
            if is_job_request:
                logger.debug(f"Request: {request.method} {request.url}")
                
                # Log pagination info from getmore requests
                if 'getmore' in request.url.lower():
                    import urllib.parse
                    parsed = urllib.parse.urlparse(request.url)
                    params = urllib.parse.parse_qs(parsed.query)
                    from_param = params.get('from', ['unknown'])[0]
                    count_param = params.get('count', ['unknown'])[0]
                    logger.info(f"Getmore request - from: {from_param}, count: {count_param}")
        
        page.on('request', handle_request)

    async def extract_jobs_from_network(self, page_number: int) -> List[Dict]:
        """Extract job data from captured network responses"""
        jobs_from_network = []
        
        # Process the most recent network responses
        for response_data in self.network_responses[-5:]:  # Check last 5 responses
            try:
                data = response_data['data']
                request_type = response_data.get('request_type', 'unknown')
                
                logger.info(f"Processing {request_type} response for page {page_number}")
                
                # Handle different response formats
                if isinstance(data, dict):
                    # Your specific API format with total and data fields
                    jobs = self._extract_jobs_from_dict(data)
                    if jobs:
                        # Add request type metadata to each job
                        for job in jobs:
                            job['request_type'] = request_type
                            job['source_url'] = response_data['url']
                        jobs_from_network.extend(jobs)
                        
                elif isinstance(data, list):
                    # Direct list of jobs (fallback)
                    for item in data:
                        if isinstance(item, dict):
                            job_data = self._normalize_job_data(item, page_number)
                            if job_data:
                                job_data['request_type'] = request_type
                                job_data['source_url'] = response_data['url']
                                jobs_from_network.append(job_data)
                                
            except Exception as e:
                logger.error(f"Error extracting jobs from network data: {e}")
        
        # Clear processed responses to avoid duplicates
        self.network_responses.clear()
        
        return jobs_from_network

    def _extract_jobs_from_dict(self, data: Dict) -> List[Dict]:
        """Extract jobs from dictionary response with common patterns"""
        jobs = []
        
        # Check if this matches your specific API format
        if 'total' in data and 'data' in data and isinstance(data['data'], list):
            logger.info(f"Found {data['total']} total jobs, processing {len(data['data'])} jobs from current page")
            for job_item in data['data']:
                if isinstance(job_item, dict):
                    normalized_job = self._normalize_job_data(job_item, None)
                    if normalized_job:
                        jobs.append(normalized_job)
            return jobs
        
        # Fallback: Common keys where job data might be stored
        possible_job_keys = [
            'data', 'jobs', 'results', 'items', 'listings', 
            'jobListings', 'jobResults', 'positions', 'opportunities'
        ]
        
        for key in possible_job_keys:
            if key in data and isinstance(data[key], list):
                logger.info(f"Found jobs under key: {key}")
                for job_item in data[key]:
                    if isinstance(job_item, dict):
                        normalized_job = self._normalize_job_data(job_item, None)
                        if normalized_job:
                            jobs.append(normalized_job)
                break  # Found jobs, no need to check other keys
        
        return jobs

    def _normalize_job_data(self, job_data: Dict, page_number: int = None) -> Dict:
        """Normalize job data to a standard format"""
        try:
            # Your specific API field mappings
            normalized = {
                'job_id': job_data.get('id'),
                'title': job_data.get('title'),
                'ref_no': job_data.get('refNo'),
                'company': job_data.get('company'),
                'location': job_data.get('location'),
                'main_location': job_data.get('mainLocation'),
                'other_locations': job_data.get('otherLocations', []),
                'description': job_data.get('jobDescription'),
                'position_type': job_data.get('positionType'),
                'pay_rate': job_data.get('payRate'),
                'pay_frequency': job_data.get('payFrequency'),
                'working_remote': job_data.get('workingRemote'),
                'experience': job_data.get('experience'),
                'direct_placement': job_data.get('directPlacement'),
                'screening_enabled': job_data.get('screeningEnabled'),
                
                # Date fields (timestamps)
                'post_date': job_data.get('postDate'),
                'start_date': job_data.get('startDate'),
                'end_date': job_data.get('endDate'),
                'post_date_str': job_data.get('postDateStr'),
                'start_date_str': job_data.get('startDateStr'),
                'end_date_str': job_data.get('endDateStr'),
                
                # Recruiter information
                'primary_recruiter_name': job_data.get('primaryRecruiterName'),
                'primary_recruiter_email': job_data.get('primaryRecruiterEmail'),
                'primary_recruiter_phone': job_data.get('primaryRecruiterPhone'),
                
                # Complex fields
                'job_udfs': job_data.get('jobUDFs', []),
                'healthcare_data': job_data.get('healthcareData', {}),
                'eeo_settings': job_data.get('eeoSettings', {}),
                'eeo': job_data.get('eeo', {}),
                'facilities': job_data.get('facilities', []),
                'screenings': job_data.get('screenings', []),
                'licenses': job_data.get('licenses', {}),
                'certificates': job_data.get('certificates', {}),
                
                # Metadata
                'page_number': page_number,
                'scraped_at': asyncio.get_event_loop().time()
            }
            
            # Convert timestamps to readable dates if they exist
            if normalized['post_date']:
                try:
                    import datetime
                    normalized['post_date_readable'] = datetime.datetime.fromtimestamp(
                        normalized['post_date'] / 1000
                    ).strftime('%Y-%m-%d %H:%M:%S')
                except:
                    pass
                    
            if normalized['start_date'] and normalized['start_date'] > 0:
                try:
                    import datetime
                    normalized['start_date_readable'] = datetime.datetime.fromtimestamp(
                        normalized['start_date'] / 1000
                    ).strftime('%Y-%m-%d %H:%M:%S')
                except:
                    pass
                    
            if normalized['end_date'] and normalized['end_date'] > 0:
                try:
                    import datetime
                    normalized['end_date_readable'] = datetime.datetime.fromtimestamp(
                        normalized['end_date'] / 1000
                    ).strftime('%Y-%m-%d %H:%M:%S')
                except:
                    pass
            
            return normalized
            
        except Exception as e:
            logger.error(f"Error normalizing job data: {e}")
            return None

    async def extract_jobs_from_page(self, page, page_number: int) -> List[Dict]:
        """Extract jobs from network responses for the current page"""
        return await self.extract_jobs_from_network(page_number)

    async def scrape_job_listings_by_pagination(self, company: str = None):
        """Scrape jobs by clicking through pagination buttons and capturing network requests"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless, slow_mo=self.slow_mo)
            context = await browser.new_context()
            page = await context.new_page()
            
            try:
                # Set up network interception before navigation
                await self.setup_network_interception(page)
                
                logger.info(f"Navigating to {self.base_url}")
                await page.goto(self.base_url)
                
                # Wait for initial page load and network requests
                await page.wait_for_load_state('networkidle', timeout=15000)
                
                current_page = 1
                has_next_page = True
                
                while has_next_page:
                    logger.info(f"Processing page {current_page}...")
                    
                    # Wait a bit for network requests to complete
                    await page.wait_for_timeout(3000)
                    
                    # Extract job data from network responses
                    jobs_on_page = await self.extract_jobs_from_page(page, current_page)
                    
                    if jobs_on_page:
                        self.all_job_data.extend(jobs_on_page)
                        logger.info(f"Found {len(jobs_on_page)} jobs on page {current_page} from network data")
                    else:
                        logger.warning(f"No jobs found in network responses for page {current_page}")
                        # Optionally, you could fallback to HTML parsing here
                    
                    # Check if there's a next page button and if it's clickable
                    next_button = await page.query_selector('button[aria-label="Next Page"]:not([disabled])')
                    
                    if next_button:
                        try:
                            # Click next page button
                            logger.info("Clicking next page button...")
                            await next_button.click()
                            
                            # Wait for network requests to complete
                            await page.wait_for_load_state('networkidle', timeout=15000)
                            
                            current_page += 1
                            
                            # Be respectful to the server
                            await page.wait_for_timeout(2000)
                            
                        except Exception as e:
                            logger.error(f"Error navigating to next page: {e}")
                            has_next_page = False
                    else:
                        logger.info("No more pages found or next button is disabled")
                        has_next_page = False
                    
                    # Safety check to prevent infinite loops
                    if current_page > 50:
                        logger.warning("Reached maximum page limit")
                        break
                        
            except Exception as e:
                logger.error(f"Error during scraping: {e}")
            finally:
                await browser.close()
        
        logger.info(f"Scraping completed. Total jobs found: {len(self.all_job_data)}")
        return self.all_job_data

    async def save_to_csv(self, company):
        """Save job data to CSV file"""
        filename = f'{company}.csv'
        try:
            if not self.all_job_data:
                logger.warning("No data to save to CSV")
                return
            
            # Get all unique fieldnames from all jobs
            all_fieldnames = set()
            for job in self.all_job_data:
                all_fieldnames.update(job.keys())
            
            # Sort fieldnames for consistent column order
            fieldnames = sorted(all_fieldnames)
            
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                
                writer.writeheader()
                writer.writerows(self.all_job_data)
            
            logger.info(f"Data saved to {filename}")
        except Exception as e:
            logger.error(f"Error saving CSV: {e}")

    def print_jobs_summary(self):
        """Print a summary of scraped jobs"""
        if not self.all_job_data:
            logger.info("No jobs were scraped")
            return
            
        logger.info(f"\n=== JOB SCRAPING SUMMARY ===")
        logger.info(f"Total jobs scraped: {len(self.all_job_data)}")
        
        # Print first few jobs as examples
        for i, job in enumerate(self.all_job_data[:3]):
            logger.info(f"\n--- Job {i+1} ---")
            logger.info(f"ID: {job.get('job_id')}")
            logger.info(f"Title: {job.get('title')}")
            logger.info(f"Company: {job.get('company')}")
            logger.info(f"Location: {job.get('location')}")
            logger.info(f"Ref No: {job.get('ref_no')}")
            logger.info(f"Post Date: {job.get('post_date_readable', job.get('post_date'))}")
            if job.get('description'):
                # Truncate description for summary
                desc = job.get('description')[:200] + "..." if len(str(job.get('description', ''))) > 200 else job.get('description')
                logger.info(f"Description: {desc}")
        
        # Print some statistics
        companies = [job.get('company') for job in self.all_job_data if job.get('company')]
        locations = [job.get('location') for job in self.all_job_data if job.get('location')]
        
        if companies:
            from collections import Counter
            company_counts = Counter(companies)
            logger.info(f"\nTop 5 Companies:")
            for company, count in company_counts.most_common(5):
                logger.info(f"  {company}: {count} jobs")
        
        if locations:
            from collections import Counter
            location_counts = Counter(locations)
            logger.info(f"\nTop 5 Locations:")
            for location, count in location_counts.most_common(5):
                logger.info(f"  {location}: {count} jobs")

    def reset_data(self):
        """Reset job data for a fresh start"""
        self.all_job_data = []
        self.network_responses = []

    def get_jobs_count(self):
        """Get current count of scraped jobs"""
        return len(self.all_job_data)

    def add_metadata_to_jobs(self, metadata_dict):
        """Add metadata to all existing jobs"""
        for job in self.all_job_data:
            job.update(metadata_dict)

    def add_job_if_unique(self, job_data):
        """Add job only if not already exists (based on job_id)"""
        job_id = job_data.get('job_id')
        existing_ids = {job.get('job_id') for job in self.all_job_data}
        
        if job_id not in existing_ids:
            self.all_job_data.append(job_data)
            return True
        return False

# Helper functions for saving data
async def save_combined_data(all_data):
    """Save combined data from all companies"""
    filename = 'all_companies_jobs.csv'
    try:
        if not all_data:
            logger.warning("No combined data to save to CSV")
            return
        
        # Get all unique fieldnames from all jobs
        all_fieldnames = set()
        for job in all_data:
            all_fieldnames.update(job.keys())
        
        # Sort fieldnames for consistent column order
        fieldnames = sorted(all_fieldnames)
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            
            writer.writeheader()
            writer.writerows(all_data)
        
        logger.info(f"Combined data saved to {filename}")
    except Exception as e:
        logger.error(f"Error saving combined CSV: {e}")

async def save_company_jobs(company, jobs):
    """Save jobs for a specific company"""
    filename = f'{company}.csv'
    try:
        if not jobs:
            logger.warning(f"No data to save for {company}")
            return
        
        # Get all unique fieldnames from all jobs
        all_fieldnames = set()
        for job in jobs:
            all_fieldnames.update(job.keys())
        
        # Sort fieldnames for consistent column order
        fieldnames = sorted(all_fieldnames)
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            
            writer.writeheader()
            writer.writerows(jobs)
        
        logger.info(f"Data for {company} saved to {filename}")
    except Exception as e:
        logger.error(f"Error saving CSV for {company}: {e}")

# Main function - approach 1: Separate scraper per company
async def main():
    # Replace with your actual job portal URL
    cdata = []
    with open('companies.csv') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',')
        for row in spamreader:
            cdata.append(row)

    all_companies_data = []  # Store data for all companies
    
    for company, url in cdata:
        logger.info(f"Starting scrape for company: {company}")
        
        scraper = NetworkJobScraper(
            base_url=url,
            headless=False,  # Set to True for production
            slow_mo=1000     # Slow down for debugging
        )
        
        try:
            # Scrape jobs for this company
            jobs = await scraper.scrape_job_listings_by_pagination()
            
            # Add company identifier to each job
            for job in jobs:
                job['source_company'] = company
                job['source_url_base'] = url
            
            # Add to overall collection
            all_companies_data.extend(jobs)
            
            # Print summary for this company
            logger.info(f"=== SUMMARY FOR {company} ===")
            scraper.print_jobs_summary()
            
            # Save individual company file
            await scraper.save_to_csv(company)
            
        except Exception as e:
            logger.error(f"Scraping failed for {company}: {e}")
            continue  # Continue to next company instead of returning
    
    # Save combined data for all companies
    if all_companies_data:
        await save_combined_data(all_companies_data)
        logger.info(f"Total jobs across all companies: {len(all_companies_data)}")
    
    return all_companies_data

# Alternative main function - approach 2: Single scraper instance
async def main_single_scraper():
    """Alternative approach using single scraper instance"""
    cdata = []
    with open('companies.csv') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',')
        for row in spamreader:
            cdata.append(row)

    # Single scraper instance that accumulates all data
    scraper = NetworkJobScraper(
        base_url="",  # Will be updated for each company
        headless=False,
        slow_mo=1000
    )
    
    for company, url in cdata:
        logger.info(f"Starting scrape for company: {company}")
        
        # Update the base URL for this company
        scraper.base_url = url
        
        try:
            # Get current count before scraping
            jobs_before = len(scraper.all_job_data)
            
            # Scrape jobs for this company
            await scraper.scrape_job_listings_by_pagination()
            
            # Add company identifier to newly scraped jobs
            jobs_after = len(scraper.all_job_data)
            for i in range(jobs_before, jobs_after):
                scraper.all_job_data[i]['source_company'] = company
                scraper.all_job_data[i]['source_url_base'] = url
            
            # Save individual company file
            company_jobs = scraper.all_job_data[jobs_before:jobs_after]
            await save_company_jobs(company, company_jobs)
            
            logger.info(f"Scraped {jobs_after - jobs_before} jobs for {company}")
            
        except Exception as e:
            logger.error(f"Scraping failed for {company}: {e}")
            continue
    
    # Print final summary and save all data
    scraper.print_jobs_summary()
    await scraper.save_to_csv("all_companies")
    
    return scraper.all_job_data

if __name__ == "__main__":
    # Use main() for approach 1 (separate scrapers) or main_single_scraper() for approach 2
    asyncio.run(main())