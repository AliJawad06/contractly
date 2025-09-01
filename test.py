import asyncio
import json
import csv
from datetime import datetime
from playwright.async_api import async_playwright
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class JobScraper:
    def __init__(self, base_url, headless=False, slow_mo=1000):
        self.base_url = base_url
        self.headless = headless
        self.slow_mo = slow_mo
        self.all_job_data = []


    


    
    async def scrape_job_listings_by_pagination(self,company):
        """Scrape jobs by clicking through pagination buttons"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless, slow_mo=self.slow_mo)
            context = await browser.new_context()
            page = await context.new_page()
            
            try:
                logger.info(f"Navigating to {self.base_url}")
                await page.goto(self.base_url)
                
                # Wait for job listings to load
                await page.wait_for_selector('.list-group-item', timeout=10000)
                
                current_page = 1
                has_next_page = True
                
                while has_next_page:
                    logger.info(f"Scraping page {current_page}...")
                    
                    # Wait for jobs to load on current page
                    await page.wait_for_selector('.list-group-item', timeout=10000)
                    
                    # Extract job data from current page
                    jobs_on_page = await self.extract_jobs_from_page(page, current_page)
                    # Add jobs from current page to overall collection
                    self.all_job_data.extend(jobs_on_page)
                    logger.info(f"Found {len(jobs_on_page)} jobs on page {current_page}")
                    
                    # Check if there's a next page button and if it's clickable
                    next_button = await page.query_selector('button[aria-label="Next Page"]:not([disabled])')
                    
                    if next_button:
                        try:
                            # Click next page button
                            await next_button.click()
                            
                            # Wait for page to load
                            await page.wait_for_timeout(2000)
                            
                            # Wait for new content to load
                            await page.wait_for_selector('.list-group-item', timeout=10000)
                            
                            current_page += 1
                            
                            # Be respectful to the server
                            await page.wait_for_timeout(1000)
                            
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
        
        return self.all_job_data
    
    async def scrape_job_listings_by_url(self,company):
        """Scrape jobs by constructing URLs with page parameters"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            context = await browser.new_context()
            page = await context.new_page()
            
            try:
                current_page = 1
                has_more_pages = True
                
                while has_more_pages:
                    # Construct URL with page parameter (adjust based on actual URL structure)
                    page_url = f"{self.base_url}?page={current_page}"
                    
                    try:
                        logger.info(f"Navigating to page {current_page}: {page_url}")
                        await page.goto(page_url)
                        
                        # Wait for job listings to load
                        await page.wait_for_selector('.list-group-item', timeout=10000)
                        
                        # Check if there are any job listings on this page
                        job_elements = await page.query_selector_all('.list-group-item')
                        
                        if len(job_elements) == 0:
                            logger.info("No jobs found on this page, stopping")
                            has_more_pages = False
                            break
                        
                        # Extract job data
                        jobs_on_page = await self.extract_jobs_from_page(page, current_page)
                        

                        self.all_job_data.extend(jobs_on_page)
                        logger.info(f"Found {len(jobs_on_page)} jobs on page {current_page}")
                        last_job = self.get_last_job(company)
                        if last_job in jobs_on_page:
                            logger.info("reached end, stopping")
                            break
                        current_page += 1
                        
                        # Add delay between requests
                        await page.wait_for_timeout(1000)
                        
                    except Exception as e:
                        logger.error(f"Error on page {current_page}: {e}")
                        has_more_pages = False
                    
                    # Safety limit
                    if current_page > 50:
                        logger.warning("Reached maximum page limit")
                        break
                        
            except Exception as e:
                logger.error(f"Error during URL-based scraping: {e}")
            finally:
                await browser.close()
        
        return self.all_job_data
    


    async def get_last_job(self,company):
        with open(f'csv/{company}/last_job.json') as file:
            last_job = json.load(file)
            return last_job
        

    async def extract_jobs_from_page(self, page, page_number):
        """Extract job data from current page"""
        jobs = await page.evaluate('''
            () => {
                const jobElements = document.querySelectorAll('.list-group-item');
                const jobs = [];
                
                jobElements.forEach(jobElement => {
                    try {
                        // Extract job title
                        const titleElement = jobElement.querySelector('.jd-nav-label');
                        const title = titleElement ? titleElement.textContent.trim() : 'N/A';
                        
                        // Extract date (first small element)
                        const dateElement = jobElement.querySelector('.text-muted small:first-child');
                        const date = dateElement ? dateElement.textContent.trim() : 'N/A';
                        
                        // Extract job ID (second small element)
                        const jobIdElement = jobElement.querySelector('.text-muted small:nth-child(2)');
                        const jobId = jobIdElement ? jobIdElement.textContent.trim() : 'N/A';
                        
                        // Extract location (third small element)
                        const locationElement = jobElement.querySelector('.text-muted small:nth-child(3)');
                        const location = locationElement ? locationElement.textContent.trim() : 'N/A';
                        
                        // Extract job description preview
                        const descriptionElement = jobElement.querySelector('.text-truncate');
                        const description = descriptionElement ? descriptionElement.textContent.trim() : 'N/A';
                        
                        // Get current URL
                        const url = window.location.href;
                        
                        jobs.push({
                            title: title,
                            date: date,
                            jobId: jobId,
                            location: location,
                            description: description,
                            url: url
                        });
                    } catch (error) {
                        console.log('Error extracting job data:', error);
                    }
                });
                
                return jobs;
            }
        ''')
        
        # Add metadata to each job
        for job in jobs:
            job['scraped_at'] = datetime.now().isoformat()
            job['page_number'] = page_number
        
        return jobs
    
    def save_to_json(self, company):
        """Save job data to JSON file"""
        filename = f'{company}.json'

        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.all_job_data, f, indent=2, ensure_ascii=False)
            logger.info(f"Data saved to {filename}")
        except Exception as e:
            logger.error(f"Error saving JSON: {e}")
    
    def save_to_csv(self, company):
        """Save job data to CSV file"""
        filename = f'{company}.csv'
        try:
            if not self.all_job_data:
                logger.warning("No data to save to CSV")
                return
            
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                fieldnames = self.all_job_data[0].keys()
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                
                writer.writeheader()
                writer.writerows(self.all_job_data)
            
            logger.info(f"Data saved to {filename}")
        except Exception as e:
            logger.error(f"Error saving CSV: {e}")
    
    def print_summary(self):
        """Print scraping summary"""
        total_jobs = len(self.all_job_data)
        print("\n" + "="*50)
        print("SCRAPING SUMMARY")
        print("="*50)
        print(f"Total jobs scraped: {total_jobs}")
        
        if total_jobs > 0:
            page_numbers = [job['page_number'] for job in self.all_job_data]
            max_page = max(page_numbers)
            print(f"Pages scraped: {max_page}")
            print(f"Average jobs per page: {total_jobs / max_page:.1f}")
            
            # Show sample job data
            print("\nSample job data:")
            print("-" * 30)
            sample_job = self.all_job_data[0]
            for key, value in sample_job.items():
                print(f"{key}: {value}")
            
            # Show job distribution by location
            locations = {}
            for job in self.all_job_data:
                loc = job['location']
                locations[loc] = locations.get(loc, 0) + 1
            
            print(f"\nTop 5 locations:")
            print("-" * 20)
            sorted_locations = sorted(locations.items(), key=lambda x: x[1], reverse=True)[:5]
            for location, count in sorted_locations:
                print(f"{location}: {count} jobs")

async def main():
    """Main execution function"""
    # Configuration
    HEADLESS = False  # Set to True for headless mode
    cdata = []
    with open('companies.csv') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',')
        for row in spamreader:
            cdata.append(row)


    for company, url in cdata:
        BASE_URL = url
         # Replace with actual URL
        try:
            # Initialize scraper
            
            scraper = JobScraper(BASE_URL, headless=HEADLESS)
            
            print("Starting job scraping...")
            
            # Choose scraping method:
            # Method 1: Navigate using next page buttons
            job_data = await scraper.scrape_job_listings_by_pagination(company)
            for job in job_data:
                job["Contractor"] = company
            # Method 2: Navigate using URL parameters (uncomment if needed)
            # job_data = await scraper.scrape_job_listings_by_url()
            # Save data
            scraper.save_to_json(company)
            scraper.save_to_csv(company)
            # Print summary
            scraper.print_summary()
            
        except Exception as e:
            logger.error(f"Error in main execution: {e}")

# Additional utility functions
def filter_jobs_by_location(job_data, location):
    """Filter jobs by location"""
    return [job for job in job_data if location.lower() in job['location'].lower()]

def filter_jobs_by_date(job_data, date_string):
    """Filter jobs by specific date"""
    return [job for job in job_data if job['date'] == date_string]

def filter_jobs_by_keywords(job_data, keywords):
    """Filter jobs by keywords in title or description"""
    filtered_jobs = []
    for job in job_data:
        job_text = f"{job['title']} {job['description']}".lower()
        if any(keyword.lower() in job_text for keyword in keywords):
            filtered_jobs.append(job)
    return filtered_jobs

# Example usage of filters
async def example_with_filters():
    """Example showing how to use filters"""
    scraper = JobScraper("YOUR_URL", headless=True)
    job_data = await scraper.scrape_job_listings_by_pagination()
    
    # Filter examples
    raleigh_jobs = filter_jobs_by_location(job_data, "Raleigh")
    print(f"Jobs in Raleigh: {len(raleigh_jobs)}")
    
    recent_jobs = filter_jobs_by_date(job_data, "08/26/2025")
    print(f"Jobs posted on 08/26/2025: {len(recent_jobs)}")
    
    tech_jobs = filter_jobs_by_keywords(job_data, ["developer", "engineer", "programmer"])
    print(f"Tech jobs: {len(tech_jobs)}")

if __name__ == "__main__":
    # Run the scraper
    asyncio.run(main())
    
    # Uncomment to run with filters example
    # asyncio.run(example_with_filters())