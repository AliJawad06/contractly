import asyncio
import json
import logging
import time
from pathlib import Path
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from datetime import datetime
from dateutil import parser
from tests.findlastJob import findLast

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CeipalJobScraper:
    def __init__(self, maxT, company_name, base_url, browser, options=None):
        self.company_name = company_name
        self.maxT = maxT
        self.base_url = base_url
        self.browser = browser  # Shared browser instance passed in
        self.options = {
            'timeout': 30000,
            'delay': 2,
            'max_retries': 3,
            'min_timestamp': 1761177600000
        }
        if options:
            self.options.update(options)

        self.all_jobs = []
        self.context = None
        self.page = None
        self.page_number = 1
        self.should_stop_scraping = False

    async def initialize(self):
        """Open an isolated browser context and page for this scraper"""
        # Spoof a real (non-headless) Chrome to bypass Ceipal's bot detection.
        # The server inspects User-Agent and sec-ch-ua client hints — if either
        # says "HeadlessChrome" it returns 400 "Bot access is not allowed".
        real_chrome_ua = (
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
            '(KHTML, like Gecko) Chrome/139.0.7258.5 Safari/537.36'
        )
        self.context = await self.browser.new_context(
            viewport={'width': 1280, 'height': 800},
            java_script_enabled=True,
            user_agent=real_chrome_ua,
            extra_http_headers={
                # Override client hints — these are what give away HeadlessChrome
                # even when User-Agent is spoofed.
                'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Linux"',
            }
        )

        # Remove the navigator.webdriver flag and other automation tells
        # before any page script runs.
        await self.context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
            window.chrome = { runtime: {} };
        """)

        self.page = await self.context.new_page()
        self.page.set_default_timeout(self.options['timeout'])

    async def close(self):
        """Close the context (and its page), not the shared browser"""
        try:
            if self.context:
                await self.context.close()
        except Exception as e:
            logger.debug(f"Error closing context for {self.company_name}: {e}")

    def extract_base_url(self, full_url):
        if 'index.html' in full_url:
            return full_url.split('index.html')[0] + 'index.html'
        return full_url

    def build_job_url(self, base_url, job_id):
        base = self.extract_base_url(base_url)
        if '?' in base:
            return f"{base}&job_id={job_id}"
        else:
            return f"{base}?job_id={job_id}"

    def _start_response_listener(self, url_pattern):
        """
        Attach a response listener IMMEDIATELY (synchronously) so we don't miss
        responses that fire during navigation. Returns (future, cleanup_fn) —
        await the future to get the response data.
        """
        future = asyncio.get_event_loop().create_future()

        async def handle_response(response):
            if future.done():
                return
            if url_pattern not in response.url:
                return
            logger.info(f"[{self.company_name}] Matched response: {response.url} (status={response.status})")

            # If the server returned an error, log the body to help diagnose
            if response.status >= 400:
                try:
                    err_body = await response.text()
                    logger.error(f"[{self.company_name}] Error body: {err_body[:500]}")
                    # Also log request headers so we can compare against the working browser request
                    req = response.request
                    logger.error(f"[{self.company_name}] Request headers: {await req.all_headers()}")
                except Exception as e:
                    logger.error(f"[{self.company_name}] Could not read error body: {e}")
                return  # don't set the future, let it timeout / move on

            try:
                # Try .json() first
                try:
                    data = await response.json()
                except Exception:
                    # Fallback: get body as text and parse manually
                    body = await response.body()
                    text = body.decode('utf-8', errors='replace')
                    data = json.loads(text)
                if not future.done():
                    future.set_result(data)
            except Exception as e:
                logger.error(f"[{self.company_name}] Failed to parse {response.url}: {e}")

        self.page.on('response', handle_response)

        def cleanup():
            try:
                self.page.remove_listener('response', handle_response)
            except Exception:
                pass

        return future, cleanup

    async def wait_for_network_response(self, url_pattern, timeout=30):
        """Standalone wait — for cases where you don't need to pre-arm the listener."""
        future, cleanup = self._start_response_listener(url_pattern)
        try:
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            logger.debug(f"Timeout waiting for response matching '{url_pattern}'")
            return None
        finally:
            cleanup()

    async def get_next_page_number(self):
        try:
            next_page_number = self.page_number + 1
            next_page_selector = f"div.colPagnation ul li a[onclick*='loadJobPostings({next_page_number})']"
            next_page_elem = await self.page.query_selector(next_page_selector)
            if next_page_elem:
                return next_page_number
            for selector in [
                "a:has-text('Next')",
                ".pagination .next",
                "[aria-label='Next page']",
                ".page-next"
            ]:
                if await self.page.query_selector(selector):
                    return next_page_number
            return None
        except Exception as e:
            logger.error(f"Error getting next page number: {e}")
            return None

    async def navigate_to_next_page(self, next_page_number):
        try:
            self.page_number = next_page_number
            li_elem = await self.page.query_selector(
                f"div.colPagnation ul li:has-text('{self.page_number}')"
            ) or await self.page.query_selector("div.colPagnation a:has-text('Next >')")

            if li_elem:
                future, cleanup = self._start_response_listener('CareerPortalJobPostings')
                try:
                    await li_elem.click()
                    logger.info(f"Navigated to page: {self.page_number}")
                    await self.page.wait_for_load_state('networkidle')
                    try:
                        return await asyncio.wait_for(future, timeout=30)
                    except asyncio.TimeoutError:
                        logger.warning(f"Timeout on page {self.page_number}")
                        return None
                finally:
                    cleanup()
            else:
                logger.warning(f"Could not find pagination element for page {self.page_number}")
                return None
        except Exception as e:
            logger.error(f"Error navigating to page {self.page_number}: {e}")
            return None

    async def scrape_jobs_list(self, page_url):
        logger.info(f"Navigating to: {page_url}")
        try:
            # DIAGNOSTIC: log every response URL so we can see what's actually happening
            seen_urls = []
            def log_response(response):
                seen_urls.append(response.url)
            self.page.on('response', log_response)

            # Attach listener BEFORE goto so we don't miss the response
            future, cleanup = self._start_response_listener('CareerPortalJobPostings')
            try:
                await self.page.goto(page_url, wait_until='networkidle')
                try:
                    jobs_data = await asyncio.wait_for(future, timeout=30)
                except asyncio.TimeoutError:
                    logger.warning(f"[{self.company_name}] Timeout waiting for CareerPortalJobPostings response")
                    # Dump what we DID see
                    logger.warning(f"[{self.company_name}] Saw {len(seen_urls)} responses. Sample URLs:")
                    for u in seen_urls[:20]:
                        logger.warning(f"  -> {u}")
                    return None
            finally:
                cleanup()
                try:
                    self.page.remove_listener('response', log_response)
                except Exception:
                    pass

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

    async def scrape_job_details(self, job_basic_info):
        job_id = job_basic_info.get('id')
        if not job_id:
            logger.warning("No job ID found")
            return None

        logger.info(f"Scraping job details for ID: {job_id}")
        try:
            job_selector = f'a[data-job-id="{job_id}"]' if "v1" in self.base_url else f'div[data-job-id="{job_id}"]'
            await self.page.wait_for_selector(job_selector, timeout=10000)

            future, cleanup = self._start_response_listener(str(job_id))
            try:
                await self.page.click(job_selector)
                logger.info(f"Clicked on job div for ID: {job_id}")
                await self.page.wait_for_load_state('networkidle')
                try:
                    job_details = await asyncio.wait_for(future, timeout=15)
                except asyncio.TimeoutError:
                    job_details = None
            finally:
                cleanup()

            if job_details:
                extracted = self.extract_job_fields(job_details)
                logger.info(f"Successfully scraped job: {extracted.get('title', 'Unknown')}")
                return extracted
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
                'experience': job_data.get('experience', 'N/A'),
                'min_experience': job_data.get('min_experience', 'N/A')
            },
            'skills': next(
                (o["value"] for o in job_data.get('display_job_view_fields', []) if o.get("label") == "Primary Skills"),
                None
            ),
            'posted': {
                'date_postedStr': date_postedStr,
                'date_posted': self.date_to_time(date_postedStr)
            },
            'description': job_data.get('public_job_desc', None),
            'closing': {
                'closing_date': closing_date,
                'closing_dateStr': self.date_to_time(date_postedStr)
            },
            'start': {
                'start_date': start_date,
                'start_dateStr': self.date_to_time(start_date)
            },
            'end': {
                'end_dateStr': end_date,
                'end_date': self.date_to_time(end_date)
            },
            'salary': pay_rate,
            'remote': job_data.get('remote_opportunities', 0),
            'assigned_recruiter': job_data.get('assigned_recruiter', 'N/A'),
            'recruiter': {
                'contact': BeautifulSoup(job_data.get('contact_person', 'N/A'), "html.parser").text or "N/A",
                'client': job_data.get('client', 'N/A')
            },
            'apply': job_data.get('easy_apply_job_login_hc', ''),
            'url': url
        }

    async def go_back_to_jobs_list(self):
        try:
            await asyncio.sleep(0.8)
            for selector in [
                'text="<< Back to all jobs"',
                'text="Back to all jobs"',
                'a:has-text("Back")',
                '.back-link',
                '[title*="back"]'
            ]:
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
                    detailed_job = await self.scrape_job_details(job)

                    if detailed_job:
                        job_timestamp = detailed_job.get('posted', {}).get('date_posted')
                        if int(job_timestamp) < findLast("day/jobsapi_day.json"):
                            logger.info(f"Job {detailed_job.get('id')} is older than cutoff. Stopping for {self.company_name}.")
                            self.should_stop_scraping = True
                            break

                        self.all_jobs.append(detailed_job)
                        logger.info(f"Added job {detailed_job.get('id')} - posted at {job_timestamp}")

                    await self.go_back_to_jobs_list()
                    await asyncio.sleep(self.options['delay'])

                if self.should_stop_scraping:
                    logger.info(f"Early stop for {self.company_name}. Total: {len(self.all_jobs)}")
                    break

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
                if job.get('description'):
                    job['description'] = BeautifulSoup(job['description'], "html.parser").text
            output = {
                'company': self.company_name,
                'scrape_date': datetime.now().isoformat(),
                'total_jobs': len(self.all_jobs),
                'jobs': self.all_jobs
            }
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(output, f, indent=2, ensure_ascii=False)
            logger.info(f"Results saved to {filename} ({len(self.all_jobs)} jobs)")
        except Exception as e:
            logger.error(f"Error saving JSON: {e}")

    async def run(self):
        logger.info(f"Starting Ceipal scraper for {self.company_name}...")
        await self.initialize()
        try:
            await self.scrape_url()
            self.save_to_json()
            logger.info(f"Done: {self.company_name} — {len(self.all_jobs)} jobs")
        except Exception as e:
            logger.error(f"Error during scraping {self.company_name}: {e}")
        finally:
            await self.close()
        return self.all_jobs


async def scrape_company(browser, company_data, options=None):
    """Scrape a single company using a shared browser instance."""
    if isinstance(company_data, (tuple, list)):
        url, company = company_data[0], company_data[1]
    elif isinstance(company_data, dict):
        url, company = company_data.get('url'), company_data.get('company')
    else:
        raise ValueError("company_data must be a tuple/list or dict")

    scraper = CeipalJobScraper(
        maxT=1762637911000,
        company_name=company,
        base_url=url,
        browser=browser,
        options=options or {}
    )
    try:
        jobs = await scraper.run()
        for j in jobs:
            j['company'] = company
        return jobs
    except Exception as e:
        logger.error(f"Error scraping {company}: {e}")
        return []


async def save_combined_json(all_job_details, date_cutoff_days=None):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    date_suffix = f"_last_{date_cutoff_days}_days" if date_cutoff_days is not None else ""
    filename = f"all_companies_job_details{date_suffix}_{timestamp}.json"
    Path("json/jobsapi").mkdir(parents=True, exist_ok=True)
    filepath = Path("json/jobsapi") / filename
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
        with open("day/jobsapi_day.json", 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved {len(all_job_details)} jobs to {filepath}")
    except Exception as e:
        logger.error(f"Error saving combined JSON: {e}")


async def main(companies_data, max_concurrent: int = 4, options: dict = None,
               date_cutoff_days: int = None, date_cutoff_timestamp: int = None):
    """
    Single event loop, single browser, pure asyncio concurrency.
    No threads, no subprocess transport issues.
    """
    if not companies_data:
        logger.error("No company data provided")
        return

    if options is None:
        options = {}
    if date_cutoff_timestamp:
        options['min_timestamp'] = date_cutoff_timestamp

    logger.info(f"Launching browser for {len(companies_data)} companies (max_concurrent={max_concurrent})")

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=options.get('headless', True),
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-blink-features=AutomationControlled',
            ]
        )

        # Semaphore limits how many companies scrape simultaneously
        semaphore = asyncio.Semaphore(max_concurrent)

        async def scrape_with_limit(company_data):
            async with semaphore:
                return await scrape_company(browser, company_data, options)

        results = await asyncio.gather(
            *[scrape_with_limit(c) for c in companies_data],
            return_exceptions=True
        )

        await browser.close()

    all_jobs = []
    for company_data, result in zip(companies_data, results):
        name = company_data.get('company') if isinstance(company_data, dict) else company_data[1]
        if isinstance(result, Exception):
            logger.error(f"{name} failed: {result}")
        elif result:
            all_jobs.extend(result)
            logger.info(f"{name}: {len(result)} jobs")
        else:
            logger.warning(f"{name}: no jobs found")

    if all_jobs:
        await save_combined_json(all_jobs, date_cutoff_days=date_cutoff_days)
        logger.info(f"Total jobs scraped: {len(all_jobs)}")
    else:
        logger.warning("No jobs scraped from any company")


if __name__ == "__main__":
    with open("/root/contractly/jacompanies.json", "r") as f:
        companies = json.load(f)

    asyncio.run(main(
        companies_data=companies,
        max_concurrent=4,
        options={
            'headless': True,
            'delay': 2,
            'timeout': 30000
        },
        date_cutoff_timestamp=1730419200000
    ))
