from playwright.sync_api import sync_playwright
import time

def automate_job_application(url):
    with sync_playwright() as p:
        # Launch browser (set headless=False to see the automation)
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        
        try:
            # Navigate to the URL
            print(f"Navigating to {url}")
            page.goto(url)
            
            # Click "Apply Now" button
            print("Clicking 'Apply Now' button...")
            page.get_by_role("button", name="Apply Now").click()
            time.sleep(1)  # Wait for page to load
            
            # Click "Quick Apply (No Account)" button
            print("Clicking 'Quick Apply (No Account)' button...")
            page.get_by_role("button", name="Quick Apply (No Account)").click()
            time.sleep(1)
            
            # Fill in First Name - using the label text to find the input
            print("Filling in First Name...")
            first_name_input = page.locator('label:has-text("First Name")').locator('..').locator('input')
            first_name_input.fill("Ali")
            
            # Fill in Last Name
            print("Filling in Last Name...")
            last_name_input = page.locator('label:has-text("Last Name")').locator('..').locator('input')
            last_name_input.fill("Jawad")
            
            # Fill in Email
            print("Filling in Email...")
            email_input = page.locator('label:has-text("Email")').locator('..').locator('input')
            email_input.fill("03jawad.ali@gmail.com")
            
            # Upload resume file
            print("Uploading resume...")
            page.locator('input[type="file"]').set_input_files("/Users/aj/Downloads/AJWMResume.pdf")
            time.sleep(10)
            
            # Click Submit Application button
            print("Clicking 'Submit Application' button...")
            page.get_by_role("button", name="Submit Application").click()
            
            print("Application submitted successfully!")
            time.sleep(3)  # Wait to see the result
            
        except Exception as e:
            print(f"An error occurred: {e}")
            import traceback
            traceback.print_exc()
            
        finally:
            # Close browser
            browser.close()

if __name__ == "__main__":
    # Replace with your actual URL
    job_url = "https://www1.jobdiva.com/portal/?a=xbjdnwgjodtga1y1im2g881fkkeiwd0775lbvq8yqgps8vb2q36w2vj1ga6xxork&jobid=30599073&source=google&SearchString=#/jobs/31411832"
    automate_job_application(job_url)