import json
from location_parser import location_parser
import asyncio
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from supabase import create_client, Client
from typing import Dict, List, Any
import re
from users.job_matcher import load_email_template



def send_email(to_email: str, subject: str, html_content: str) -> bool:
    """Send an HTML email via Gmail SMTP."""


    GMAIL_ADDRESS = "03jawad.ali@gmail.com"  # <-- UPDATE THIS
    GMAIL_APP_PASSWORD = "oazl rwfo eqem qetk"  # <-- UPDATE THIS
    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = GMAIL_ADDRESS
        msg['To'] = to_email
        
        html_part = MIMEText(html_content, 'html')
        msg.attach(html_part)
        
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
        server.sendmail(GMAIL_ADDRESS, to_email, msg.as_string(),)
        server.quit()
        
        print(f"    ✓ Email sent to {to_email}")
        return True
    
    except Exception as e:
        print(f"    ✗ Failed to send email to {to_email}: {e}")
        return False


if __name__ == "__main__":
    template = load_email_template()
    send_email("03jawad.ali@gmail.com", "This is a test", template)

    

        

