#!/usr/bin/env python3
"""
Job Matching and Email Notification System

This script:
1. Loads all jobs from day.json and stores them in the jobs table
2. For each user:
   - Finds jobs matching their industries
   - Calculates relevancy scores based on skill matching (Levenshtein distance ≤ 3)
   - Stores matches in matched_jobs table (url + relevancy_score)
3. Sends email notifications with top 2 jobs per user
"""

import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from supabase import create_client, Client
from typing import Dict, List, Any
import re


# =============================================================================
# CONFIGURATION - UPDATE THESE VALUES
# =============================================================================

# Supabase credentials
SUPABASE_URL = "https://cvfqdvugrnuvdmzpodks.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImN2ZnFkdnVncm51dmRtenBvZGtzIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjI3NDkzMjgsImV4cCI6MjA3ODMyNTMyOH0.5S2--uDnOpX_QXWQfuYA_cVzHwhr3eaMpEYEU_ujNJI"

# Gmail SMTP credentials - UPDATE THESE
GMAIL_ADDRESS = "03jawad.ali@gmail.com"  # <-- UPDATE THIS
GMAIL_APP_PASSWORD = "oazl rwfo eqem qetk"  # <-- UPDATE THIS

# Path to jobs JSON file
JOBS_FILE_PATH = "/Users/aj/Downloads/DhikrUp/contractJobs/day/day.json"


# =============================================================================
# FUZZY MATCHING UTILITIES
# =============================================================================

def levenshtein_distance(s1: str, s2: str) -> int:
    """Calculate the Levenshtein edit distance between two strings."""
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)
    
    if len(s2) == 0:
        return len(s1)
    
    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row
    
    return previous_row[-1]


def skill_matches_text(skill: str, text: str, max_distance: int = 3) -> bool:
    """
    Check if a skill matches any word in the text with fuzzy matching.
    
    Args:
        skill: The skill to search for
        text: The text to search in (job description)
        max_distance: Maximum Levenshtein distance for a match
    
    Returns:
        True if skill matches any word in text within the distance threshold
    """
    skill_lower = skill.lower().strip()
    text_lower = text.lower()
    
    # Check if skill appears as a substring (handles multi-word skills)
    if skill_lower in text_lower:
        return True
    
    # Extract words from text (alphanumeric sequences including + and #)
    words = re.findall(r'\b[a-zA-Z0-9+#]+\b', text_lower)
    
    for word in words:
        # Exact match
        if skill_lower == word:
            return True
        
        # Fuzzy match (only for words of reasonable length)
        if len(skill_lower) >= 3 and len(word) >= 3:
            distance = levenshtein_distance(skill_lower, word)
            if distance <= max_distance:
                return True
    
    return False


def calculate_relevancy_score(user_skills: List[str], job_description: str) -> int:
    """
    Calculate relevancy score based on skill matches.
    
    Args:
        user_skills: List of user's skills
        job_description: Job description text
    
    Returns:
        Number of matching skills (relevancy score)
    """
    if not user_skills or not job_description:
        return 0
    
    score = 0
    for skill in user_skills:
        if skill_matches_text(skill, job_description, max_distance=3):
            score += 1
    
    return score


# =============================================================================
# SUPABASE OPERATIONS
# =============================================================================

def get_supabase_client() -> Client:
    """Create and return a Supabase client."""
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def load_jobs_from_file(file_path: str) -> List[Dict[str, Any]]:
    """Load jobs from JSON file."""
    with open(file_path, 'r') as f:
        return json.load(f)


def upsert_jobs_to_database(supabase: Client, jobs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Insert or update all jobs in the jobs table.
    
    Returns:
        Dict with 'success_count', 'failed_count', 'failed_jobs', and 'duplicate_count'
    """
    success_count = 0
    failed_jobs = []
    
    # Deduplicate jobs by URL (keep the last occurrence)
    seen_urls = {}
    for job in jobs:
        url = job.get("url")
        if url:
            seen_urls[url] = job
    
    duplicate_count = len(jobs) - len(seen_urls)
    if duplicate_count > 0:
        print(f"    Found {duplicate_count} duplicate URLs, keeping unique jobs only")
    
    # Transform deduplicated jobs to match table schema
    job_records = []
    for url, job in seen_urls.items():
        job_records.append({
            "url": url,
            "title": job.get("title"),
            "category": job.get("category"),
            "location": job.get("location"),
            "description": job.get("description"),
            "experience": job.get("experience"),
            "remote": job.get("remote", 0),
            "posted": job.get("posted"),
            "posted_str": job.get("postedStr"),
            "start": job.get("start"),
            "start_str": job.get("startStr"),  
        })
    
    # Upsert in batches
    batch_size = 50
    for i in range(0, len(job_records), batch_size):
        batch = job_records[i:i + batch_size]
        try:
            supabase.table("jobs").upsert(batch, on_conflict="url").execute()
            success_count += len(batch)
        except Exception as batch_error:
            # If batch fails, try inserting one by one to identify the problem job(s)

            print(f"    Batch {i//batch_size + 1} failed, trying individual inserts...")
            for job_record in batch:
                try:
                    supabase.table("jobs").upsert([job_record], on_conflict="url").execute()
                    success_count += 1
                except Exception as job_error:
                    failed_jobs.append({
                        "url": job_record.get("url"),
                        "title": job_record.get("title"),
                        "error": str(job_error)
                    })
                    print(str(job_error))
    
    return {
        "success_count": success_count,
        "failed_count": len(failed_jobs),
        "failed_jobs": failed_jobs,
        "duplicate_count": duplicate_count
    }


def get_all_users(supabase: Client) -> List[Dict[str, Any]]:
    """Fetch all users from the profiles table."""
    response = supabase.table("profiles").select("*").execute()
    return response.data


def clear_user_matched_jobs(supabase: Client, user_id: str) -> None:
    """Clear existing matched jobs for a user."""
    supabase.table("matched_jobs").delete().eq("user_id", user_id).execute()


def insert_matched_jobs(supabase: Client, user_id: str, matches: List[Dict[str, Any]]) -> None:
    """
    Insert matched jobs for a user.
    
    Args:
        supabase: Supabase client
        user_id: User's UUID
        matches: List of {"url": "...", "relevancy_score": N}
    """
    if not matches:
        return
    
    records = [
        {
            "user_id": user_id,
            "job_url": match["url"],
            "relevancy_score": match["relevancy_score"]
        }
        for match in matches
    ]
    
    # Insert in batches
    batch_size = 500
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        supabase.table("matched_jobs").insert(batch).execute()


def get_top_jobs_for_user(supabase: Client, user_id: str, limit: int = 2) -> List[Dict[str, Any]]:
    """
    Get top N matched jobs for a user with full job details.
    
    Returns:
        List of jobs with relevancy scores, sorted by relevancy DESC
    """
    response = supabase.table("matched_jobs") \
        .select("relevancy_score, jobs(*)") \
        .eq("user_id", user_id) \
        .order("relevancy_score", desc=True) \
        .limit(limit) \
        .execute()
    
    return response.data


# =============================================================================
# EMAIL SENDING
# =============================================================================

def load_email_template() -> str:
    """Load the HTML email template."""
    return '''<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
    <meta content="width=device-width, initial-scale=1" name="viewport">
    <meta charset="UTF-8">
    <title>New Job Matches</title>
    <style type="text/css">
        body { margin: 0; padding: 0; font-family: arial, 'helvetica neue', helvetica, sans-serif; }
    </style>
</head>
<body style="width:100%; font-family: arial, 'helvetica neue', helvetica, sans-serif; padding:0; margin:0;">
    <div style="background-color:#FFCD00; padding: 20px 0;">
        <table width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse;">
            <tr>
                <td align="center">
                    <!-- Header -->
                    <table width="600" cellspacing="0" cellpadding="0" style="border-collapse:collapse;">
                        <tr>
                            <td align="center" style="padding: 20px 40px;">
                                <h2 style="margin:0; color:#000;">Job Matches</h2>
                            </td>
                        </tr>
                    </table>
                    
                    <!-- Main Content -->
                    <table width="600" cellspacing="0" cellpadding="0" style="border-collapse:collapse; background-color:#ffffff; border-radius: 12px 12px 0 0;">
                        <tr>
                            <td align="center" style="padding: 20px 50px;">
                                <h1 style="margin:0; font-size:24px; font-weight:normal; color:#000000;">
                                    New jobs matching your profile!
                                </h1>
                            </td>
                        </tr>
                    </table>
                    
                    {{JOB_LISTINGS}}
                    
                    <!-- Footer -->
                    <table width="600" cellspacing="0" cellpadding="0" style="border-collapse:collapse; background-color:#ffffff; border-radius: 0 0 12px 12px;">
                        <tr>
                            <td align="center" style="padding: 20px;">
                                <p style="margin: 0; font-size: 14px; color: #666;">
                                    Relevancy scores are based on your skills matching the job description.
                                </p>
                            </td>
                        </tr>
                    </table>
                    
                    <table width="600" cellspacing="0" cellpadding="0" style="border-collapse:collapse; background-color:#FFCD00; margin-top: 20px;">
                        <tr>
                            <td align="center" style="padding: 20px;">
                                <p style="margin:0; font-size:13px; color:#4F4F42;">
                                    You're receiving this because you signed up for job alerts.
                                </p>
                            </td>
                        </tr>
                    </table>
                </td>
            </tr>
        </table>
    </div>
</body>
</html>'''


def generate_job_listing_html(job_data: Dict[str, Any], relevancy_score: int) -> str:
    """Generate HTML for a single job listing."""
    title = job_data.get("title", "Unknown Position")
    location = job_data.get("location", "Remote")
    category = job_data.get("category", "N/A")
    url = job_data.get("url", "#")
    
    return f'''
    <table width="600" cellspacing="0" cellpadding="0" style="border-collapse:collapse; background-color:#ffffff;">
        <tr>
            <td style="padding: 20px 40px;">
                <p style="margin:0; font-size:18px; color:#000000;">
                    <strong>{title}</strong>
                </p>
                <p style="margin:5px 0 0 0; font-size:14px; color:#666666;">
                    {category}
                </p>
                <p style="margin:10px 0; font-size:14px; color:#000000;">
                    📍 {location}
                </p>
                <p style="margin:10px 0; font-size:14px; color:#000000;">
                    <strong>Relevancy Score: {relevancy_score}</strong> skill matches
                </p>
                <p style="margin:15px 0 0 0;">
                    <a href="{url}" style="background:#FFCD00; color:#000000; padding:8px 16px; border-radius:20px; text-decoration:none; display:inline-block; font-size:14px;">
                        View Job →
                    </a>
                </p>
            </td>
        </tr>
        <tr>
            <td style="padding: 0 40px;">
                <hr style="border: none; border-bottom: 1px solid #EEEEEE; margin: 0;">
            </td>
        </tr>
    </table>
    '''


def render_email(top_jobs: List[Dict[str, Any]]) -> str:
    """Render the complete email with job listings."""
    template = load_email_template()
    
    job_listings_html = ""
    for match in top_jobs:
        relevancy = match.get("relevancy_score", 0)
        job_data = match.get("jobs", {})
        job_listings_html += generate_job_listing_html(job_data, relevancy)
    
    return template.replace("{{JOB_LISTINGS}}", job_listings_html)


def send_email(to_email: str, subject: str, html_content: str) -> bool:
    """Send an HTML email via Gmail SMTP."""
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
        server.sendmail(GMAIL_ADDRESS, to_email, msg.as_string())
        server.quit()
        
        print(f"    ✓ Email sent to {to_email}")
        return True
        
    except Exception as e:
        print(f"    ✗ Failed to send email to {to_email}: {e}")
        return False


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def process_user(
    supabase: Client, 
    user: Dict[str, Any], 
    all_jobs: Dict[str, Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Process a single user: match jobs, calculate relevancy, save to database.
    
    Args:
        supabase: Supabase client
        user: User profile data
        all_jobs: Dict of all jobs keyed by URL
    
    Returns:
        Result dict with user info and match count
    """
    user_id = user.get("id")
    user_email = user.get("email")
    user_industries = user.get("industries") or []
    user_skills = user.get("skills") or []
    
    print(f"\n  Processing: {user_email or user_id}")
    print(f"    Industries: {user_industries}")
    print(f"    Skills: {user_skills[:5]}{'...' if len(user_skills) > 5 else ''}")
    
    # Step 1: Find jobs matching user's industries
    matching_jobs = []
    for url, job in all_jobs.items():
        job_category = job.get("category", "N/A")
        if job_category and job_category != "N/A":
            if job_category in user_industries:
                matching_jobs.append(job)
    
    print(f"    Jobs matching industries: {len(matching_jobs)}")
    
    # Step 2: Calculate relevancy scores
    matches = []
    for job in matching_jobs:
        description = job.get("description", "")
        relevancy_score = calculate_relevancy_score(user_skills, description)
        
        matches.append({
            "url": job.get("url"),
            "relevancy_score": relevancy_score
        })
    
    # Sort by relevancy score (highest first)
    matches.sort(key=lambda x: x["relevancy_score"], reverse=True)
    
    # Step 3: Save to database
    if matches:
        print(f"    Saving {len(matches)} matches to database...")
        try:
            clear_user_matched_jobs(supabase, user_id)
            insert_matched_jobs(supabase, user_id, matches)
            print(f"    ✓ Saved to matched_jobs table")
        except Exception as e:
            print(f"    ✗ Database error: {e}")
    
    return {
        "user_id": user_id,
        "email": user_email,
        "matched_count": len(matches),
        "top_relevancy": matches[0]["relevancy_score"] if matches else 0
    }


def main():
    """Main execution function."""
    print("=" * 60)
    print("JOB MATCHING AND EMAIL NOTIFICATION SYSTEM")
    print("=" * 60)
    
    # Initialize Supabase client
    print("\n[1/6] Connecting to Supabase...")
    supabase = get_supabase_client()
    print("  ✓ Connected")
    
    # Load jobs from file
    print(f"\n[2/6] Loading jobs from {JOBS_FILE_PATH}...")
    try:
        jobs_list = load_jobs_from_file(JOBS_FILE_PATH)
        print(f"  ✓ Loaded {len(jobs_list)} jobs from file")
    except FileNotFoundError:
        print(f"  ✗ File not found: {JOBS_FILE_PATH}")
        return
    except json.JSONDecodeError as e:
        print(f"  ✗ Invalid JSON: {e}")
        return
    
    # Store jobs in database
    print("\n[3/6] Storing jobs in database...")
    try:
        result = upsert_jobs_to_database(supabase, jobs_list)
        print(f"  ✓ Successfully upserted {result['success_count']} jobs")
        
        if result['failed_count'] > 0:
            print(f"  ✗ Failed to store {result['failed_count']} jobs:")
            for failed in result['failed_jobs']:
                print(f"    - Title: {failed['title']}")
                print(f"      URL: {failed['url']}")
                print(f"      Error: {failed['error']}")
                print()
    except Exception as e:
        print(f"  ✗ Failed to store jobs: {e}")
        print("  Make sure you've run setup_tables.sql first!")
        return
    
    # Create a dict for quick lookup
    all_jobs = {job.get("url"): job for job in jobs_list}
    
    # Get all users
    print("\n[4/6] Fetching users from Supabase...")
    users = get_all_users(supabase)
    print(f"  ✓ Found {len(users)} users")
    
    if not users:
        print("  No users found. Exiting.")
        return
    
    # Process each user
    print("\n[5/6] Matching jobs to users...")
    results = []
    for user in users:
        result = process_user(supabase, user, all_jobs)
        results.append(result)
    
    # Send emails
    print("\n[6/6] Sending email notifications...")
    emails_sent = 0
    emails_failed = 0
    
    for result in results:
        user_id = result.get("user_id")
        email = result.get("email")
        matched_count = result.get("matched_count", 0)
        
        if not email:
            print(f"  Skipping user {user_id} - no email address")
            continue
        
        if matched_count == 0:
            print(f"  Skipping {email} - no matching jobs")
            continue
        
        # Get top 2 jobs with full details
        top_jobs = get_top_jobs_for_user(supabase, user_id, limit=2)
        
        if not top_jobs:
            print(f"  Skipping {email} - could not fetch top jobs")
            continue
        
        # Render and send email
        html_content = render_email(top_jobs)
        subject = f"🎯 {len(top_jobs)} New Job Match{'es' if len(top_jobs) > 1 else ''} For You!"
        
        if send_email(email, subject, html_content):
            emails_sent += 1
        else:
            emails_failed += 1
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Users processed: {len(users)}")
    print(f"Jobs in database: {len(jobs_list)}")
    print(f"Emails sent: {emails_sent}")
    print(f"Emails failed: {emails_failed}")
    print("=" * 60)


if __name__ == "__main__":
    main()