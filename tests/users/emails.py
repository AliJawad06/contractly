import os
from supabase import create_client, Client
from typing import Optional, Dict, List
from datetime import datetime, timedelta


# ==================== FEATURE CONFIGURATION ====================
# Define tier types
TIER_FREE = 'free'
TIER_PRO = 'pro'

# Free tier limits
FREE_TIER_LIMITS = {
    'MAX_ALERTS_PER_DAY': 20,
    'JOB_UPDATE_FREQUENCY': 'bi-weekly',  # bi-weekly
    'INCLUDE_SALARY': False,
    'INCLUDE_YOE': False,
    'INCLUDE_RECRUITER': False,
    'TRACK_APPLICATIONS': False,
}

# Pro tier limits
PRO_TIER_LIMITS = {
    'MAX_ALERTS_PER_DAY': float('inf'),  # unlimited
    'JOB_UPDATE_FREQUENCY': 'daily',
    'INCLUDE_SALARY': True,
    'INCLUDE_YOE': True,
    'INCLUDE_RECRUITER': True,
    'TRACK_APPLICATIONS': True,
}


# ==================== SUBSCRIPTION UTILITIES ====================
def get_user_subscription(user_email: str) -> Optional[Dict]:
    """
    Get user's subscription from the subscriptions table.
    Similar to the Paddle integration pattern.
    """
    try:
        # First get customer_id from customers table
        customer_response = supabase.table("customers").select("customer_id").eq("email", user_email).execute()

        if not customer_response.data or len(customer_response.data) == 0:
            return None

        customer_id = customer_response.data[0].get("customer_id")

        # Then get subscription details
        subscription_response = supabase.table("subscriptions").select("*").eq("customer_id", customer_id).execute()

        if not subscription_response.data or len(subscription_response.data) == 0:
            return None

        return subscription_response.data[0]
    except Exception as e:
        print(f"Error fetching subscription for {user_email}: {e}")
        return None


def get_user_tier_from_subscription(subscription: Optional[Dict]) -> str:
    """
    Determine user tier based on subscription status.
    Returns 'free' or 'pro'.
    """
    if not subscription:
        return TIER_FREE

    # Active subscription statuses that grant pro access
    active_statuses = ['active', 'trialing']
    subscription_status = subscription.get('subscription_status', '')

    if subscription_status in active_statuses:
        return TIER_PRO

    return TIER_FREE


def get_tier_limits(user_tier: str) -> Dict:
    """
    Get feature limits based on user tier.
    """
    if user_tier == TIER_PRO:
        return PRO_TIER_LIMITS
    return FREE_TIER_LIMITS


def has_feature_access(user_tier: str, feature_key: str) -> bool:
    """
    Check if user has access to a specific feature.
    """
    limits = get_tier_limits(user_tier)
    return limits.get(feature_key, False)


# ==================== USER DATA FUNCTIONS ====================
def get_all_auth_users():
    all_users = []
    page = 1
    per_page = 100  # max per request

    while True:
        response = supabase.auth.admin.list_users(page=page, per_page=per_page)
        users = response
        if not users:
            break
        all_users.extend(users)
        page += 1

    return all_users


def get_all_profiles():
    response = supabase.table("profiles").select("*").execute()
    # v2 client returns dict-like object with .data
    profiles = response.data
    return profiles


def combine_users_and_profiles(users, profiles):
    """
    Combine users, profiles, and subscription data with feature gating.
    """
    combined = []
    for user in users:
        profile = next((p for p in profiles if p.get("id") == user.id), None)

        # Get subscription and tier
        subscription = get_user_subscription(user.email)
        user_tier = get_user_tier_from_subscription(subscription)
        tier_limits = get_tier_limits(user_tier)

        combined.append({
            "id": user.id,
            "email": user.email,
            "profile": profile,
            "subscription": subscription,
            "tier": user_tier,
            "limits": tier_limits
        })
    return combined

# ==================== EMAIL ALERT FUNCTIONS ====================
def can_receive_alert(user_email: str, user_tier: str) -> bool:
    """
    Check if user can receive an alert based on their daily limit.
    You would typically track this in a database table.
    """
    tier_limits = get_tier_limits(user_tier)
    max_alerts = tier_limits['MAX_ALERTS_PER_DAY']

    if max_alerts == float('inf'):
        return True

    # TODO: Implement actual daily alert count tracking
    # Example: Query a table that tracks alerts sent per user per day
    # For now, we'll just return True as a placeholder
    # In production, you'd do something like:
    # today = datetime.now().date()
    # count = supabase.table("user_alerts").select("count").eq("user_email", user_email).eq("date", today).execute()
    # return count.data[0]['count'] < max_alerts if count.data else True

    return True


def get_jobs_for_alert(user_tier: str, limit: int = 10) -> List[Dict]:
    """
    Fetch jobs with columns based on user tier.
    Pro users get salary, yoe, and recruiter columns.
    Free users get basic job info only.
    """
    tier_limits = get_tier_limits(user_tier)

    # Build select query based on tier
    base_columns = ["id", "title", "company", "location", "description", "posted_date", "url"]

    if tier_limits['INCLUDE_SALARY']:
        base_columns.append("salary")

    if tier_limits['INCLUDE_YOE']:
        base_columns.append("years_of_experience")

    if tier_limits['INCLUDE_RECRUITER']:
        base_columns.append("recruiter_name")
        base_columns.append("recruiter_email")

    # Construct the select string
    select_string = ", ".join(base_columns)

    try:
        # Fetch jobs from database
        response = supabase.table("jobs").select(select_string).limit(limit).execute()
        return response.data if response.data else []
    except Exception as e:
        print(f"Error fetching jobs: {e}")
        return []


def should_send_jobs_update(user_tier: str, last_update_date: Optional[datetime]) -> bool:
    """
    Determine if jobs update should be sent based on update frequency.
    Free: bi-weekly
    Pro: daily
    """
    tier_limits = get_tier_limits(user_tier)
    frequency = tier_limits['JOB_UPDATE_FREQUENCY']

    if not last_update_date:
        return True

    now = datetime.now()
    days_since_update = (now - last_update_date).days

    if frequency == 'daily':
        return days_since_update >= 1
    elif frequency == 'bi-weekly':
        return days_since_update >= 14

    return False


def build_email_content(user_data: Dict, jobs: List[Dict]) -> str:
    """
    Build email content with feature-gated information.
    """
    user_tier = user_data['tier']
    tier_limits = user_data['limits']
    email = user_data['email']

    # Email header
    content = f"Hello {email},\n\n"
    content += f"Here are your latest job opportunities ({user_tier.upper()} tier):\n\n"

    # Job listings
    for idx, job in enumerate(jobs, 1):
        content += f"{idx}. {job.get('title', 'N/A')} at {job.get('company', 'N/A')}\n"
        content += f"   Location: {job.get('location', 'N/A')}\n"

        # Pro features
        if tier_limits['INCLUDE_SALARY'] and 'salary' in job:
            content += f"   Salary: {job.get('salary', 'Not disclosed')}\n"

        if tier_limits['INCLUDE_YOE'] and 'years_of_experience' in job:
            content += f"   Experience: {job.get('years_of_experience', 'N/A')} years\n"

        if tier_limits['INCLUDE_RECRUITER'] and 'recruiter_name' in job:
            content += f"   Recruiter: {job.get('recruiter_name', 'N/A')}"
            if 'recruiter_email' in job:
                content += f" ({job.get('recruiter_email', '')})"
            content += "\n"

        content += f"   URL: {job.get('url', 'N/A')}\n\n"

    # Footer with upgrade CTA for free users
    if user_tier == TIER_FREE:
        content += "\n---\n"
        content += "Upgrade to PRO to unlock:\n"
        content += "- Unlimited daily alerts\n"
        content += "- Daily job updates\n"
        content += "- Salary information\n"
        content += "- Years of experience requirements\n"
        content += "- Recruiter contact information\n"
        content += "- Application tracking\n"

    return content


def send_job_alerts():
    """
    Main function to send job alerts with subscription-based feature gating.
    """
    print("Starting job alert process with subscription feature gating...\n")

    # Get all users with their subscription data
    users = get_all_auth_users()
    profiles = get_all_profiles()
    combined_users = combine_users_and_profiles(users, profiles)

    for user_data in combined_users:
        email = user_data['email']
        user_tier = user_data['tier']
        tier_limits = user_data['limits']

        print(f"\nProcessing user: {email} (Tier: {user_tier.upper()})")

        # Check if user can receive alerts
        if not can_receive_alert(email, user_tier):
            print(f"  ⚠️  Daily alert limit reached for {email}")
            continue

        # Check if it's time for jobs update based on frequency
        # In production, fetch last_update_date from database
        last_update_date = None  # Placeholder
        if not should_send_jobs_update(user_tier, last_update_date):
            print(f"  ⏭️  Skipping - not time for update based on {tier_limits['JOB_UPDATE_FREQUENCY']} frequency")
            continue

        # Fetch jobs with appropriate columns based on tier
        jobs = get_jobs_for_alert(user_tier, limit=5)

        if not jobs:
            print(f"  📭 No jobs found for {email}")
            continue

        # Build email content
        email_content = build_email_content(user_data, jobs)

        # In production, send the actual email here
        print(f"  ✉️  Email preview for {email}:")
        print("  " + "-" * 50)
        print("  " + email_content.replace("\n", "\n  "))
        print("  " + "-" * 50)

        # TODO: Send actual email using your email service
        # send_email(to=email, subject="Your Job Alerts", body=email_content)

        # TODO: Track that alert was sent
        # supabase.table("user_alerts").insert({"user_email": email, "date": datetime.now().date(), ...}).execute()


# --- MAIN ---
def main():
    print("Fetching all users and profiles from Supabase...")

    users = get_all_auth_users()
    profiles = get_all_profiles()

    combined = combine_users_and_profiles(users, profiles)

    print(f"\nFetched {len(users)} users and {len(profiles)} profiles.\n")

    # Display user info with subscription details
    for entry in combined:
        print(f"User: {entry['email']}")
        print(f"  Tier: {entry['tier'].upper()}")
        print(f"  Limits: {entry['limits']}")
        print()

    # Send job alerts
    print("\n" + "=" * 60)
    send_job_alerts()

if __name__ == "__main__":
    url: str = "https://cvfqdvugrnuvdmzpodks.supabase.co"
    key: str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImN2ZnFkdnVncm51dmRtenBvZGtzIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2Mjc0OTMyOCwiZXhwIjoyMDc4MzI1MzI4fQ.dIb8GKOlFwUGV1SEDHYF9ExZp7S-7PC53OKEscHiNuA"
    supabase: Client = create_client(url, key)
    main()