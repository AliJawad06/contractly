import os
from supabase import create_client, Client
from typing import Optional, Dict, List
from datetime import datetime, timedelta

#Get all users
# For each user, return all of the jobs with the same industry, and add a relevance column to each job for that user. Relevency is determined by user.profile.skills. For Each skill that is in the job.description, the relevancy score goes up by one
# write all of the jobs to the users supabase profile, with the added relevancy column
# choose the two jobs with the highest salaries and put them in the email template
# all jobs are in /Users/aj/Downloads/DhikrUp/contractJobs/day/day.json

# --- MAIN ---

url: str = "https://cvfqdvugrnuvdmzpodks.supabase.co"
key: str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImN2ZnFkdnVncm51dmRtenBvZGtzIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2Mjc0OTMyOCwiZXhwIjoyMDc4MzI1MzI4fQ.dIb8GKOlFwUGV1SEDHYF9ExZp7S-7PC53OKEscHiNuA"
supabase: Client = create_client(url, key)
