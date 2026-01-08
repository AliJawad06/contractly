import os
from supabase import create_client, Client



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

# --- GET ALL PROFILES ---
def get_all_profiles():
    response = supabase.table("profiles").select("*").execute()
    # v2 client returns dict-like object with .data
    profiles = response.data
    return profiles

# --- COMBINE USERS + PROFILES ---
def combine_users_and_profiles(users, profiles):
    combined = []
    for user in users:
        profile = next((p for p in profiles if p.get("id") == user.id), None)
        combined.append({
            "id": user.id,
            "email": user.email,
            "profile": profile
        })
    return combined

# --- MAIN ---
def main():
    print("Fetching all users and profiles from Supabase...")

    users = get_all_auth_users()
    profiles = get_all_profiles()

    combined = combine_users_and_profiles(users, profiles)

    print(f"\nFetched {len(users)} users and {len(profiles)} profiles.\n")

    for entry in combined:
        print(entry)

if __name__ == "__main__":
    url: str = "https://cvfqdvugrnuvdmzpodks.supabase.co"
    key: str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImN2ZnFkdnVncm51dmRtenBvZGtzIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2Mjc0OTMyOCwiZXhwIjoyMDc4MzI1MzI4fQ.dIb8GKOlFwUGV1SEDHYF9ExZp7S-7PC53OKEscHiNuA"
    supabase: Client = create_client(url, key)
    main()