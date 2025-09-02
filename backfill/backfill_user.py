#!/usr/bin/env python3
import json
import time
import requests
from pathlib import Path
import sys
import os
import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add backfill folder to path for auth
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../backfill")))

from dotenv import load_dotenv
from auth import get_authenticated_session

load_dotenv()

# ------------------------
# Config
# ------------------------
BASE_URL = "http://localhost:80/api"
USERS_FILE = Path("../generated_json/users.json")
CREATE_USER_URL = "http://localhost:80/auth/sign-up/"

# Profile options
ROLES = [
    "Individual contributor",
    "Senior Leader", 
    "Manager",
    "Executive",
    "Freelancer",
    "Student"
]

USE_CASES = [
    "Engineering",
    "Product", 
    "Marketing",
    "Sales",
    "Operations",
    "Legal",
    "Finance",
    "Human Resources",
    "Project",
    "Other"
]

# Threading configuration for profile setup
MAX_WORKERS = 6  # Number of concurrent threads
API_RATE_LIMIT = 5  # requests per second
BATCH_SIZE = 20  # Process users in batches for progress reporting

# Thread-safe variables
api_lock = threading.Lock()
last_api_call = {"time": 0}
results_lock = threading.Lock()
success_count = {"value": 0}
error_count = {"value": 0}

# ------------------------
# User Creation Functions
# ------------------------
def create_users():
    """Create all users first before setting up profiles"""
    print("🚀 Starting user creation phase...")
    
    if not USERS_FILE.exists():
        raise SystemExit(f"❌ File not found: {USERS_FILE}")
    
    session = get_authenticated_session()

    with open(USERS_FILE, "r") as f:
        users = json.load(f)

    print(f"👥 Found {len(users)} users to create.")
    
    created_count = 0
    existing_count = 0
    failed_count = 0
    
    for i, user in enumerate(users, 1):
        # Send as form data, not JSON
        user_data = {
            "email": user["email"],
            "password": user["password"],
            "csrfmiddlewaretoken": session.cookies.get('csrftoken', '')  # Get from session cookies
        }
        
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": "http://localhost:3000/",  # Match the browser request
            "Origin": "http://localhost:3000"
        }
        
        response = session.post(CREATE_USER_URL, data=user_data, headers=headers, allow_redirects=False)

        if response.status_code == 302:  # Success is 302, not 200!
            created_count += 1
            print(f"  ✅ [{i}/{len(users)}] Created user: {user['email']}")
        elif response.status_code == 409:
            existing_count += 1
            print(f"  ⚠️ [{i}/{len(users)}] User already exists: {user['email']}")
        else:
            failed_count += 1
            print(f"  ❌ [{i}/{len(users)}] Failed to create user: {user['email']}")
            print(f"      Status: {response.status_code}, Response: {response.text[:100]}")

    print(f"\n📊 User creation complete:")
    print(f"   - Created: {created_count}")
    print(f"   - Already existed: {existing_count}")
    print(f"   - Failed: {failed_count}")
    print(f"   - Total processed: {created_count + existing_count + failed_count}")
    
    return created_count + existing_count > 0  # Return True if we have users to setup profiles for

# ------------------------
# Profile Setup Functions (from original script)
# ------------------------
def rate_limit_api():
    """Ensure we don't exceed API rate limits"""
    with api_lock:
        now = time.time()
        time_since_last = now - last_api_call["time"]
        min_interval = 1.0 / API_RATE_LIMIT
        
        if time_since_last < min_interval:
            sleep_time = min_interval - time_since_last
            time.sleep(sleep_time)
        
        last_api_call["time"] = time.time()

def update_counters(success=False, error=False):
    """Thread-safe counter updates"""
    with results_lock:
        if success:
            success_count["value"] += 1
        if error:
            error_count["value"] += 1

def get_counts():
    """Thread-safe counter reads"""
    with results_lock:
        return success_count["value"], error_count["value"]

def get_user_authenticated_session(email, password):
    """Get authenticated session for a specific user"""
    session = requests.Session()
    
    # Step 1: Get CSRF token
    csrf_resp = session.get("http://localhost:80/auth/get-csrf-token/")
    if csrf_resp.status_code != 200:
        raise Exception(f"Failed to get CSRF token for {email}: {csrf_resp.status_code}")
    
    csrf_token = csrf_resp.json().get("csrf_token")
    if not csrf_token:
        raise Exception(f"CSRF token not found for {email}")

    # Step 2: Login
    login_payload = {
        "csrfmiddlewaretoken": csrf_token,
        "email": email,
        "password": password,
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": "http://localhost:80/auth/sign-in/",
    }

    login_resp = session.post(
        "http://localhost:80/auth/sign-in/", 
        data=login_payload, 
        headers=headers, 
        allow_redirects=False
    )

    if login_resp.status_code != 302:
        raise Exception(f"Login failed for {email}: {login_resp.status_code}")

    return session

def generate_realistic_profile(user_email):
    """Generate realistic role and use_case based on user email patterns"""
    email_lower = user_email.lower()
    
    # Try to infer role from email patterns
    if any(keyword in email_lower for keyword in ['ceo', 'founder', 'president', 'chief']):
        role = "Executive"
    elif any(keyword in email_lower for keyword in ['manager', 'lead', 'head', 'director']):
        role = random.choice(["Manager", "Senior Leader"])
    elif any(keyword in email_lower for keyword in ['senior', 'sr', 'principal']):
        role = "Senior Leader"
    elif any(keyword in email_lower for keyword in ['intern', 'junior', 'jr']):
        role = "Individual contributor"
    elif any(keyword in email_lower for keyword in ['student', 'edu']):
        role = "Student"
    elif any(keyword in email_lower for keyword in ['freelance', 'contractor']):
        role = "Freelancer"
    else:
        # Random assignment with realistic distribution
        role = random.choices(
            ROLES,
            weights=[40, 20, 15, 10, 10, 5],  # More individual contributors, fewer executives
            k=1
        )[0]
    
    # Try to infer use_case from email patterns
    if any(keyword in email_lower for keyword in ['dev', 'engineer', 'tech', 'code']):
        use_case = "Engineering"
    elif any(keyword in email_lower for keyword in ['product', 'pm']):
        use_case = "Product"
    elif any(keyword in email_lower for keyword in ['marketing', 'growth', 'content']):
        use_case = "Marketing"
    elif any(keyword in email_lower for keyword in ['sales', 'revenue', 'business']):
        use_case = "Sales"
    elif any(keyword in email_lower for keyword in ['hr', 'people', 'talent']):
        use_case = "Human Resources"
    elif any(keyword in email_lower for keyword in ['finance', 'accounting', 'money']):
        use_case = "Finance"
    elif any(keyword in email_lower for keyword in ['legal', 'law', 'compliance']):
        use_case = "Legal"
    elif any(keyword in email_lower for keyword in ['ops', 'operations']):
        use_case = "Operations"
    elif any(keyword in email_lower for keyword in ['project', 'scrum', 'agile']):
        use_case = "Project"
    else:
        # Random assignment with realistic distribution
        use_case = random.choices(
            USE_CASES,
            weights=[25, 15, 10, 8, 8, 5, 5, 5, 10, 9],  # More engineering/product, fewer others
            k=1
        )[0]
    
    return role, use_case

def setup_user_profile(user_data: tuple) -> tuple:
    """Setup profile for a single user (runs in thread)"""
    user, index, total = user_data
    user_email = user.get("email", "Unknown")
    user_password = user.get("password", "")
    first_name = user.get("first_name", "")
    last_name = user.get("last_name", "")
    
    if not user_email or not user_password:
        error_msg = "Missing email or password"
        update_counters(error=True)
        return (False, index, user_email, error_msg)
    
    if not first_name or not last_name:
        error_msg = "Missing first_name or last_name in user data"
        update_counters(error=True)
        return (False, index, user_email, error_msg)
    
    try:
        # Login as this user
        rate_limit_api()
        session = get_user_authenticated_session(user_email, user_password)
        
        # Generate realistic profile
        role, use_case = generate_realistic_profile(user_email)
        
        # Step 1: Set first and last name (use data from JSON)
        name_payload = {
            "first_name": first_name,
            "last_name": last_name
        }
        
        headers = {
            "Content-Type": "application/json",
            "X-CSRFToken": session.cookies.get("csrftoken", ""),
            "Referer": "http://localhost:80/"
        }
        
        rate_limit_api()
        name_resp = session.patch(
            f"{BASE_URL}/users/me/", 
            json=name_payload, 
            headers=headers
        )
        
        if name_resp.status_code != 200:
            error_msg = f"Failed to set name - HTTP {name_resp.status_code}: {name_resp.text[:100]}"
            update_counters(error=True)
            return (False, index, user_email, error_msg)
        
        # Step 2: Setup profile (role and use_case)
        profile_payload = {
            "role": role,
            "use_case": use_case
        }
        
        rate_limit_api()
        profile_resp = session.patch(
            f"{BASE_URL}/users/me/profile/", 
            json=profile_payload, 
            headers=headers
        )
        
        if profile_resp.status_code == 200:
            update_counters(success=True)
            return (True, index, user_email, f"Name: {first_name} {last_name}, Role: {role}, Use case: {use_case}")
        else:
            error_msg = f"Failed to set profile - HTTP {profile_resp.status_code}: {profile_resp.text[:100]}"
            update_counters(error=True)
            return (False, index, user_email, error_msg)
            
    except Exception as e:
        error_msg = f"Exception: {str(e)[:100]}"
        update_counters(error=True)
        return (False, index, user_email, error_msg)

def process_users_batch(users_batch: list, batch_num: int, total_batches: int):
    """Process a batch of users with multithreading"""
    print(f"\n🔄 Processing batch {batch_num}/{total_batches} ({len(users_batch)} users)")
    
    # Prepare user data with indices
    user_data = [(user, i + ((batch_num - 1) * BATCH_SIZE), len(users_batch)) 
                 for i, user in enumerate(users_batch, 1)]
    
    batch_success = 0
    batch_errors = 0
    
    # Process batch with threading
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all user tasks
        future_to_user = {
            executor.submit(setup_user_profile, data): data[0]["email"] 
            for data in user_data
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_user):
            user_email = future_to_user[future]
            try:
                success, index, email, message = future.result()
                
                if success:
                    batch_success += 1
                    print(f"  ✅ [{index}] Setup complete for {email}: {message}")
                else:
                    batch_errors += 1
                    print(f"  ❌ [{index}] Failed for {email}: {message}")
                    
            except Exception as e:
                batch_errors += 1
                print(f"  ❌ Error processing user {user_email}: {e}")
    
    # Batch summary
    total_success, total_errors = get_counts()
    print(f"📊 Batch {batch_num} complete: {batch_success} success, {batch_errors} errors")
    print(f"📈 Overall progress: {total_success} success, {total_errors} errors, {total_success + total_errors} total")

def validate_users(users):
    """Validate users have required fields"""
    valid_users = []
    invalid_count = 0
    
    for i, user in enumerate(users):
        if not isinstance(user, dict):
            print(f"⚠️ User {i+1} is not a dictionary, skipping")
            invalid_count += 1
            continue
            
        required_fields = ["email", "password", "first_name", "last_name"]
        missing_fields = [field for field in required_fields if not user.get(field)]
        
        if missing_fields:
            print(f"⚠️ User {i+1} missing required fields: {missing_fields}, skipping")
            invalid_count += 1
            continue
            
        valid_users.append(user)
    
    if invalid_count > 0:
        print(f"⚠️ Skipped {invalid_count} invalid users")
    
    return valid_users

def setup_profiles():
    """Setup profiles for all users"""
    print("\n🔧 Starting user profile and name setup phase...")
    
    # Reset counters for profile setup phase
    with results_lock:
        success_count["value"] = 0
        error_count["value"] = 0
    
    # Load users again for profile setup
    with USERS_FILE.open("r", encoding="utf-8") as f:
        users = json.load(f)

    print(f"👥 Found {len(users)} users to setup profiles and names for.")
    
    if not users:
        print("ℹ️ No users to process.")
        return

    # Validate users
    valid_users = validate_users(users)
    if not valid_users:
        print("❌ No valid users found. Cannot proceed.")
        return
    
    if len(valid_users) != len(users):
        print(f"📊 Processing {len(valid_users)} valid users out of {len(users)} total")

    # Show configuration
    print(f"\n⚙️ Configuration:")
    print(f"   - Max workers: {MAX_WORKERS}")
    print(f"   - API rate limit: {API_RATE_LIMIT} req/sec")
    print(f"   - Batch size: {BATCH_SIZE}")
    print(f"   - Total users: {len(valid_users)}")
    print(f"   - Available roles: {', '.join(ROLES)}")
    print(f"   - Available use cases: {', '.join(USE_CASES)}")

    # Process in batches
    start_time = time.time()
    total_batches = (len(valid_users) + BATCH_SIZE - 1) // BATCH_SIZE
    
    for batch_num in range(1, total_batches + 1):
        start_idx = (batch_num - 1) * BATCH_SIZE
        end_idx = min(start_idx + BATCH_SIZE, len(valid_users))
        batch_users = valid_users[start_idx:end_idx]
        
        process_users_batch(batch_users, batch_num, total_batches)
        
        # Small delay between batches to be respectful
        if batch_num < total_batches:
            time.sleep(1)

    # Final summary
    elapsed_time = time.time() - start_time
    final_success, final_errors = get_counts()
    total_processed = final_success + final_errors
    
    print(f"\n🎉 User profile and name setup complete!")
    print(f"📊 Final Statistics:")
    print(f"   - Total processed: {total_processed}/{len(valid_users)}")
    print(f"   - Successful: {final_success} ({final_success/total_processed*100:.1f}%)" if total_processed > 0 else "")
    print(f"   - Failed: {final_errors} ({final_errors/total_processed*100:.1f}%)" if total_processed > 0 else "")
    print(f"   - Time elapsed: {elapsed_time:.1f} seconds")
    print(f"   - Average rate: {total_processed/elapsed_time:.1f} users/second" if elapsed_time > 0 else "")
    
    if final_errors > 0:
        print(f"\n⚠️ {final_errors} user profiles/names failed to setup. Check the error messages above.")
    else:
        print(f"\n✅ All user profiles and names setup successfully!")

# ------------------------
# Main
# ------------------------
def main():
    print("🚀 Starting combined user creation and profile setup...")
    start_time = time.time()
    
    # Phase 1: Create users
    users_available = create_users()
    
    if not users_available:
        print("❌ No users were created or available. Stopping.")
        return
    
    # Small delay between phases
    print("\n⏳ Waiting 3 seconds before starting profile setup...")
    time.sleep(3)
    
    # Phase 2: Setup profiles
    setup_profiles()
    
    # Overall summary
    total_elapsed = time.time() - start_time
    print(f"\n🎊 Complete! Total time elapsed: {total_elapsed:.1f} seconds")
    print("✅ Both user creation and profile setup phases completed successfully!")

if __name__ == "__main__":
    main()