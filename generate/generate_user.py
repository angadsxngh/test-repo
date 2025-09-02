import json
import requests
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import anthropic

load_dotenv()

client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

CSRF_URL = "http://localhost:8000/auth/get-csrf-token/"
OUTPUT_FILE = "../generated_json/users.json"
NUM_USERS = 50

# Threading configuration
MAX_WORKERS = 6  # Number of concurrent threads
CLAUDE_RATE_LIMIT = 3  # requests per second for Claude API
API_RATE_LIMIT = 10  # requests per second for CSRF API

# Thread-safe locks and variables
claude_lock = threading.Lock()
api_lock = threading.Lock()
last_claude_call = {"time": 0}
last_api_call = {"time": 0}
results_lock = threading.Lock()
all_users = []

# Thread-safe sets to track uniqueness
used_emails = set()
used_full_names = set()
uniqueness_lock = threading.Lock()

# Name variation categories for diversity
NAME_CATEGORIES = [
    "American", "British", "Hispanic", "Asian", "European", "African", 
    "Middle Eastern", "Scandinavian", "Irish", "Italian"
]

PROFESSION_HINTS = [
    "tech professional", "business executive", "creative professional", 
    "healthcare worker", "educator", "consultant", "analyst", 
    "marketing specialist", "sales representative", "project manager"
]

def is_unique_user(email, first_name, last_name):
    """Thread-safe check if user is unique"""
    with uniqueness_lock:
        full_name = f"{first_name.lower()} {last_name.lower()}"
        email_lower = email.lower()
        
        if email_lower in used_emails or full_name in used_full_names:
            return False
        
        # Add to used sets
        used_emails.add(email_lower)
        used_full_names.add(full_name)
        return True

def is_unique_user(email, first_name, last_name):
    """Thread-safe check if user is unique"""
    with uniqueness_lock:
        full_name = f"{first_name.lower()} {last_name.lower()}"
        email_lower = email.lower()
        
        if email_lower in used_emails or full_name in used_full_names:
            return False
        
        # Add to used sets
        used_emails.add(email_lower)
        used_full_names.add(full_name)
        return True

def get_used_names_sample():
    """Get a sample of used names to help Claude avoid duplicates"""
    with uniqueness_lock:
        # Return a sample of used names for context
        sample_names = list(used_full_names)[-20:]  # Last 20 names
        sample_emails = list(used_emails)[-20:]     # Last 20 emails
        return sample_names, sample_emails

def rate_limit_claude():
    """Get a sample of used names to help Claude avoid duplicates"""
    with uniqueness_lock:
        # Return a sample of used names for context
        sample_names = list(used_full_names)[-20:]  # Last 20 names
        sample_emails = list(used_emails)[-20:]     # Last 20 emails
        return sample_names, sample_emails
    """Ensure we don't exceed Claude API rate limits"""
    with claude_lock:
        now = time.time()
        time_since_last = now - last_claude_call["time"]
        min_interval = 1.0 / CLAUDE_RATE_LIMIT
        
        if time_since_last < min_interval:
            sleep_time = min_interval - time_since_last
            time.sleep(sleep_time)
        
        last_claude_call["time"] = time.time()

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

def get_csrf_token():
    """Get CSRF token with rate limiting"""
    rate_limit_api()
    try:
        response = requests.get(CSRF_URL, timeout=10)
        if response.status_code == 200:
            return response.json().get("csrf_token")
        else:
            print(f"❌ Failed to fetch CSRF token: {response.status_code}")
            return None
    except Exception as e:
        print(f"❌ Error fetching CSRF token: {e}")
        return None

def generate_user_batch(batch_info):
    """Generate a batch of users with variation (runs in thread)"""
    batch_num, batch_size, start_idx = batch_info
    users = []
    
    print(f"  🔄 Thread {batch_num}: Generating {batch_size} users...")
    
    for i in range(batch_size):
        user_num = start_idx + i + 1
        max_attempts = 5  # Maximum attempts to generate a unique user
        
        for attempt in range(max_attempts):
            # Get sample of used names to avoid duplicates
            used_names, used_emails_list = get_used_names_sample()
            
            # Add variation by selecting different categories
            import random
            name_category = random.choice(NAME_CATEGORIES)
            profession = random.choice(PROFESSION_HINTS)
            
            # Generate domains variety
            domains = [
                "gmail.com", "outlook.com", "yahoo.com", "hotmail.com", 
                "protonmail.com", "icloud.com", "live.com", "aol.com"
            ]
            preferred_domain = random.choice(domains)
            
            # Create context about used names/emails to avoid duplicates
            avoid_context = ""
            if used_names or used_emails_list:
                avoid_context = f"""
IMPORTANT: Avoid creating duplicates. 
Recently used names: {', '.join(used_names[-10:]) if used_names else 'none'}
Recently used emails: {', '.join(used_emails_list[-10:]) if used_emails_list else 'none'}
Generate a completely different and unique user.
"""

            prompt = f"""Generate a realistic user with {name_category} background who works as a {profession}.

{avoid_context}

Requirements:
- Create authentic first_name and last_name that fit the {name_category} naming pattern
- Email should be based on the name using {preferred_domain} (use patterns like firstname.lastname@{preferred_domain} or firstnamelastname@{preferred_domain})
- Generate a realistic password (8-12 characters)
- Make it feel like a real person
- Ensure the user is completely unique and different from any previously generated users

Return exactly this JSON format:
{{
  "first_name": "...",
  "last_name": "...", 
  "email": "...",
  "password": "..."
}}

No markdown, no explanation, no extra text."""

            try:
                rate_limit_claude()
                message = client.messages.create(
                    model="claude-3-5-sonnet-20241022",
                    max_tokens=300,
                    temperature=0.9 + (attempt * 0.1),  # Increase temperature with attempts for more variation
                    system="You are a generator of diverse, realistic user data. Create authentic users from different backgrounds with consistent name-email matching. Never create duplicate names or emails.",
                    messages=[
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "text",
                                    "text": prompt
                                }
                            ]
                        }
                    ]
                )

                text = message.content[0].text.strip()

                # Parse JSON
                json_start = text.find("{")
                json_end = text.rfind("}") + 1
                if json_start >= 0 and json_end > json_start:
                    json_str = text[json_start:json_end]
                    user = json.loads(json_str)
                    
                    # Validate required fields
                    required_fields = ["first_name", "last_name", "email", "password"]
                    if all(field in user and user[field] for field in required_fields):
                        
                        # Check if user is unique
                        if is_unique_user(user["email"], user["first_name"], user["last_name"]):
                            # Get CSRF token for this user
                            csrf_token = get_csrf_token()
                            if csrf_token:
                                user["csrfmiddlewaretoken"] = csrf_token
                                users.append(user)
                                print(f"    ✅ Generated: {user['first_name']} {user['last_name']} ({user['email']})")
                                break  # Success, exit attempt loop
                            else:
                                print(f"    ⚠️ Failed to get CSRF token for user {user_num}, attempt {attempt + 1}")
                        else:
                            print(f"    🔄 Duplicate user detected, retrying... (attempt {attempt + 1})")
                    else:
                        print(f"    ⚠️ User {user_num} missing required fields, attempt {attempt + 1}: {user}")
                else:
                    print(f"    ❌ User {user_num}: Could not find valid JSON in response, attempt {attempt + 1}")
                    
            except Exception as e:
                print(f"    ❌ User {user_num}: Error generating user (attempt {attempt + 1}) - {e}")
        
        else:
            # If we've exhausted all attempts
            print(f"    ❌ Failed to generate unique user {user_num} after {max_attempts} attempts")
    
    print(f"  ✅ Thread {batch_num}: Generated {len(users)} unique users successfully")
    return users

def save_users_thread_safe(new_users):
    """Thread-safe way to add users to the global list"""
    with results_lock:
        all_users.extend(new_users)

def main():
    print(f"🚀 Starting multithreaded generation of {NUM_USERS} diverse users...")
    print(f"⚙️ Configuration:")
    print(f"   - Max workers: {MAX_WORKERS}")
    print(f"   - Claude API rate limit: {CLAUDE_RATE_LIMIT} req/sec")
    print(f"   - CSRF API rate limit: {API_RATE_LIMIT} req/sec")
    print(f"   - Name categories: {', '.join(NAME_CATEGORIES[:5])}...")
    
    # Calculate batch sizes
    batch_size = max(1, NUM_USERS // MAX_WORKERS)
    batches = []
    
    for i in range(MAX_WORKERS):
        start_idx = i * batch_size
        if i == MAX_WORKERS - 1:
            # Last batch gets any remaining users
            current_batch_size = NUM_USERS - start_idx
        else:
            current_batch_size = batch_size
        
        if current_batch_size > 0:
            batches.append((i + 1, current_batch_size, start_idx))
    
    print(f"\n📊 Processing {len(batches)} batches...")
    
    start_time = time.time()
    
    # Process batches in parallel
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all batch tasks
        future_to_batch = {
            executor.submit(generate_user_batch, batch_info): batch_info[0] 
            for batch_info in batches
        }
        
        # Collect results as they complete
        completed_batches = 0
        for future in as_completed(future_to_batch):
            batch_num = future_to_batch[future]
            try:
                batch_users = future.result()
                save_users_thread_safe(batch_users)
                completed_batches += 1
                print(f"📈 Progress: {completed_batches}/{len(batches)} batches completed, {len(all_users)} users generated")
                
            except Exception as e:
                print(f"❌ Error in batch {batch_num}: {e}")
                completed_batches += 1

    # Save to file
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_users, f, indent=2, ensure_ascii=False)

    elapsed_time = time.time() - start_time

    print(f"\n🎉 Generation complete!")
    print(f"📊 Final Statistics:")
    print(f"   - Total users generated: {len(all_users)}/{NUM_USERS}")
    print(f"   - Success rate: {len(all_users)/NUM_USERS*100:.1f}%")
    print(f"   - Time elapsed: {elapsed_time:.1f} seconds")
    print(f"   - Average rate: {len(all_users)/elapsed_time:.1f} users/second")
    print(f"✅ Saved to: {OUTPUT_FILE}")

    if all_users:
        print("\n👥 Sample of generated users:")
        sample_size = min(8, len(all_users))
        for i, user in enumerate(all_users[:sample_size]):
            print(f"   {i+1}. {user['first_name']} {user['last_name']} - {user['email']}")
        if len(all_users) > sample_size:
            print(f"   ... and {len(all_users) - sample_size} more")
        
        # Show diversity stats
        print(f"\n🌍 Name diversity preview:")
        first_names = [u['first_name'] for u in all_users]
        last_names = [u['last_name'] for u in all_users]
        domains = [u['email'].split('@')[1] for u in all_users]
        
        print(f"   - Unique first names: {len(set(first_names))}/{len(first_names)}")
        print(f"   - Unique last names: {len(set(last_names))}/{len(last_names)}")
        print(f"   - Email domains used: {len(set(domains))} different domains")


if __name__ == "__main__":
    main()