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

# Add backfill folder to path for auth utilities
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../backfill")))
from auth import get_authenticated_session

from dotenv import load_dotenv

load_dotenv()

# ------------------------
# Config
# ------------------------
BASE_URL = "http://localhost:8000/api"
USERS_FILE = Path("../generated_json/users.json")
QUICK_LINKS_PER_USER = 3

# Threading configuration
MAX_WORKERS = 4  # Number of concurrent threads
API_RATE_LIMIT = 5  # requests per second

# Thread-safe variables
api_lock = threading.Lock()
last_api_call = {"time": 0}
results_lock = threading.Lock()
success_count = {"value": 0}
error_count = {"value": 0}

# Global cache of available content
available_content = []
content_lock = threading.Lock()

# ------------------------
# Rate limiting and utilities
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

def try_get_json(resp):
    """Safely get JSON from response"""
    try:
        data = resp.json()
        if isinstance(data, dict) and "results" in data and isinstance(data["results"], list):
            return data["results"]
        if isinstance(data, list):
            return data
        return data
    except Exception:
        return None

# ------------------------
# Authentication functions
# ------------------------
def get_user_authenticated_session(email, password):
    """Get authenticated session for a specific user"""
    session = requests.Session()
    
    # Step 1: Get CSRF token
    rate_limit_api()
    csrf_resp = session.get("http://localhost:8000/auth/get-csrf-token/")
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
        "Referer": "http://localhost:8000/auth/sign-in/",
    }

    login_resp = session.post(
        "http://localhost:8000/auth/sign-in/", 
        data=login_payload, 
        headers=headers, 
        allow_redirects=False
    )

    if login_resp.status_code != 302:
        raise Exception(f"Login failed for {email}: {login_resp.status_code}")

    return session

# ------------------------
# Content collection (admin only)
# ------------------------
def collect_all_available_content():
    """Use admin session to collect all available content once"""
    global available_content
    
    print("🔍 Collecting all available content using admin session...")
    admin_session = get_authenticated_session()
    
    try:
        # Get workspaces
        rate_limit_api()
        workspaces_resp = admin_session.get(f"{BASE_URL}/users/me/workspaces/")
        if workspaces_resp.status_code != 200:
            print(f"❌ Could not fetch workspaces: {workspaces_resp.status_code}")
            return
        
        workspaces = workspaces_resp.json()
        print(f"📂 Found {len(workspaces)} workspaces")
        
        for workspace in workspaces:
            if not isinstance(workspace, dict):
                continue
                
            workspace_slug = workspace.get("slug")
            workspace_name = workspace.get("name", workspace_slug)
            
            if not workspace_slug:
                continue
            
            print(f"  📂 Processing workspace: {workspace_name}")
            
            # Get projects
            rate_limit_api()
            projects_resp = admin_session.get(f"{BASE_URL}/workspaces/{workspace_slug}/projects/")
            if projects_resp.status_code != 200:
                continue
                
            projects = projects_resp.json()
            print(f"    📌 Found {len(projects)} projects")
            
            for project in projects:
                if not isinstance(project, dict):
                    continue
                    
                project_id = project.get("id")
                project_name = project.get("name", "Untitled Project")
                
                if not project_id:
                    continue
                
                print(f"      📋 Collecting content from: {project_name}")
                
                # Collect Issues
                try:
                    rate_limit_api()
                    issues_resp = admin_session.get(f"{BASE_URL}/workspaces/{workspace_slug}/projects/{project_id}/issues/")
                    if issues_resp.status_code == 200:
                        issues = try_get_json(issues_resp)
                        if issues and isinstance(issues, list):
                            for issue in issues[:3]:  # Limit to first 3 issues per project
                                if isinstance(issue, dict) and issue.get('id'):
                                    available_content.append({
                                        "type": "issue",
                                        "title": f"{project_name} - {issue.get('name', 'Untitled Issue')[:30]}",
                                        "url": f"http://localhost:8000/workspaces/{workspace_slug}/projects/{project_id}/issues/{issue.get('id')}",
                                        "workspace": workspace_name,
                                        "project": project_name
                                    })
                except Exception as e:
                    print(f"        ⚠️ Error fetching issues: {e}")
                
                # Collect Modules
                try:
                    rate_limit_api()
                    modules_resp = admin_session.get(f"{BASE_URL}/workspaces/{workspace_slug}/projects/{project_id}/modules/")
                    if modules_resp.status_code == 200:
                        modules = try_get_json(modules_resp)
                        if modules and isinstance(modules, list):
                            for module in modules[:2]:  # Limit to first 2 modules per project
                                if isinstance(module, dict) and module.get('id'):
                                    available_content.append({
                                        "type": "module",
                                        "title": f"{project_name} - {module.get('name', 'Untitled Module')[:30]}",
                                        "url": f"http://localhost:8000/workspaces/{workspace_slug}/projects/{project_id}/modules/{module.get('id')}",
                                        "workspace": workspace_name,
                                        "project": project_name
                                    })
                except Exception as e:
                    print(f"        ⚠️ Error fetching modules: {e}")
                
                # Collect Cycles
                try:
                    rate_limit_api()
                    cycles_resp = admin_session.get(f"{BASE_URL}/workspaces/{workspace_slug}/projects/{project_id}/cycles/")
                    if cycles_resp.status_code == 200:
                        cycles = try_get_json(cycles_resp)
                        if cycles and isinstance(cycles, list):
                            for cycle in cycles[:2]:  # Limit to first 2 cycles per project
                                if isinstance(cycle, dict) and cycle.get('id'):
                                    available_content.append({
                                        "type": "cycle",
                                        "title": f"{project_name} - {cycle.get('name', 'Untitled Cycle')[:30]}",
                                        "url": f"http://localhost:8000/workspaces/{workspace_slug}/projects/{project_id}/cycles/{cycle.get('id')}",
                                        "workspace": workspace_name,
                                        "project": project_name
                                    })
                except Exception as e:
                    print(f"        ⚠️ Error fetching cycles: {e}")
                
                # Collect Views
                try:
                    rate_limit_api()
                    views_resp = admin_session.get(f"{BASE_URL}/workspaces/{workspace_slug}/projects/{project_id}/views/")
                    if views_resp.status_code == 200:
                        views = try_get_json(views_resp)
                        if views and isinstance(views, list):
                            for view in views[:2]:  # Limit to first 2 views per project
                                if isinstance(view, dict) and view.get('id'):
                                    available_content.append({
                                        "type": "view",
                                        "title": f"{project_name} - {view.get('name', 'Untitled View')[:30]}",
                                        "url": f"http://localhost:8000/workspaces/{workspace_slug}/projects/{project_id}/views/{view.get('id')}",
                                        "workspace": workspace_name,
                                        "project": project_name
                                    })
                except Exception as e:
                    print(f"        ⚠️ Error fetching views: {e}")
        
        print(f"✅ Collected {len(available_content)} total content items")
        
        # Show content breakdown
        content_by_type = {}
        for content in available_content:
            content_type = content["type"]
            content_by_type[content_type] = content_by_type.get(content_type, 0) + 1
        
        print("📊 Content breakdown:")
        for content_type, count in content_by_type.items():
            print(f"   - {content_type}: {count} items")
        
    except Exception as e:
        print(f"❌ Error collecting content: {e}")

def get_random_content_for_user():
    """Get 3 random diverse content items for a user"""
    with content_lock:
        if len(available_content) < QUICK_LINKS_PER_USER:
            return available_content.copy()  # Return all if we don't have enough
        
        # Try to get diverse types
        selected = []
        content_by_type = {}
        
        for content in available_content:
            content_type = content["type"]
            if content_type not in content_by_type:
                content_by_type[content_type] = []
            content_by_type[content_type].append(content)
        
        # Select one from each type if possible
        for content_type in ["issue", "module", "cycle", "view"]:
            if content_type in content_by_type and len(selected) < QUICK_LINKS_PER_USER:
                selected.append(random.choice(content_by_type[content_type]))
        
        # Fill remaining slots randomly
        while len(selected) < QUICK_LINKS_PER_USER:
            remaining = [c for c in available_content if c not in selected]
            if not remaining:
                break
            selected.append(random.choice(remaining))
        
        return selected

def create_quick_link(session, workspace_slug, title, url):
    """Create a quick link for the user"""
    quick_link_url = f"{BASE_URL}/workspaces/{workspace_slug}/quick-links/"
    
    payload = {
        "title": title,
        "url": url
    }
    
    headers = {
        "Content-Type": "application/json",
        "X-CSRFToken": session.cookies.get("csrftoken", ""),
        "Referer": f"http://localhost:8000/workspaces/{workspace_slug}/"
    }
    
    rate_limit_api()
    resp = session.post(quick_link_url, json=payload, headers=headers)
    
    if resp.status_code in [200, 201]:
        return True
    else:
        print(f"        ❌ Failed to create quick link '{title}': {resp.status_code}")
        return False

# ------------------------
# Main processing function
# ------------------------
def setup_user_quick_links(user_data: tuple) -> tuple:
    """Setup quick links for a single user (runs in thread)"""
    user, index, total = user_data
    user_email = user.get("email", "Unknown")
    user_password = user.get("password", "")
    first_name = user.get("first_name", "User")
    last_name = user.get("last_name", "")
    
    if not user_email or not user_password:
        error_msg = "Missing email or password"
        update_counters(error=True)
        return (False, index, user_email, error_msg)
    
    try:
        # Login as this user
        session = get_user_authenticated_session(user_email, user_password)
        
        # Get user's workspaces to find primary workspace
        rate_limit_api()
        workspaces_resp = session.get(f"{BASE_URL}/users/me/workspaces/")
        if workspaces_resp.status_code != 200:
            error_msg = f"Could not fetch workspaces: {workspaces_resp.status_code}"
            update_counters(error=True)
            return (False, index, user_email, error_msg)
        
        workspaces = workspaces_resp.json()
        if not workspaces or not isinstance(workspaces, list):
            error_msg = "User has no accessible workspaces"
            update_counters(error=True)
            return (False, index, user_email, error_msg)
        
        primary_workspace = workspaces[0].get("slug")
        if not primary_workspace:
            error_msg = "Primary workspace has no slug"
            update_counters(error=True)
            return (False, index, user_email, error_msg)
        
        # Get random content for this user
        selected_content = get_random_content_for_user()
        
        if len(selected_content) == 0:
            error_msg = "No content available for quick links"
            update_counters(error=True)
            return (False, index, user_email, error_msg)
        
        # Create the quick links
        created_links = 0
        for i, content in enumerate(selected_content):
            title = f"{content['type'].title()}: {content['title']}"
            url = content['url']
            
            if create_quick_link(session, primary_workspace, title, url):
                created_links += 1
            
            time.sleep(0.1)  # Small delay between creations
        
        if created_links > 0:
            update_counters(success=True)
            return (True, index, user_email, f"Created {created_links}/{len(selected_content)} quick links")
        else:
            error_msg = "Failed to create any quick links"
            update_counters(error=True)
            return (False, index, user_email, error_msg)
            
    except Exception as e:
        error_msg = f"Exception: {str(e)[:100]}"
        update_counters(error=True)
        return (False, index, user_email, error_msg)

def process_users_batch(users_batch: list, batch_num: int, total_batches: int):
    """Process a batch of users with threading"""
    print(f"\n🔄 Processing batch {batch_num}/{total_batches} ({len(users_batch)} users)")
    
    # Use threading for this batch
    batch_success = 0
    batch_errors = 0
    
    with ThreadPoolExecutor(max_workers=2) as executor:  # Small thread pool per batch
        # Prepare user data with indices
        user_data_list = [(user, i + ((batch_num - 1) * len(users_batch)), len(users_batch)) 
                         for i, user in enumerate(users_batch)]
        
        # Submit all user tasks
        future_to_user = {
            executor.submit(setup_user_quick_links, user_data): user_data[0]["email"] 
            for user_data in user_data_list
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_user):
            user_email = future_to_user[future]
            try:
                success, index, email, message = future.result()
                
                if success:
                    batch_success += 1
                    print(f"  ✅ [{index}] {email}: {message}")
                else:
                    batch_errors += 1
                    print(f"  ❌ [{index}] {email}: {message}")
                    
            except Exception as e:
                batch_errors += 1
                print(f"  ❌ Error processing user {user_email}: {e}")
    
    # Batch summary
    total_success, total_errors = get_counts()
    print(f"📊 Batch {batch_num} complete: {batch_success} success, {batch_errors} errors")

def validate_users(users):
    """Validate users have required fields"""
    valid_users = []
    invalid_count = 0
    
    for i, user in enumerate(users):
        if not isinstance(user, dict):
            print(f"⚠️ User {i+1} is not a dictionary, skipping")
            invalid_count += 1
            continue
            
        required_fields = ["email", "password"]
        missing_fields = [field for field in required_fields if not user.get(field)]
        
        if missing_fields:
            print(f"⚠️ User {i+1} missing required fields: {missing_fields}, skipping")
            invalid_count += 1
            continue
            
        valid_users.append(user)
    
    if invalid_count > 0:
        print(f"⚠️ Skipped {invalid_count} invalid users")
    
    return valid_users

# ------------------------
# Main
# ------------------------
def main():
    print("🚀 Starting simplified user quick links setup...")
    
    # Step 1: Collect all available content using admin session
    collect_all_available_content()
    
    if len(available_content) < QUICK_LINKS_PER_USER:
        print(f"❌ Not enough content available ({len(available_content)} found, need {QUICK_LINKS_PER_USER})")
        return
    
    # Step 2: Load and validate users
    if not USERS_FILE.exists():
        raise SystemExit(f"❌ File not found: {USERS_FILE}")

    with USERS_FILE.open("r", encoding="utf-8") as f:
        users = json.load(f)

    # Step 3: Add admin user to the list
    admin_email = os.getenv("SEEDER_ADMIN_EMAIL")
    admin_password = os.getenv("SEEDER_ADMIN_PASSWORD")
    
    if admin_email and admin_password:
        admin_user = {
            "first_name": "Admin",
            "last_name": "User", 
            "email": admin_email,
            "password": admin_password
        }
        users.append(admin_user)
        print(f"➕ Added admin account ({admin_email}) to processing list")
    else:
        print("⚠️ Admin credentials not found in environment variables, skipping admin account")

    print(f"👥 Found {len(users)} total users (including admin) to setup quick links for.")
    
    if not users:
        print("ℹ️ No users to process.")
        return

    valid_users = validate_users(users)
    if not valid_users:
        print("❌ No valid users found. Cannot proceed.")
        return
    
    print(f"📊 Processing {len(valid_users)} valid users")

    # Show configuration
    print(f"\n⚙️ Configuration:")
    print(f"   - Available content items: {len(available_content)}")
    print(f"   - Quick links per user: {QUICK_LINKS_PER_USER}")
    print(f"   - Total users: {len(valid_users)}")
    print(f"   - Max workers per batch: 2")

    # Process in batches
    start_time = time.time()
    batch_size = 10  # Larger batches since we're not doing complex queries per user
    total_batches = (len(valid_users) + batch_size - 1) // batch_size
    
    for batch_num in range(1, total_batches + 1):
        start_idx = (batch_num - 1) * batch_size
        end_idx = min(start_idx + batch_size, len(valid_users))
        batch_users = valid_users[start_idx:end_idx]
        
        process_users_batch(batch_users, batch_num, total_batches)
        
        # Small delay between batches
        if batch_num < total_batches:
            time.sleep(1)

    # Final summary
    elapsed_time = time.time() - start_time
    final_success, final_errors = get_counts()
    total_processed = final_success + final_errors
    
    print(f"\n🎉 Quick links setup complete!")
    print(f"📊 Final Statistics:")
    print(f"   - Total processed: {total_processed}/{len(valid_users)}")
    print(f"   - Successful: {final_success} ({final_success/total_processed*100:.1f}%)" if total_processed > 0 else "")
    print(f"   - Failed: {final_errors} ({final_errors/total_processed*100:.1f}%)" if total_processed > 0 else "")
    print(f"   - Time elapsed: {elapsed_time:.1f} seconds")
    print(f"   - Average rate: {total_processed/elapsed_time:.1f} users/second" if elapsed_time > 0 else "")
    print(f"   - Total quick links created: ~{final_success * QUICK_LINKS_PER_USER}")

if __name__ == "__main__":
    main()