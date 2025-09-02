#!/usr/bin/env python3
import os
import sys
import time
import requests
from pathlib import Path
from dotenv import load_dotenv

# Add backfill folder to path for auth and settings (matching user script pattern)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../backfill")))

# Import the same way as user script
try:
    from auth import get_authenticated_session
except ImportError:
    print("❌ Could not import auth module. Make sure you're running from the correct directory.")
    sys.exit(1)

# Load env variables
load_dotenv()

ADMIN_FIRST_NAME = os.getenv("SEEDER_ADMIN_FIRST_NAME", "Admin")
ADMIN_LAST_NAME = os.getenv("SEEDER_ADMIN_LAST_NAME", "Example")
ADMIN_EMAIL = os.getenv("SEEDER_ADMIN_EMAIL", "admin@example.com")
ADMIN_COMPANY = os.getenv("SEEDER_COMPANY_NAME", "Example Org")
ADMIN_PASSWORD = os.getenv("SEEDER_ADMIN_PASSWORD", "Admin@1234!")
IS_TELEMETRY_ENABLED = os.getenv("IS_TELEMETRY_ENABLED", "True")

# URLs (matching user script pattern)
BASE_URL = "http://localhost:80/api"
CSRF_URL = "http://localhost:80/auth/get-csrf-token/"
SIGNUP_URL = "http://localhost:80/api/instances/admins/sign-up/"
LOGIN_URL = "http://localhost/auth/sign-in/"

def get_csrf_token(session):
    """Get CSRF token from server"""
    csrf_response = session.get(CSRF_URL)
    csrf_response.raise_for_status()
    csrf_token = csrf_response.json().get("csrf_token")
    
    if not csrf_token:
        raise ValueError("Failed to fetch CSRF token")
    
    return csrf_token

def create_admin():
    """Create admin user (Phase 1)"""
    print("🚀 Phase 1: Creating admin user...")
    
    # Create session
    session = requests.Session()
    
    # Get CSRF token
    csrf_token = get_csrf_token(session)
    print("[INFO] Fetched CSRF Token for admin creation")

    # Prepare payload
    payload = {
        "csrfmiddlewaretoken": csrf_token,
        "is_telemetry_enabled": IS_TELEMETRY_ENABLED,
        "first_name": ADMIN_FIRST_NAME,
        "last_name": ADMIN_LAST_NAME,
        "email": ADMIN_EMAIL,
        "company_name": ADMIN_COMPANY,
        "password": ADMIN_PASSWORD,
        "confirm_password": ADMIN_PASSWORD
    }

    headers = {
        "Referer": "http://localhost:80",
        "X-CSRFToken": csrf_token,
        "Accept": "application/json"
    }

    # Try to create admin
    print(f"[INFO] Creating admin: {ADMIN_EMAIL}")
    response = session.post(SIGNUP_URL, data=payload, headers=headers)

    try:
        resp_json = response.json()
    except Exception:
        resp_json = {"raw": response.text}

    # Handle response
    admin_created = False
    if response.status_code in (200, 201):
        print(f"✅ Admin created: {ADMIN_EMAIL}")
        admin_created = True
    elif response.status_code in (302, 400):
        error_msg = str(resp_json)
        if "email" in error_msg.lower() and "exists" in error_msg.lower():
            print(f"ℹ️ Admin already exists: {ADMIN_EMAIL} — proceeding to profile setup")
            admin_created = True  # Existing admin, we can still setup profile
        elif "invalid_admin_password" in error_msg.lower():
            print("[ERROR] Password does not meet requirements — update ADMIN_PASSWORD in .env")
            return False
        else:
            print(f"[ERROR] Could not create admin: {error_msg}")
            return False
    elif response.status_code == 500:
        # Check if this is a frontend build error (HTML response) vs actual API error
        if "<!DOCTYPE html>" in str(resp_json.get("raw", "")):
            print("⚠️ Frontend build error detected (status 500), but admin might be created")
            print("🔍 Checking if admin was actually created by attempting login...")
            # We'll try to proceed to login phase to verify if admin exists
            admin_created = True
        else:
            print(f"[ERROR] Server error (500): {resp_json}")
            return False
    else:
        print(f"[ERROR] Unexpected status {response.status_code}: {resp_json}")
        return False
    
    return admin_created

def get_admin_authenticated_session():
    """Get authenticated session for admin user (using same pattern as user script)"""
    session = requests.Session()
    
    # Step 1: Get CSRF token (exact same pattern as user script)
    csrf_resp = session.get(CSRF_URL)
    if csrf_resp.status_code != 200:
        raise Exception(f"Failed to get CSRF token for admin: {csrf_resp.status_code}")
    
    csrf_token = csrf_resp.json().get("csrf_token")
    if not csrf_token:
        raise Exception("CSRF token not found for admin")

    # Step 2: Login (exact same pattern as user script)
    login_payload = {
        "csrfmiddlewaretoken": csrf_token,
        "email": ADMIN_EMAIL,
        "password": ADMIN_PASSWORD,
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": "http://localhost:8000/auth/sign-in/",
    }

    login_resp = session.post(
        LOGIN_URL, 
        data=login_payload, 
        headers=headers, 
        allow_redirects=False
    )

    if login_resp.status_code != 302:
        raise Exception(f"Login failed for admin: {login_resp.status_code}")

    return session

def setup_admin_profile():
    """Setup admin profile information (Phase 2) - using exact same pattern as user script"""
    print("\n🔧 Phase 2: Setting up admin profile...")
    
    try:
        # Use the same authentication pattern as the user script
        session = get_admin_authenticated_session()
        print("✅ Admin authentication successful")
        
        # Prepare headers for API calls (exact same pattern as user script)
        headers = {
            "Content-Type": "application/json",
            "X-CSRFToken": session.cookies.get("csrftoken", ""),
            "Referer": "http://localhost:8000/"
        }
        
        # Step 1: Update admin name (using same pattern as user script)
        print(f"📝 Setting admin name: {ADMIN_FIRST_NAME} {ADMIN_LAST_NAME}")
        name_payload = {
            "first_name": ADMIN_FIRST_NAME,
            "last_name": ADMIN_LAST_NAME
        }
        
        name_resp = session.patch(
            f"{BASE_URL}/users/me/", 
            json=name_payload, 
            headers=headers
        )
        
        if name_resp.status_code == 200:
            print("✅ Admin name updated successfully")
        else:
            print(f"⚠️ Failed to update admin name - HTTP {name_resp.status_code}: {name_resp.text[:200]}")
        
        # Step 2: Setup admin profile (exact same pattern as user script)
        print("👑 Setting admin profile: Executive role, Engineering use case")
        profile_payload = {
            "role": "Executive",
            "use_case": "Engineering"
        }
        
        profile_resp = session.patch(
            f"{BASE_URL}/users/me/profile/", 
            json=profile_payload, 
            headers=headers
        )
        
        if profile_resp.status_code == 200:
            print("✅ Admin profile updated successfully")
            return True
        else:
            print(f"❌ Failed to update admin profile - HTTP {profile_resp.status_code}: {profile_resp.text[:200]}")
            return False
            
    except Exception as e:
        print(f"❌ Error setting up admin profile: {str(e)}")
        return False

def main():
    """Main function to create admin and setup profile"""
    print("🚀 Starting admin backfill with profile setup...")
    start_time = time.time()
    
    # Phase 1: Create admin user
    admin_available = create_admin()
    
    if not admin_available:
        print("❌ Admin creation failed. Stopping.")
        return
    
    # Small delay between phases (same as user script)
    print("\n⏳ Waiting 2 seconds before setting up admin profile...")
    time.sleep(2)
    
    # Phase 2: Setup admin profile
    profile_success = setup_admin_profile()
    
    # Summary
    elapsed_time = time.time() - start_time
    print(f"\n🎊 Admin backfill complete! Total time: {elapsed_time:.1f} seconds")
    
    if profile_success:
        print("✅ Admin user created and profile setup completed successfully!")
        print("📋 Admin Details:")
        print(f"   - Email: {ADMIN_EMAIL}")
        print(f"   - Name: {ADMIN_FIRST_NAME} {ADMIN_LAST_NAME}")
        print(f"   - Company: {ADMIN_COMPANY}")
        print("   - Role: Executive")
        print("   - Use Case: Engineering")
    else:
        print("⚠️ Admin user created but profile setup had issues. Check the logs above.")

if __name__ == "__main__":
    main()