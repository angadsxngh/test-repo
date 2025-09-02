import requests
from dotenv import load_dotenv
import os

load_dotenv()

CSRF_URL = "http://localhost:80/auth/get-csrf-token/"
ADMIN_EMAIL = os.getenv("SEEDER_ADMIN_EMAIL")
ADMIN_PASSWORD = os.getenv("SEEDER_ADMIN_PASSWORD")
LOGIN_URL = "http://localhost:80/auth/sign-in/"


def get_authenticated_session():
    session = requests.Session()

    # Step 1: Get CSRF token from dedicated endpoint
    response = session.get(CSRF_URL)
    if response.status_code != 200:
        raise Exception(f"❌ Failed to get CSRF token: {response.status_code}")
    
    

    csrf_token = response.json().get("csrf_token")
    if not csrf_token:
        raise Exception("❌ CSRF token not found in response.")

    # Step 2: Log in using POST
    payload = {
        "csrfmiddlewaretoken": csrf_token,
        "email": ADMIN_EMAIL,
        "password": ADMIN_PASSWORD,
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": LOGIN_URL,
    }

    response = session.post(LOGIN_URL, data=payload, headers=headers, allow_redirects=False)

    if response.status_code != 302:
        raise Exception(f"❌ Login failed: {response.status_code}\n{response.text}")

    print("✅ Logged in successfully.")
    return session
