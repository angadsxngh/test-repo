import os
from dotenv import load_dotenv
import requests

# Local imports
from generate.generate_workspace import generate_workspace
from generate.generate_user import generate_user

from backfill.backfill_workspace import backfill_workspace
from backfill.backfill_issue import backfill_issue
from backfill.backfill_user import backfill_user

# -----------------------------
# Load .env
# -----------------------------
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path = os.path.join(project_root, '.env')
load_dotenv(dotenv_path=dotenv_path)

SEEDER_ADMIN_EMAIL = os.getenv("SEEDER_ADMIN_EMAIL")
SEEDER_ADMIN_PASSWORD = os.getenv("SEEDER_ADMIN_PASSWORD")

class PlaneSeeder:
    def __init__(self):
        self.session = requests.Session()
        self.csrf_token = None
        self.base_url = "http://localhost:8000"
        self.web_url = "http://localhost:3000"

    def login(self):
        print("🔑 Getting CSRF token...")
        resp = self.session.get(f"{self.base_url}/auth/get-csrf-token/")
        self.csrf_token = self.session.cookies.get("csrftoken")
        print(f"CSRF Token: {self.csrf_token}")

        if not self.csrf_token:
            print("❌ Failed to fetch CSRF.")
            return False

        payload = {
            "csrfmiddlewaretoken": self.csrf_token,
            "email": SEEDER_ADMIN_EMAIL,
            "password": SEEDER_ADMIN_PASSWORD
        }

        headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": self.web_url  # ✅ Keep Referer!
        }

        print("Logging in...")

        resp = self.session.post(
            f"{self.base_url}/auth/sign-in/",
            data=payload,
            headers=headers
        )

        print(f"Status: {resp.status_code} | {resp.text}")

        # ✅ Now test with GET to profile
        test = self.session.get(f"{self.base_url}/api/users/me/profile/")
        print(f"Profile status: {test.status_code} | {test.text}")

        if test.status_code == 200:
            print("✅ Login success.")
            return True
        else:
            print("❌ Login failed.")
            return False



if __name__ == "__main__":
    seeder = PlaneSeeder()

    if not seeder.login():
        exit(1)

    generate_workspace()
    backfill_workspace(seeder)

    generate_user()
    backfill_user(seeder, "example-workspace-slug")  # Replace with real slug!
