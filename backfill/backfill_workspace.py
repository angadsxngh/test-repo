import json
import os
import time
import random
import requests
from dotenv import load_dotenv
from auth import get_authenticated_session  # Admin login session provider

load_dotenv()

# =========================
# CONFIG
# =========================
# Paths
WORKSPACE_FILE = "../generated_json/workspace.json"
USERS_FILE = "../generated_json/users.json"

# URLs
WORKSPACE_URL = "http://localhost:8000/api/workspaces/"
INVITE_URL_TEMPLATE = "http://localhost:8000/api/workspaces/{slug}/invitations/"
MEMBERS_URL_TEMPLATE = "http://localhost:8000/api/workspaces/{slug}/members/"
INVITATION_FETCH_URL = "http://localhost:8000/api/users/me/workspaces/invitations/"
INVITATION_ACCEPT_URL = "http://localhost:8000/api/users/me/workspaces/invitations/"

# Environment variables
ADMIN_EMAIL = os.getenv("SEEDER_ADMIN_EMAIL")
ADMIN_PASSWORD = os.getenv("SEEDER_ADMIN_PASSWORD")

# =========================
# UTILITY FUNCTIONS
# =========================
def load_json(file_path):
    """Load JSON file with error handling"""
    try:
        with open(file_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"❌ File not found: {file_path}")
        return []
    except json.JSONDecodeError:
        print(f"❌ Invalid JSON in file: {file_path}")
        return []

# =========================
# WORKSPACE CREATION FUNCTIONS
# =========================
def create_workspaces(session):
    """Create all workspaces from the JSON file"""
    print("🚀 Starting workspace creation phase...")
    
    workspaces = load_json(WORKSPACE_FILE)
    if not workspaces:
        print("❌ No workspaces to create.")
        return False

    print(f"📂 Found {len(workspaces)} workspaces to create.")
    
    created_count = 0
    existing_count = 0
    failed_count = 0

    for i, workspace in enumerate(workspaces, 1):
        workspace_name = workspace.get("name", "Unknown")
        workspace_slug = workspace.get("slug", "")
        
        if not workspace_slug:
            print(f"  ⚠️ [{i}/{len(workspaces)}] Skipping workspace without slug: {workspace_name}")
            failed_count += 1
            continue

        payload = {
            "name": workspace["name"],
            "slug": workspace["slug"],
            "organization_size": workspace.get("organization_size", "1-10")
        }

        try:
            response = session.post(WORKSPACE_URL, json=payload)
            
            if response.status_code == 201:
                created_count += 1
                print(f"  ✅ [{i}/{len(workspaces)}] Created workspace: {workspace_name}")
            elif response.status_code == 400 and "slug" in response.text.lower():
                existing_count += 1
                print(f"  ⚠️ [{i}/{len(workspaces)}] Workspace already exists: {workspace_name}")
            else:
                failed_count += 1
                print(f"  ❌ [{i}/{len(workspaces)}] Failed to create workspace: {workspace_name}")
                print(f"      Status: {response.status_code}, Response: {response.text[:100]}")
                
        except Exception as e:
            failed_count += 1
            print(f"  ❌ [{i}/{len(workspaces)}] Error creating workspace {workspace_name}: {e}")

        # Small delay to be respectful to the API
        time.sleep(0.5)

    print(f"\n📊 Workspace creation complete:")
    print(f"   - Created: {created_count}")
    print(f"   - Already existed: {existing_count}")
    print(f"   - Failed: {failed_count}")
    print(f"   - Total processed: {created_count + existing_count + failed_count}")
    
    return created_count + existing_count > 0  # Return True if we have workspaces to assign users to

# =========================
# USER ASSIGNMENT FUNCTIONS
# =========================
def get_existing_members(admin_session, workspace_slug):
    """Get list of existing member emails in the workspace"""
    url = MEMBERS_URL_TEMPLATE.format(slug=workspace_slug)
    
    headers = {
        "Referer": f"http://localhost:8000/workspaces/{workspace_slug}/settings/members",
        "X-CSRFToken": admin_session.cookies.get("csrftoken", ""),
        "Content-Type": "application/json",
    }
    
    try:
        response = admin_session.get(url, headers=headers)
        if response.status_code == 200:
            members_data = response.json()
            # Extract emails from member data - adjust this based on your API response structure
            existing_emails = set()
            if isinstance(members_data, list):
                for member in members_data:
                    if 'email' in member:
                        existing_emails.add(member['email'])
                    elif 'member' in member and 'email' in member['member']:
                        existing_emails.add(member['member']['email'])
            elif isinstance(members_data, dict) and 'results' in members_data:
                for member in members_data['results']:
                    if 'email' in member:
                        existing_emails.add(member['email'])
                    elif 'member' in member and 'email' in member['member']:
                        existing_emails.add(member['member']['email'])
            
            return existing_emails
        else:
            print(f"⚠️ Could not fetch members for {workspace_slug}: {response.status_code}")
            return set()
    except Exception as e:
        print(f"⚠️ Error fetching members for {workspace_slug}: {e}")
        return set()

def send_invites(admin_session, workspace_slug, user_batch):
    """Send invitations to users for a workspace"""
    if not user_batch:
        print(f"ℹ️ No users to invite to workspace '{workspace_slug}' (all are already members)")
        return 0
        
    url = INVITE_URL_TEMPLATE.format(slug=workspace_slug)
    ROLE_ID = 15  # member role

    payload = {
        "emails": [{"email": user["email"], "role": ROLE_ID} for user in user_batch]
    }

    headers = {
        "Referer": f"http://localhost:8000/workspaces/{workspace_slug}/settings/members",
        "X-CSRFToken": admin_session.cookies.get("csrftoken", ""),
        "Content-Type": "application/json",
    }

    print(f"  ➡️ Sending invites to workspace '{workspace_slug}' for {len(user_batch)} users:")
    for u in user_batch[:3]:  # Show first 3 users to avoid spam
        print(f"     - {u['email']}")
    if len(user_batch) > 3:
        print(f"     - ... and {len(user_batch) - 3} more")

    try:
        response = admin_session.post(url, json=payload, headers=headers)

        if response.status_code == 200:
            print(f"  ✅ Invited {len(user_batch)} users to '{workspace_slug}'")
            return len(user_batch)
        else:
            print(f"  ❌ Failed to invite users to '{workspace_slug}': {response.status_code} - {response.text[:100]}")
            return 0
    except Exception as e:
        print(f"  ❌ Error sending invites to '{workspace_slug}': {e}")
        return 0

def accept_all_invites(email, password):
    """Accept all workspace invitations for a user"""
    print(f"  🔑 Accepting invites for: {email}")
    session = requests.Session()

    try:
        # Get CSRF token via API
        csrf_resp = session.get("http://localhost:8000/auth/get-csrf-token/")
        if csrf_resp.status_code != 200:
            print(f"    ❌ Failed to get CSRF token for {email}")
            return False

        csrf_token = csrf_resp.json().get("csrf_token")
        if not csrf_token:
            print(f"    ❌ CSRF token missing from API response for {email}")
            return False

        login_payload = {
            "email": email,
            "password": password,
            "csrfmiddlewaretoken": csrf_token,
        }

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": "http://localhost:8000/auth/sign-in/",
        }

        login_resp = session.post("http://localhost:8000/auth/sign-in/", data=login_payload, headers=headers, allow_redirects=False)
        if login_resp.status_code != 302:
            print(f"    ❌ Login failed for {email}: {login_resp.status_code}")
            return False

        # Fetch invitations
        invites_resp = session.get(INVITATION_FETCH_URL)
        if invites_resp.status_code != 200:
            print(f"    ❌ Could not fetch invites for {email}: {invites_resp.status_code}")
            return False

        invite_data = invites_resp.json()
        invite_ids = [invite["id"] for invite in invite_data if "id" in invite]

        if not invite_ids:
            print(f"    📭 No invites to accept for {email}")
            return True

        print(f"    📋 Accepting {len(invite_ids)} invites for {email}")

        csrf_token = session.cookies.get("csrftoken")
        session_id = session.cookies.get("session-id")

        headers = {
            "Content-Type": "application/json",
            "X-CSRFToken": csrf_token,
            "Cookie": f"csrftoken={csrf_token}; session-id={session_id}",
        }

        payload = {"invitations": invite_ids}
        accept_resp = session.post(INVITATION_ACCEPT_URL, json=payload, headers=headers)

        if accept_resp.status_code == 204:
            print(f"    ✅ Successfully accepted invites for {email}")
            return True
        else:
            print(f"    ❌ Error accepting invites for {email}: {accept_resp.status_code}")
            return False
            
    except Exception as e:
        print(f"    ❌ Error processing invites for {email}: {e}")
        return False

def assign_users_to_workspaces():
    """Assign users to workspaces through invitations"""
    print("\n🔧 Starting user assignment phase...")
    
    workspaces = load_json(WORKSPACE_FILE)
    users = load_json(USERS_FILE)

    if not workspaces:
        print("❌ No workspaces loaded.")
        return False
    if not users:
        print("❌ No users loaded.")
        return False

    # Admin login
    admin_session = get_authenticated_session()

    print(f"\n📊 Summary:")
    print(f"   - Total workspaces: {len(workspaces)}")
    print(f"   - Total users available: {len(users)}")

    print("\n📨 Starting to send invites...\n")
    
    total_invites_sent = 0
    users_who_received_invites = set()
    
    for i, workspace in enumerate(workspaces, 1):
        slug = workspace.get("slug")
        name = workspace.get("name", slug)
        
        if not slug:
            print(f"⚠️ [{i}/{len(workspaces)}] Skipping workspace without slug.")
            continue

        print(f"\n🔍 [{i}/{len(workspaces)}] Processing workspace '{name}' ({slug})...")
        
        # Get existing members for this workspace
        existing_members = get_existing_members(admin_session, slug)
        print(f"     - Found {len(existing_members)} existing members")
        
        # Filter out users who are already members
        users_to_invite = [user for user in users if user["email"] not in existing_members]
        already_members = [user for user in users if user["email"] in existing_members]
        
        if already_members:
            print(f"     - Skipping {len(already_members)} users who are already members")
        
        # Send invites to users who are not already members
        if users_to_invite:
            invites_sent = send_invites(admin_session, slug, users_to_invite)
            total_invites_sent += invites_sent
            if invites_sent > 0:
                users_who_received_invites.update([user["email"] for user in users_to_invite])
        
        time.sleep(1)  # Prevent rate-limiting

    print(f"\n✅ All invites sent. Total invitations: {total_invites_sent}")
    print(f"📧 Users who received invites: {len(users_who_received_invites)}")
    
    if users_who_received_invites:
        print("\n🔄 Now accepting invitations...\n")
        
        # Accept invites only for users who actually received them
        invited_users = [user for user in users if user["email"] in users_who_received_invites]
        successful_accepts = 0
        
        for i, user in enumerate(invited_users, 1):
            print(f"📧 [{i}/{len(invited_users)}] Processing invites for {user['email']}")
            if accept_all_invites(user["email"], user["password"]):
                successful_accepts += 1
            time.sleep(1)
        
        print(f"\n📊 Invitation acceptance complete:")
        print(f"   - Users who received invites: {len(invited_users)}")
        print(f"   - Successfully accepted: {successful_accepts}")
        print(f"   - Failed to accept: {len(invited_users) - successful_accepts}")

    return True

# =========================
# MAIN FUNCTION
# =========================
def main():
    """Main function that orchestrates workspace creation and user assignment"""
    print("🚀 Starting combined workspace creation and user assignment...")
    start_time = time.time()
    
    # Phase 1: Create workspaces
    workspaces_available = create_workspaces(get_authenticated_session())
    
    if not workspaces_available:
        print("❌ No workspaces were created or available. Stopping.")
        return
    
    # Small delay between phases
    print("\n⏳ Waiting 3 seconds before starting user assignment...")
    time.sleep(3)
    
    # Phase 2: Assign users to workspaces
    assignment_success = assign_users_to_workspaces()
    
    # Overall summary
    total_elapsed = time.time() - start_time
    print(f"\n🎊 Complete! Total time elapsed: {total_elapsed:.1f} seconds")
    
    if assignment_success:
        print("✅ Both workspace creation and user assignment phases completed successfully!")
    else:
        print("⚠️ Workspace creation completed, but user assignment had issues.")

if __name__ == "__main__":
    main()