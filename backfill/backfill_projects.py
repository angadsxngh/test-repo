import json
import os
import requests
import random
import time
from collections import defaultdict
from auth import get_authenticated_session

# =========================
# CONFIG
# =========================
# File paths
WORKSPACES_FILE = "../generated_json/workspace.json"
PROJECTS_FILE = "../generated_json/projects.json"

# API configuration
BASE_URL = "http://localhost:8000/api"
ROLE = 15  # 5 = guest, 15 = member

# Realistic assignment parameters
MIN_PROJECTS_PER_MEMBER = 2    # Each member should be in at least 2 projects
MAX_PROJECTS_PER_MEMBER = 6    # No member should be in more than 6 projects
MIN_MEMBERS_PER_PROJECT = 3    # Each project should have at least 3 members
MAX_MEMBERS_PER_PROJECT = 8    # No project should have more than 8 members

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

def get_workspaces_from_file():
    """Load workspace slugs from workspaces.json"""
    workspaces = load_json(WORKSPACES_FILE)
    return [ws["slug"] for ws in workspaces if "slug" in ws]

# =========================
# PROJECT CREATION FUNCTIONS
# =========================
def create_projects(session):
    """Create all projects from the JSON file"""
    print("🚀 Starting project creation phase...")
    
    workspaces = load_json(WORKSPACES_FILE)
    all_projects = load_json(PROJECTS_FILE)
    
    if not workspaces:
        print("❌ No workspaces loaded.")
        return False
    
    if not all_projects:
        print("❌ No projects loaded.")
        return False

    print(f"📂 Found {len(workspaces)} workspaces")
    print(f"📌 Found {len(all_projects)} total projects to create")
    
    total_created = 0
    total_existing = 0
    total_failed = 0

    for workspace in workspaces:
        slug = workspace["slug"]
        workspace_name = workspace.get("name", slug)
        print(f"\n🚀 Processing workspace: {workspace_name} ({slug})")

        # Filter projects for this workspace
        workspace_projects = [p for p in all_projects if p.get("workspace_slug") == slug]
        
        if not workspace_projects:
            print("⚠️ No projects found for this workspace.")
            continue

        print(f"📌 Found {len(workspace_projects)} projects for this workspace")
        
        url = f"{BASE_URL}/workspaces/{slug}/projects/"
        
        workspace_created = 0
        workspace_existing = 0
        workspace_failed = 0

        for i, project in enumerate(workspace_projects, 1):
            project_name = project.get("name", "Unknown")
            
            try:
                # Remove workspace_slug and csrfmiddlewaretoken as they're not needed for API
                project_payload = {k: v for k, v in project.items() 
                                 if k not in ["workspace_slug", "csrfmiddlewaretoken"]}
                
                response = session.post(url, json=project_payload)

                if response.status_code == 201:
                    workspace_created += 1
                    print(f"  ✅ [{i}/{len(workspace_projects)}] Created project: {project_name}")
                elif response.status_code == 400 and ("identifier" in response.text.lower() or "name" in response.text.lower()):
                    workspace_existing += 1
                    print(f"  ⚠️ [{i}/{len(workspace_projects)}] Project already exists: {project_name}")
                else:
                    workspace_failed += 1
                    print(f"  ❌ [{i}/{len(workspace_projects)}] Failed to create project: {project_name}")
                    print(f"      Status: {response.status_code}, Response: {response.text[:100]}")

            except Exception as e:
                workspace_failed += 1
                print(f"  ❌ [{i}/{len(workspace_projects)}] Error creating project {project_name}: {e}")

            time.sleep(0.5)  # Rate limiting

        print(f"📊 Workspace {workspace_name} summary:")
        print(f"   - Created: {workspace_created}")
        print(f"   - Already existed: {workspace_existing}")
        print(f"   - Failed: {workspace_failed}")
        
        total_created += workspace_created
        total_existing += workspace_existing
        total_failed += workspace_failed

    print(f"\n📊 Project creation complete:")
    print(f"   - Total created: {total_created}")
    print(f"   - Total already existed: {total_existing}")
    print(f"   - Total failed: {total_failed}")
    print(f"   - Total processed: {total_created + total_existing + total_failed}")
    
    return total_created + total_existing > 0  # Return True if we have projects to assign users to

# =========================
# USER ASSIGNMENT FUNCTIONS
# =========================
def get_workspace_members(session, slug):
    """Fetch all members of a workspace"""
    url = f"{BASE_URL}/workspaces/{slug}/members/"
    try:
        resp = session.get(url)
        if resp.status_code != 200:
            print(f"⚠️ Failed to fetch members for {slug}: {resp.status_code}")
            return []
        return resp.json()
    except Exception as e:
        print(f"⚠️ Error fetching members for {slug}: {e}")
        return []

def get_workspace_projects(session, slug):
    """Fetch all projects in a workspace"""
    url = f"{BASE_URL}/workspaces/{slug}/projects/"
    try:
        resp = session.get(url)
        if resp.status_code != 200:
            print(f"⚠️ Failed to fetch projects for {slug}: {resp.status_code}")
            return []
        return resp.json()
    except Exception as e:
        print(f"⚠️ Error fetching projects for {slug}: {e}")
        return []

def create_realistic_assignments(members, projects):
    """
    Create realistic project assignments that ensure:
    - Every member is assigned to multiple projects (but not all)
    - Every project has a reasonable number of members
    - Distribution follows realistic patterns
    """
    
    # Initialize tracking
    member_assignments = defaultdict(set)  # member_id -> set of project_ids
    project_assignments = defaultdict(set)  # project_id -> set of member_ids
    
    member_ids = [m["member"]["id"] for m in members]
    project_ids = [p["id"] for p in projects]
    
    print(f"    📊 Creating assignments for {len(member_ids)} members and {len(project_ids)} projects")
    
    # Phase 1: Ensure every member gets minimum projects
    for member in members:
        member_id = member["member"]["id"]
        
        # Determine how many projects this member should be in
        target_projects = random.randint(MIN_PROJECTS_PER_MEMBER, MAX_PROJECTS_PER_MEMBER)
        target_projects = min(target_projects, len(project_ids))  # Can't exceed total projects
        
        # Randomly select projects for this member
        available_projects = [p for p in project_ids 
                            if len(project_assignments[p]) < MAX_MEMBERS_PER_PROJECT]
        
        if len(available_projects) < target_projects:
            available_projects = project_ids  # Fallback to all projects if needed
        
        selected_projects = random.sample(available_projects, 
                                        min(target_projects, len(available_projects)))
        
        for project_id in selected_projects:
            member_assignments[member_id].add(project_id)
            project_assignments[project_id].add(member_id)
    
    # Phase 2: Ensure every project has minimum members
    for project in projects:
        project_id = project["id"]
        current_members = len(project_assignments[project_id])
        
        if current_members < MIN_MEMBERS_PER_PROJECT:
            # Need to add more members to this project
            needed = MIN_MEMBERS_PER_PROJECT - current_members
            
            # Find members who aren't already in this project and aren't at max capacity
            available_members = []
            for member in members:
                member_id = member["member"]["id"]
                if (member_id not in project_assignments[project_id] and 
                    len(member_assignments[member_id]) < MAX_PROJECTS_PER_MEMBER):
                    available_members.append(member_id)
            
            # If not enough available members, relax the max constraint
            if len(available_members) < needed:
                available_members = [m["member"]["id"] for m in members 
                                   if m["member"]["id"] not in project_assignments[project_id]]
            
            # Add random members to reach minimum
            if available_members:
                additional_members = random.sample(available_members, 
                                                 min(needed, len(available_members)))
                
                for member_id in additional_members:
                    member_assignments[member_id].add(project_id)
                    project_assignments[project_id].add(member_id)
    
    return member_assignments, project_assignments

def add_members_to_project(session, slug, project_id, project_members, all_members):
    """Add specific members to a project"""
    url = f"{BASE_URL}/workspaces/{slug}/projects/{project_id}/members/"
    
    # Find member objects for the assigned member IDs
    payload_members = []
    for member in all_members:
        member_id = member["member"]["id"]
        if member_id in project_members:
            workspace_role = member.get("role", ROLE)
            project_role = workspace_role
            
            payload_members.append({
                "role": project_role,
                "member_id": member_id
            })

    if not payload_members:
        print(f"      ⚠️ No members to add for project {project_id}")
        return False

    try:
        payload = {"members": payload_members}
        resp = session.post(url, json=payload)
        if resp.status_code not in [200, 201]:
            print(f"      ❌ Failed to add members to project {project_id}: {resp.status_code} - {resp.text[:100]}")
            return False
        
        print(f"      ✅ Added {len(payload_members)} members to project")
        return True
    except Exception as e:
        print(f"      ❌ Error adding members to project {project_id}: {e}")
        return False

def print_assignment_summary(member_assignments, project_assignments, members, projects):
    """Print a summary of the assignments"""
    print(f"    📈 Assignment Summary:")
    
    # Member statistics
    if member_assignments:
        member_project_counts = [len(assignments) for assignments in member_assignments.values()]
        print(f"    👥 Member Statistics:")
        print(f"       - Average projects per member: {sum(member_project_counts)/len(member_project_counts):.1f}")
        print(f"       - Min projects per member: {min(member_project_counts)}")
        print(f"       - Max projects per member: {max(member_project_counts)}")
    
    # Project statistics
    if project_assignments:
        project_member_counts = [len(assignments) for assignments in project_assignments.values()]
        print(f"    📌 Project Statistics:")
        print(f"       - Average members per project: {sum(project_member_counts)/len(project_member_counts):.1f}")
        print(f"       - Min members per project: {min(project_member_counts)}")
        print(f"       - Max members per project: {max(project_member_counts)}")
    
    # Coverage check
    unassigned_members = len([m for m in members if m["member"]["id"] not in member_assignments])
    empty_projects = len([p for p in projects if p["id"] not in project_assignments])
    
    print(f"    ✅ Coverage:")
    print(f"       - Members with assignments: {len(members) - unassigned_members}/{len(members)}")
    print(f"       - Projects with members: {len(projects) - empty_projects}/{len(projects)}")

def assign_users_to_projects():
    """Assign users to projects with realistic distributions"""
    print("\n🔧 Starting user assignment phase...")
    
    session = get_authenticated_session()
    workspaces = get_workspaces_from_file()
    
    if not workspaces:
        print("❌ No workspaces found.")
        return False

    total_assignments = 0
    successful_workspaces = 0

    for i, slug in enumerate(workspaces, 1):
        print(f"\n📂 [{i}/{len(workspaces)}] Processing workspace: {slug}")

        # Get members and projects
        members = get_workspace_members(session, slug)
        projects = get_workspace_projects(session, slug)
        
        print(f"    👥 Found {len(members)} members")
        print(f"    📌 Found {len(projects)} projects")

        if len(members) == 0:
            print("    ⚠️ Skipping workspace with no members")
            continue
            
        if len(projects) == 0:
            print("    ⚠️ Skipping workspace with no projects")
            continue

        # Create realistic assignments
        member_assignments, project_assignments = create_realistic_assignments(members, projects)
        
        # Print summary before applying
        print_assignment_summary(member_assignments, project_assignments, members, projects)
        
        # Apply assignments to each project
        print(f"    🔄 Applying assignments...")
        successful_projects = 0
        
        for project in projects:
            project_id = project["id"]
            project_name = project.get("name", str(project_id))
            project_members = project_assignments[project_id]
            
            if project_members:
                print(f"      📌 Assigning {len(project_members)} members to '{project_name}'")
                if add_members_to_project(session, slug, project_id, project_members, members):
                    successful_projects += 1
                    total_assignments += len(project_members)
            else:
                print(f"      ⚠️ No members assigned to '{project_name}'")
            
            time.sleep(0.3)  # Rate limiting

        print(f"    ✅ Completed assignments for workspace {slug}")
        print(f"       - Successfully assigned: {successful_projects}/{len(projects)} projects")
        successful_workspaces += 1

    print(f"\n📊 User assignment complete:")
    print(f"   - Workspaces processed: {successful_workspaces}/{len(workspaces)}")
    print(f"   - Total member-project assignments: {total_assignments}")
    
    return successful_workspaces > 0

# =========================
# MAIN FUNCTION
# =========================
def main():
    """Main function that orchestrates project creation and user assignment"""
    print("🚀 Starting combined project creation and user assignment...")
    start_time = time.time()
    
    # Get authenticated session once
    session = get_authenticated_session()
    if not session:
        print("❌ Failed to authenticate session.")
        return
    
    print("✅ Authenticated successfully.")
    
    # Phase 1: Create projects
    projects_available = create_projects(session)
    
    if not projects_available:
        print("❌ No projects were created or available. Stopping.")
        return
    
    # Small delay between phases
    print("\n⏳ Waiting 3 seconds before starting user assignment...")
    time.sleep(3)
    
    # Phase 2: Assign users to projects
    assignment_success = assign_users_to_projects()
    
    # Overall summary
    total_elapsed = time.time() - start_time
    print(f"\n🎊 Complete! Total time elapsed: {total_elapsed:.1f} seconds")
    
    if assignment_success:
        print("✅ Both project creation and user assignment phases completed successfully!")
    else:
        print("⚠️ Project creation completed, but user assignment had issues.")

if __name__ == "__main__":
    main()