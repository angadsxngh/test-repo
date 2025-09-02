#!/usr/bin/env python3
import os
import sys
import json
import random
import time
from pathlib import Path
from dotenv import load_dotenv
import requests

# Add backfill folder to path so we can import your auth module
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../backfill")))
from auth import get_authenticated_session

load_dotenv()

# =========================
# CONFIG
# =========================
BASE_URL = "http://localhost:8000/api"

# Assignment constraints
MIN_ISSUES_PER_MODULE = 1  # Each module should have at least 1 issue
MAX_ISSUES_PER_MODULE = 3  # Each module should have at most 3 issues

# Rate limiting
API_RATE_LIMIT = 5  # requests per second
last_api_call = {"time": 0}

# =========================
# HELPERS
# =========================
def rate_limit_api():
    """Ensure we don't exceed API rate limits"""
    now = time.time()
    time_since_last = now - last_api_call["time"]
    min_interval = 1.0 / API_RATE_LIMIT
    
    if time_since_last < min_interval:
        sleep_time = min_interval - time_since_last
        time.sleep(sleep_time)
    
    last_api_call["time"] = time.time()

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

# =========================
# API FUNCTIONS
# =========================
def get_workspaces(session):
    """Get all workspaces"""
    rate_limit_api()
    resp = session.get(f"{BASE_URL}/users/me/workspaces/")
    resp.raise_for_status()
    return resp.json()

def get_projects(session, workspace_slug):
    """Get all projects in a workspace"""
    rate_limit_api()
    resp = session.get(f"{BASE_URL}/workspaces/{workspace_slug}/projects/")
    resp.raise_for_status()
    return resp.json()

def get_project_issues(session, workspace_slug, project_id):
    """Get all issues in a project"""
    rate_limit_api()
    resp = session.get(f"{BASE_URL}/workspaces/{workspace_slug}/projects/{project_id}/issues/")
    if resp.status_code != 200:
        print(f"    ⚠️ Could not fetch issues for project {project_id}: {resp.status_code}")
        return []
    
    data = try_get_json(resp)
    return data if data else []

def get_project_modules(session, workspace_slug, project_id):
    """Get all modules in a project"""
    rate_limit_api()
    resp = session.get(f"{BASE_URL}/workspaces/{workspace_slug}/projects/{project_id}/modules/")
    if resp.status_code != 200:
        print(f"    ⚠️ Could not fetch modules for project {project_id}: {resp.status_code}")
        return []
    
    data = try_get_json(resp)
    return data if data else []

def assign_modules_to_issue(session, workspace_slug, project_id, issue_id, module_ids):
    """Assign modules to an issue"""
    url = f"{BASE_URL}/workspaces/{workspace_slug}/projects/{project_id}/issues/{issue_id}/modules/"
    
    payload = {
        "modules": module_ids,
        "removed_modules": []
    }
    
    headers = {
        "Content-Type": "application/json",
        "X-CSRFToken": session.cookies.get("csrftoken", ""),
        "Referer": f"http://localhost:8000/workspaces/{workspace_slug}/projects/{project_id}/"
    }
    
    rate_limit_api()
    resp = session.post(url, json=payload, headers=headers)
    
    if resp.status_code in [200, 201]:
        return True
    else:
        print(f"      ❌ Failed to assign modules to issue {issue_id}: {resp.status_code} - {resp.text}")
        return False

def create_issue_module_assignments(issues, modules):
    """
    Create realistic assignments where each module gets 1-3 issues
    and issues can be assigned to multiple modules
    """
    assignments = {}  # module_id -> [issue_ids]
    issue_assignments = {}  # issue_id -> [module_ids]
    
    if not issues or not modules:
        return assignments, issue_assignments
    
    print(f"    📊 Creating assignments for {len(issues)} issues and {len(modules)} modules")
    
    # Initialize
    for module in modules:
        module_id = module.get("id")
        if module_id:
            assignments[module_id] = []
    
    # First, ensure each module gets at least one issue
    available_issues = issues.copy()
    random.shuffle(available_issues)
    
    issue_index = 0
    for module in modules:
        module_id = module.get("id")
        if not module_id or issue_index >= len(available_issues):
            continue
            
        # Assign at least one issue to this module
        issue = available_issues[issue_index]
        issue_id = issue.get("id")
        if issue_id:
            assignments[module_id].append(issue_id)
            if issue_id not in issue_assignments:
                issue_assignments[issue_id] = []
            issue_assignments[issue_id].append(module_id)
            issue_index += 1
    
    # Then, randomly assign additional issues to modules (up to max)
    for module in modules:
        module_id = module.get("id")
        if not module_id:
            continue
            
        current_count = len(assignments[module_id])
        if current_count >= MAX_ISSUES_PER_MODULE:
            continue
            
        # Determine how many more issues this module should get
        additional_issues = random.randint(0, MAX_ISSUES_PER_MODULE - current_count)
        
        if additional_issues == 0:
            continue
            
        # Get issues not already assigned to this module
        available_for_module = [
            issue for issue in issues 
            if issue.get("id") and issue.get("id") not in assignments[module_id]
        ]
        
        if not available_for_module:
            continue
            
        # Randomly select additional issues
        num_to_assign = min(additional_issues, len(available_for_module))
        selected_issues = random.sample(available_for_module, num_to_assign)
        
        for issue in selected_issues:
            issue_id = issue.get("id")
            if issue_id:
                assignments[module_id].append(issue_id)
                if issue_id not in issue_assignments:
                    issue_assignments[issue_id] = []
                issue_assignments[issue_id].append(module_id)
    
    return assignments, issue_assignments

# =========================
# MAIN SCRIPT
# =========================
def main():
    session = get_authenticated_session()
    print("✅ Authenticated successfully.")
    
    # Get all workspaces
    workspaces = get_workspaces(session)
    print(f"📂 Found {len(workspaces)} workspaces")
    
    total_assignments = 0
    total_issues_processed = 0
    total_modules_processed = 0
    
    for workspace in workspaces:
        workspace_slug = workspace["slug"]
        print(f"\n📂 Processing workspace: {workspace_slug}")
        
        # Get projects in workspace
        projects = get_projects(session, workspace_slug)
        print(f"  📌 Found {len(projects)} projects")
        
        for project in projects:
            project_id = project["id"]
            project_name = project.get("name", project_id)
            print(f"\n  🔄 Processing project: {project_name}")
            
            # Get issues and modules for this project
            issues = get_project_issues(session, workspace_slug, project_id)
            modules = get_project_modules(session, workspace_slug, project_id)
            
            print(f"    📋 Found {len(issues)} issues and {len(modules)} modules")
            
            if not issues:
                print(f"    ⚠️ No issues found for project {project_name}, skipping")
                continue
                
            if not modules:
                print(f"    ⚠️ No modules found for project {project_name}, skipping")
                continue
            
            # Create assignments
            module_assignments, issue_assignments = create_issue_module_assignments(issues, modules)
            
            if not issue_assignments:
                print(f"    ⚠️ No assignments created for project {project_name}")
                continue
            
            # Apply assignments (assign modules to each issue)
            print(f"    🔄 Applying assignments...")
            project_assignments = 0
            
            for issue_id, module_ids in issue_assignments.items():
                if not module_ids:
                    continue
                
                issue_name = next((i.get("name", issue_id) for i in issues if i.get("id") == issue_id), issue_id)
                module_names = [next((m.get("name", mid) for m in modules if m.get("id") == mid), mid) for mid in module_ids]
                
                print(f"      🔄 Assigning {len(module_ids)} modules to issue '{issue_name}'")
                print(f"         Modules: {', '.join(module_names)}")
                
                if assign_modules_to_issue(session, workspace_slug, project_id, issue_id, module_ids):
                    project_assignments += 1
                    print(f"      ✅ Successfully assigned modules to issue '{issue_name}'")
                else:
                    print(f"      ❌ Failed to assign modules to issue '{issue_name}'")
                
                # Small delay to be respectful
                time.sleep(0.1)
            
            total_assignments += project_assignments
            total_issues_processed += len(issues)
            total_modules_processed += len(modules)
            
            print(f"    ✅ Completed project {project_name}: {project_assignments} issues assigned to modules")
            
            # Show module assignment summary
            print(f"    📊 Module assignment summary:")
            for module_id, assigned_issues in module_assignments.items():
                if assigned_issues:
                    module_name = next((m.get("name", module_id) for m in modules if m.get("id") == module_id), module_id)
                    print(f"       📦 {module_name}: {len(assigned_issues)} issues")
    
    print(f"\n🎉 Assignment Summary:")
    print(f"   📊 Total issues processed: {total_issues_processed}")
    print(f"   📊 Total modules processed: {total_modules_processed}")
    print(f"   📊 Total assignments made: {total_assignments}")
    print(f"   📊 Assignment rate: {total_assignments/total_issues_processed:.1f} assignments per issue" if total_issues_processed > 0 else "")

if __name__ == "__main__":
    main()