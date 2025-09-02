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
MIN_CYCLES_PER_ISSUE = 2  # Each issue should be in at least 2 cycles
MAX_CYCLES_PER_ISSUE = 4  # Each issue should be in at most 4 cycles

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

def get_project_cycles(session, workspace_slug, project_id):
    """Get all cycles in a project"""
    rate_limit_api()
    resp = session.get(f"{BASE_URL}/workspaces/{workspace_slug}/projects/{project_id}/cycles/")
    if resp.status_code != 200:
        print(f"    ⚠️ Could not fetch cycles for project {project_id}: {resp.status_code}")
        return []
    
    data = try_get_json(resp)
    return data if data else []

def assign_issue_to_cycle(session, workspace_slug, project_id, cycle_id, issue_id):
    """Assign an issue to a cycle"""
    url = f"{BASE_URL}/workspaces/{workspace_slug}/projects/{project_id}/cycles/{cycle_id}/cycle-issues/"
    
    payload = {
        "issues": [issue_id]
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
        print(f"      ❌ Failed to assign issue {issue_id} to cycle {cycle_id}: {resp.status_code} - {resp.text}")
        return False

def create_issue_cycle_assignments(issues, cycles):
    """
    Create realistic assignments where each issue is assigned to 2-4 cycles
    and cycles get a reasonable distribution of issues
    """
    assignments = {}  # cycle_id -> [issue_ids]
    issue_assignments = {}  # issue_id -> [cycle_ids]
    
    if not issues or not cycles:
        return assignments
    
    print(f"    📊 Creating assignments for {len(issues)} issues and {len(cycles)} cycles")
    
    # Initialize
    for cycle in cycles:
        cycle_id = cycle.get("id")
        if cycle_id:
            assignments[cycle_id] = []
    
    # Assign each issue to random cycles
    for issue in issues:
        issue_id = issue.get("id")
        if not issue_id:
            continue
            
        # Determine how many cycles this issue should be in
        num_cycles = random.randint(MIN_CYCLES_PER_ISSUE, MAX_CYCLES_PER_ISSUE)
        num_cycles = min(num_cycles, len(cycles))  # Can't exceed available cycles
        
        if num_cycles == 0:
            continue
            
        # Randomly select cycles for this issue
        available_cycles = [c["id"] for c in cycles if c.get("id")]
        selected_cycles = random.sample(available_cycles, num_cycles)
        
        # Add to assignments
        for cycle_id in selected_cycles:
            assignments[cycle_id].append(issue_id)
        
        issue_assignments[issue_id] = selected_cycles
    
    return assignments

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
    total_cycles_processed = 0
    
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
            
            # Get issues and cycles for this project
            issues = get_project_issues(session, workspace_slug, project_id)
            cycles = get_project_cycles(session, workspace_slug, project_id)
            
            print(f"    📋 Found {len(issues)} issues and {len(cycles)} cycles")
            
            if not issues:
                print(f"    ⚠️ No issues found for project {project_name}, skipping")
                continue
                
            if not cycles:
                print(f"    ⚠️ No cycles found for project {project_name}, skipping")
                continue
            
            if len(cycles) < MIN_CYCLES_PER_ISSUE:
                print(f"    ⚠️ Project {project_name} has fewer than {MIN_CYCLES_PER_ISSUE} cycles, skipping")
                continue
            
            # Create assignments
            assignments = create_issue_cycle_assignments(issues, cycles)
            
            if not assignments:
                print(f"    ⚠️ No assignments created for project {project_name}")
                continue
            
            # Apply assignments
            print(f"    🔄 Applying assignments...")
            project_assignments = 0
            
            for cycle_id, issue_ids in assignments.items():
                if not issue_ids:
                    continue
                    
                cycle_name = next((c.get("name", cycle_id) for c in cycles if c.get("id") == cycle_id), cycle_id)
                print(f"      🔄 Assigning {len(issue_ids)} issues to cycle '{cycle_name}'")
                
                successful_assignments = 0
                for issue_id in issue_ids:
                    if assign_issue_to_cycle(session, workspace_slug, project_id, cycle_id, issue_id):
                        successful_assignments += 1
                        project_assignments += 1
                    
                    # Small delay to be respectful
                    time.sleep(0.1)
                
                print(f"      ✅ Successfully assigned {successful_assignments}/{len(issue_ids)} issues to cycle '{cycle_name}'")
            
            total_assignments += project_assignments
            total_issues_processed += len(issues)
            total_cycles_processed += len(cycles)
            
            print(f"    ✅ Completed project {project_name}: {project_assignments} total assignments")
    
    print(f"\n🎉 Assignment Summary:")
    print(f"   📊 Total issues processed: {total_issues_processed}")
    print(f"   📊 Total cycles processed: {total_cycles_processed}")
    print(f"   📊 Total assignments made: {total_assignments}")
    print(f"   📊 Average assignments per issue: {total_assignments/total_issues_processed:.1f}" if total_issues_processed > 0 else "")

if __name__ == "__main__":
    main()