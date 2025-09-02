#WORKING CODE FOR REFERENCE
import os
import json
import random
from pathlib import Path
import sys
from dotenv import load_dotenv

# Add backfill folder to path for your auth module
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../backfill")))
from auth import get_authenticated_session  # your existing auth

# =========================
# CONFIG
# =========================
BASE_URL = "http://localhost:8000/api"
ISSUES_FILE = Path("../generated_json/issues.json")

load_dotenv()

# Cache for API data to avoid repeated requests
_cache = {
    "workspaces": None,
    "projects": {},
    "members": {},
    "cycles": {},
    "states": {},
    "modules": {},
    "project_mapping": None
}

def get_workspaces():
    """Get all workspaces and cache them"""
    if _cache["workspaces"] is None:
        session = get_authenticated_session()
        r = session.get(f"{BASE_URL}/users/me/workspaces/")
        r.raise_for_status()
        _cache["workspaces"] = r.json()
    return _cache["workspaces"]

def get_projects_for_workspace(workspace_slug):
    """Get all projects for a workspace and cache them"""
    if workspace_slug not in _cache["projects"]:
        session = get_authenticated_session()
        r = session.get(f"{BASE_URL}/workspaces/{workspace_slug}/projects/")
        r.raise_for_status()
        _cache["projects"][workspace_slug] = r.json()
    return _cache["projects"][workspace_slug]

def build_project_mapping():
    """Build mapping from project names/identifiers to actual project info"""
    if _cache["project_mapping"] is not None:
        return _cache["project_mapping"]
    
    # Get all real projects
    workspaces = get_workspaces()
    mapping = {}
    
    for workspace in workspaces:
        ws_slug = workspace["slug"]
        projects = get_projects_for_workspace(ws_slug)
        for project in projects:
            project_name = project.get("name", "")
            project_identifier = project.get("identifier", "")
            
            # Map by both name and identifier
            mapping[project_name] = (ws_slug, project)
            mapping[project_identifier] = (ws_slug, project)
    
    _cache["project_mapping"] = mapping
    print(f"✅ Built mapping for {len(mapping)} project references")
    return mapping

def resolve_issue_data(issue_data):
    """Convert issue data with project name/identifier to actual UUIDs"""
    project_mapping = build_project_mapping()
    
    # Try to find project by name first, then identifier
    project_name = issue_data.get("project_name", "")
    project_identifier = issue_data.get("project_identifier", "")
    
    workspace_slug = None
    project = None
    
    # Try exact name match first
    if project_name in project_mapping:
        workspace_slug, project = project_mapping[project_name]
        print(f"✅ Found project by name: '{project_name}' -> {project.get('identifier', 'Unknown')}")
    # Try identifier match
    elif project_identifier in project_mapping:
        workspace_slug, project = project_mapping[project_identifier]
        print(f"✅ Found project by identifier: '{project_identifier}' -> {project.get('name', 'Unknown')}")
    else:
        # Fallback: pick the first available project
        if project_mapping:
            workspace_slug, project = list(project_mapping.values())[0]
            print(f"🎯 Fallback: using first available project -> {project.get('identifier', 'Unknown')}")
        else:
            raise ValueError("No projects found in your system")
    
    project_id = project["id"]
    
    # Get all necessary data
    member_ids = get_project_members(workspace_slug, project_id)
    cycle_ids = get_project_cycles(workspace_slug, project_id)
    state_ids = get_project_states(workspace_slug, project_id)
    module_ids = get_project_modules(workspace_slug, project_id)
    
    # Build the resolved issue payload
    resolved_issue = {
        "project_id": project_id,
        "type_id": issue_data.get("type_id"),
        "name": issue_data["name"],
        "description_html": issue_data["description_html"],
        "assignee_ids": [],
        "cycle_id": None,
        "estimate_point": issue_data.get("estimate_point"),
        "label_ids": issue_data.get("label_ids", []),
        "module_ids": None,
        "parent_id": issue_data.get("parent_id"),
        "priority": issue_data["priority"],
        "start_date": issue_data.get("start_date"),
        "state_id": None,
        "target_date": issue_data.get("target_date"),
    }
    
    # Resolve assignees
    assignee_count = issue_data.get("assignee_count", 1)
    if member_ids and assignee_count > 0:
        actual_count = min(assignee_count, len(member_ids))
        resolved_issue["assignee_ids"] = random.sample(member_ids, actual_count)
    
    # Resolve cycle
    cycle_index = issue_data.get("cycle_index")
    if cycle_index is not None and cycle_ids and cycle_index < len(cycle_ids):
        resolved_issue["cycle_id"] = cycle_ids[cycle_index]
    elif cycle_ids:
        resolved_issue["cycle_id"] = random.choice(cycle_ids)
    
    # Resolve state
    state_index = issue_data.get("state_index")
    if state_index is not None and state_ids and state_index < len(state_ids):
        resolved_issue["state_id"] = state_ids[state_index]
    elif state_ids:
        resolved_issue["state_id"] = random.choice(state_ids)
    
    # Resolve module
    module_index = issue_data.get("module_index")
    if module_index is not None and module_ids and module_index < len(module_ids):
        resolved_issue["module_ids"] = module_ids[module_index]
    elif module_ids:
        resolved_issue["module_ids"] = random.choice(module_ids)
    
    return workspace_slug, resolved_issue

def get_project_members(workspace_slug, project_id):
    """Get project members and cache them"""
    cache_key = f"{workspace_slug}/{project_id}"
    if cache_key not in _cache["members"]:
        session = get_authenticated_session()
        r = session.get(f"{BASE_URL}/workspaces/{workspace_slug}/projects/{project_id}/members/")
        r.raise_for_status()
        members = r.json()
        member_ids = [m.get("member") for m in members if isinstance(m, dict) and m.get("member")]
        _cache["members"][cache_key] = member_ids
    return _cache["members"][cache_key]

def get_project_cycles(workspace_slug, project_id):
    """Get project cycles and cache them"""
    cache_key = f"{workspace_slug}/{project_id}"
    if cache_key not in _cache["cycles"]:
        session = get_authenticated_session()
        r = session.get(f"{BASE_URL}/workspaces/{workspace_slug}/projects/{project_id}/cycles/")
        if r.status_code == 200:
            cycles = r.json()
            cycle_ids = [c.get("id") for c in cycles if isinstance(c, dict) and c.get("id")]
            _cache["cycles"][cache_key] = cycle_ids
        else:
            _cache["cycles"][cache_key] = []
    return _cache["cycles"][cache_key]

def get_project_states(workspace_slug, project_id):
    """Get project states and cache them"""
    cache_key = f"{workspace_slug}/{project_id}"
    if cache_key not in _cache["states"]:
        session = get_authenticated_session()
        state_ids = []
        
        # Try multiple endpoints to find states
        endpoints = [
            f"{BASE_URL}/workspaces/{workspace_slug}/projects/{project_id}/states/",
            f"{BASE_URL}/workspaces/{workspace_slug}/projects/{project_id}/columns/",
            f"{BASE_URL}/projects/{project_id}/states/",
            f"{BASE_URL}/projects/{project_id}/columns/",
            f"{BASE_URL}/workspaces/{workspace_slug}/states/",
            f"{BASE_URL}/states/",
        ]
        
        for endpoint in endpoints:
            try:
                r = session.get(endpoint, timeout=10)
                if r.status_code != 200:
                    continue
                
                items = r.json()
                # Handle paginated response
                if isinstance(items, dict) and "results" in items:
                    items = items["results"]
                if not isinstance(items, list):
                    continue
                
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    
                    item_id = item.get("id")
                    if not item_id:
                        continue
                    
                    # Check if state belongs to this project
                    proj_field = item.get("project") or item.get("project_id") or item.get("projectId")
                    if proj_field and str(proj_field) != str(project_id):
                        continue
                    
                    state_ids.append(item_id)
                
                if state_ids:
                    break
                    
            except Exception:
                continue
        
        _cache["states"][cache_key] = state_ids
    return _cache["states"][cache_key]

def get_project_modules(workspace_slug, project_id):
    """Get project modules and cache them"""
    cache_key = f"{workspace_slug}/{project_id}"
    if cache_key not in _cache["modules"]:
        session = get_authenticated_session()
        r = session.get(f"{BASE_URL}/workspaces/{workspace_slug}/projects/{project_id}/modules/")
        if r.status_code == 200:
            modules = r.json()
            # Handle paginated response
            if isinstance(modules, dict) and "results" in modules:
                modules = modules["results"]
            module_ids = [m.get("id") for m in modules if isinstance(m, dict) and m.get("id")]
            _cache["modules"][cache_key] = module_ids
        else:
            _cache["modules"][cache_key] = []
    return _cache["modules"][cache_key]

def create_issue(workspace_slug, issue_data):
    """POST a single issue to the API."""
    project_id = issue_data["project_id"]
    url = f"{BASE_URL}/workspaces/{workspace_slug}/projects/{project_id}/issues/"
    
    session = get_authenticated_session()
    r = session.post(url, json=issue_data)
    if r.status_code == 201:
        print(f"✅ Created issue: {issue_data['name']}")
        return True
    else:
        print(f"❌ Failed to create issue: {issue_data['name']}")
        print(f"   Status: {r.status_code}")
        print(f"   Response: {r.text[:200]}...")
        return False

def main():
    if not ISSUES_FILE.exists():
        print(f"❌ Issues file not found: {ISSUES_FILE}")
        return
    
    with open(ISSUES_FILE, "r", encoding="utf-8") as f:
        issues = json.load(f)

    print(f"📂 Loaded {len(issues)} issues from {ISSUES_FILE}")
    
    # Show available projects
    print("\n🔍 Discovering available projects...")
    try:
        workspaces = get_workspaces()
        total_projects = 0
        for ws in workspaces:
            projects = get_projects_for_workspace(ws["slug"])
            total_projects += len(projects)
            for proj in projects:
                print(f"    📋 {proj.get('identifier', 'Unknown')} - {proj.get('name', 'Unnamed')}")
        
        if total_projects == 0:
            print("❌ No projects found in your system.")
            return
        
        print(f"Found {total_projects} projects total")
        
    except Exception as e:
        print(f"❌ Error fetching projects: {e}")
        return
    
    print(f"\n🚀 Starting backfill for {len(issues)} issues...")
    
    success_count = 0
    failed_count = 0

    for i, issue in enumerate(issues, 1):
        print(f"\n🔄 Processing issue {i}/{len(issues)}: {issue.get('name', 'Unnamed')}")
        
        try:
            # Resolve the issue data (convert project name/identifier to UUIDs)
            workspace_slug, resolved_issue = resolve_issue_data(issue)
            
            # Create the issue
            if create_issue(workspace_slug, resolved_issue):
                success_count += 1
            else:
                failed_count += 1
                
        except Exception as e:
            print(f"❌ Error processing issue {i}: {e}")
            failed_count += 1
    
    print(f"\n🎉 Backfill complete!")
    print(f"✅ Successfully created: {success_count} issues")
    print(f"❌ Failed to create: {failed_count} issues")

if __name__ == "__main__":
    main()