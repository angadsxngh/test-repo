import os
import json
from pathlib import Path
from dotenv import load_dotenv
import requests
import sys

# Make sure we can import from ../backfill
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../backfill")))
from auth import get_authenticated_session

# =========================
# CONFIG
# =========================
BASE_URL = "http://localhost:8000/api"
CYCLES_FILE = Path("../generated_json/cycles.json")

load_dotenv()

# Cache for API data to avoid repeated requests
_cache = {
    "workspaces": None,
    "projects": {},
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

def resolve_cycle_data(cycle_data):
    """Convert cycle data with project name/identifier to actual UUIDs"""
    project_mapping = build_project_mapping()
    
    # Try to find project by name first, then identifier
    project_name = cycle_data.get("project_name", "")
    project_identifier = cycle_data.get("project_identifier", "")
    
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
    
    # Build the resolved cycle payload
    resolved_cycle = {
        "name": cycle_data["name"],
        "description": cycle_data["description"],
        "project_id": project_id
    }
    
    return workspace_slug, resolved_cycle

def create_cycle(workspace_slug, cycle_data):
    """
    Create a cycle in Plane using the provided workspace_slug and resolved cycle data.
    """
    project_id = cycle_data["project_id"]
    url = f"{BASE_URL}/workspaces/{workspace_slug}/projects/{project_id}/cycles/"
    
    session = get_authenticated_session()
    r = session.post(url, json=cycle_data)
    
    if r.status_code == 201:
        print(f"✅ Created cycle '{cycle_data['name']}'")
        return True
    else:
        print(f"❌ Failed to create cycle '{cycle_data['name']}'")
        print(f"   Status: {r.status_code}")
        print(f"   Response: {r.text[:200]}...")
        return False

def main():
    print("🚀 Starting cycle backfill...")
    
    if not CYCLES_FILE.exists():
        print(f"❌ cycles.json not found at {CYCLES_FILE}")
        return
    
    with open(CYCLES_FILE, "r", encoding="utf-8") as f:
        cycles = json.load(f)

    print(f"📦 Found {len(cycles)} cycles to backfill.")
    
    if not cycles:
        print("ℹ️ No cycles to process.")
        return

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

    print(f"\n🚀 Processing {len(cycles)} cycles...")
    
    success_count = 0
    failed_count = 0

    for i, cycle in enumerate(cycles, 1):
        print(f"\n🔄 Processing cycle {i}/{len(cycles)}: {cycle.get('name', 'Unnamed')}")
        
        try:
            # Resolve the cycle data (convert project name/identifier to UUIDs)
            workspace_slug, resolved_cycle = resolve_cycle_data(cycle)
            
            # Create the cycle
            if create_cycle(workspace_slug, resolved_cycle):
                success_count += 1
            else:
                failed_count += 1
                
        except Exception as e:
            print(f"❌ Error processing cycle {i}: {e}")
            failed_count += 1

    print(f"\n🎉 Cycle backfill complete!")
    print(f"📊 Final Statistics:")
    print(f"   - Total processed: {success_count + failed_count}/{len(cycles)}")
    print(f"   - Successful: {success_count}")
    print(f"   - Failed: {failed_count}")
    
    if failed_count > 0:
        print(f"\n⚠️ {failed_count} cycles failed to backfill. Check the error messages above.")
    else:
        print(f"\n✅ All cycles backfilled successfully!")

if __name__ == "__main__":
    main()