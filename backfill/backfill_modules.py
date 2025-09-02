import json
import time
import random
from pathlib import Path
import sys
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import authentication helper
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../backfill")))
from auth import get_authenticated_session

# ------------------------
# Config
# ------------------------
BASE_URL = "http://localhost:8000/api"
MODULES_FILE = Path("../generated_json/modules.json")
PROJECTS_FILE = Path("../generated_json/projects.json")

# Threading configuration
MAX_WORKERS = 8
API_RATE_LIMIT = 10
BATCH_SIZE = 50

# Thread-safe variables
api_lock = threading.Lock()
last_api_call = {"time": 0}
results_lock = threading.Lock()
success_count = {"value": 0}
error_count = {"value": 0}

# Cache for API data
_cache = {
    "workspaces": None,
    "projects": {},
    "members": {},
    "project_mapping": None
}

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

def get_thread_session():
    """Get a new authenticated session for this thread"""
    return get_authenticated_session()

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

# ------------------------
# Data fetching functions
# ------------------------
def get_workspaces():
    """Get all workspaces and cache them"""
    if _cache["workspaces"] is None:
        session = get_thread_session()
        rate_limit_api()
        r = session.get(f"{BASE_URL}/users/me/workspaces/")
        r.raise_for_status()
        _cache["workspaces"] = r.json()
    return _cache["workspaces"]

def get_projects_for_workspace(workspace_slug):
    """Get all projects for a workspace and cache them"""
    if workspace_slug not in _cache["projects"]:
        session = get_thread_session()
        rate_limit_api()
        r = session.get(f"{BASE_URL}/workspaces/{workspace_slug}/projects/")
        r.raise_for_status()
        _cache["projects"][workspace_slug] = r.json()
    return _cache["projects"][workspace_slug]

def build_project_mapping():
    """Build mapping from project names to actual project info"""
    if _cache["project_mapping"] is not None:
        return _cache["project_mapping"]
    
    # Get all real projects
    workspaces = get_workspaces()
    name_to_project = {}
    
    for workspace in workspaces:
        ws_slug = workspace["slug"]
        projects = get_projects_for_workspace(ws_slug)
        for project in projects:
            project_name = project.get("name", "")
            name_to_project[project_name] = (ws_slug, project)
    
    _cache["project_mapping"] = name_to_project
    print(f"✅ Built mapping for {len(name_to_project)} real projects")
    return name_to_project

def get_project_members(workspace_slug, project_id):
    """Get project members and cache them"""
    cache_key = f"{workspace_slug}/{project_id}"
    if cache_key not in _cache["members"]:
        session = get_thread_session()
        rate_limit_api()
        r = session.get(f"{BASE_URL}/workspaces/{workspace_slug}/projects/{project_id}/members/")
        r.raise_for_status()
        members = r.json()
        member_ids = [m.get("member") for m in members if isinstance(m, dict) and m.get("member")]
        _cache["members"][cache_key] = member_ids
    return _cache["members"][cache_key]

def resolve_module_data(module_data):
    """Convert module data with placeholders to actual UUIDs"""
    project_name = module_data["project_name"]
    project_mapping = build_project_mapping()
    
    # Find real project by name
    if project_name in project_mapping:
        workspace_slug, project = project_mapping[project_name]
        print(f"✅ Found exact match: '{project_name}' -> {project.get('identifier', 'Unknown')}")
    else:
        # Fallback: pick a random project
        if not project_mapping:
            raise ValueError("No projects found in your system")
        
        workspace_slug, project = random.choice(list(project_mapping.values()))
        print(f"🎯 Random assignment: '{project_name}' -> {project.get('identifier', 'Unknown')}")
    
    project_id = project["id"]
    
    # Get project members
    member_ids = get_project_members(workspace_slug, project_id)
    
    # Build the resolved module payload
    resolved_module = {
        "name": module_data["name"],
        "description": module_data["description"],
        "status": module_data["status"],
        "lead_id": None,
        "member_ids": [],
        "project_id": project_id,
    }
    
    # Resolve lead
    lead_index = module_data.get("lead_index", 0)
    if member_ids and lead_index < len(member_ids):
        resolved_module["lead_id"] = member_ids[lead_index]
    elif member_ids:
        resolved_module["lead_id"] = random.choice(member_ids)
    
    # Resolve members
    member_count = module_data.get("member_count", 1)
    if member_ids and member_count > 0:
        actual_count = min(member_count, len(member_ids))
        resolved_module["member_ids"] = random.sample(member_ids, actual_count)
        
        # Ensure lead is included in members if set
        if resolved_module["lead_id"] and resolved_module["lead_id"] not in resolved_module["member_ids"]:
            if len(resolved_module["member_ids"]) < len(member_ids):
                resolved_module["member_ids"].append(resolved_module["lead_id"])
            else:
                resolved_module["member_ids"][0] = resolved_module["lead_id"]
    
    return workspace_slug, resolved_module

# ------------------------
# Module backfill functions
# ------------------------
def backfill_module(module_data: tuple) -> tuple:
    """Backfill a single module"""
    module, index, total = module_data
    module_name = module.get("name", "Unknown")
    
    try:
        # Resolve the module data (convert placeholders to UUIDs)
        workspace_slug, resolved_module = resolve_module_data(module)
        project_id = resolved_module["project_id"]
        
        # Create new session for this thread
        session = get_thread_session()
        url = f"{BASE_URL}/workspaces/{workspace_slug}/projects/{project_id}/modules/"

        rate_limit_api()
        resp = session.post(url, json=resolved_module)
        
        if resp.status_code in (200, 201):
            update_counters(success=True)
            return (True, index, module_name, "Success")
        else:
            error_msg = f"HTTP {resp.status_code}: {resp.text[:200]}"
            update_counters(error=True)
            return (False, index, module_name, error_msg)
            
    except Exception as e:
        error_msg = f"Exception: {str(e)[:200]}"
        update_counters(error=True)
        return (False, index, module_name, error_msg)

def process_modules_batch(modules_batch: list, batch_num: int, total_batches: int):
    """Process a batch of modules with multithreading"""
    print(f"\n🔄 Processing batch {batch_num}/{total_batches} ({len(modules_batch)} modules)")
    
    module_data = [(module, i + ((batch_num - 1) * BATCH_SIZE), len(modules_batch)) 
                   for i, module in enumerate(modules_batch, 1)]
    
    batch_success = 0
    batch_errors = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_module = {
            executor.submit(backfill_module, data): data[0]["name"] 
            for data in module_data
        }
        
        for future in as_completed(future_to_module):
            module_name = future_to_module[future]
            try:
                success, index, name, message = future.result()
                
                if success:
                    batch_success += 1
                    print(f"  ✅ [{index}] Created module: {name}")
                else:
                    batch_errors += 1
                    print(f"  ❌ [{index}] Failed for {name}: {message}")
                    
            except Exception as e:
                batch_errors += 1
                print(f"  ❌ Error processing module {module_name}: {e}")
    
    total_success, total_errors = get_counts()
    print(f"📊 Batch {batch_num} complete: {batch_success} success, {batch_errors} errors")
    print(f"📈 Overall progress: {total_success} success, {total_errors} errors")

# ------------------------
# Main
# ------------------------
def main():
    print("🚀 Starting multithreaded module backfill...")
    
    if not MODULES_FILE.exists():
        raise SystemExit(f"❌ File not found: {MODULES_FILE}")

    with MODULES_FILE.open("r", encoding="utf-8") as f:
        modules = json.load(f)

    print(f"📦 Found {len(modules)} modules to backfill.")
    
    if not modules:
        print("ℹ️ No modules to process.")
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

    print(f"\n⚙️ Configuration:")
    print(f"   - Max workers: {MAX_WORKERS}")
    print(f"   - API rate limit: {API_RATE_LIMIT} req/sec")
    print(f"   - Batch size: {BATCH_SIZE}")
    print(f"   - Total modules: {len(modules)}")

    # Process in batches
    start_time = time.time()
    total_batches = (len(modules) + BATCH_SIZE - 1) // BATCH_SIZE
    
    for batch_num in range(1, total_batches + 1):
        start_idx = (batch_num - 1) * BATCH_SIZE
        end_idx = min(start_idx + BATCH_SIZE, len(modules))
        batch_modules = modules[start_idx:end_idx]
        
        process_modules_batch(batch_modules, batch_num, total_batches)
        
        if batch_num < total_batches:
            time.sleep(1)

    # Final summary
    elapsed_time = time.time() - start_time
    final_success, final_errors = get_counts()
    total_processed = final_success + final_errors
    
    print(f"\n🎉 Module backfill complete!")
    print(f"📊 Final Statistics:")
    print(f"   - Total processed: {total_processed}/{len(modules)}")
    print(f"   - Successful: {final_success} ({final_success/total_processed*100:.1f}%)" if total_processed > 0 else "")
    print(f"   - Failed: {final_errors} ({final_errors/total_processed*100:.1f}%)" if total_processed > 0 else "")
    print(f"   - Time elapsed: {elapsed_time:.1f} seconds")
    print(f"   - Average rate: {total_processed/elapsed_time:.1f} modules/second" if elapsed_time > 0 else "")

if __name__ == "__main__":
    main()