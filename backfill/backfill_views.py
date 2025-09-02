#!/usr/bin/env python3
import json
import time
import requests
from pathlib import Path
import sys
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Optional

# Import authentication helper
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../backfill")))
from auth import get_authenticated_session

# ------------------------
# Config
# ------------------------
BASE_URL = "http://localhost:8000/api"
VIEWS_FILE = Path("../generated_json/views.json")

# Threading configuration
MAX_WORKERS = 6  # Number of concurrent threads
API_RATE_LIMIT = 8  # requests per second
BATCH_SIZE = 25  # Process views in batches for progress reporting

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

def resolve_view_data(view_data):
    """Convert view data with project name/identifier to actual UUIDs"""
    project_mapping = build_project_mapping()
    
    # Try to find project by name first, then identifier
    project_name = view_data.get("project_name", "")
    project_identifier = view_data.get("project_identifier", "")
    
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
    
    # Build the resolved view payload (remove mapping fields, add project_id)
    resolved_view = view_data.copy()
    
    # Remove fields that are only needed for mapping
    resolved_view.pop("project_name", None)
    resolved_view.pop("project_identifier", None)
    
    # Add the actual project_id
    resolved_view["project_id"] = project_id
    
    return workspace_slug, resolved_view

# ------------------------
# View backfill functions
# ------------------------
def prepare_view_payload(view):
    """Prepare view payload by removing backfill-specific fields"""
    # Create a copy and remove fields that shouldn't be sent to API
    payload = view.copy()
    
    # Remove fields that are only needed for backfilling
    payload.pop("workspace_slug", None)
    
    return payload

def backfill_view(view_data: tuple) -> tuple:
    """Backfill a single view (runs in thread)"""
    view, index, total = view_data
    view_name = view.get("name", "Unknown")
    
    try:
        # Resolve the view data (convert project name/identifier to UUIDs)
        workspace_slug, resolved_view = resolve_view_data(view)
        project_id = resolved_view["project_id"]
        
        # Create new session for this thread
        session = get_thread_session()
        url = f"{BASE_URL}/workspaces/{workspace_slug}/projects/{project_id}/views/"
        
        # Prepare payload
        payload = prepare_view_payload(resolved_view)

        rate_limit_api()
        
        headers = {
            "Content-Type": "application/json",
            "X-CSRFToken": session.cookies.get("csrftoken", ""),
            "Referer": f"http://localhost:8000/workspaces/{workspace_slug}/projects/{project_id}/"
        }
        
        resp = session.post(url, json=payload, headers=headers)
        
        if resp.status_code in (200, 201):
            update_counters(success=True)
            return (True, index, view_name, "Success")
        else:
            error_msg = f"HTTP {resp.status_code}: {resp.text[:200]}"
            update_counters(error=True)
            return (False, index, view_name, error_msg)
            
    except Exception as e:
        error_msg = f"Exception: {str(e)[:200]}"
        update_counters(error=True)
        return (False, index, view_name, error_msg)

def process_views_batch(views_batch: list, batch_num: int, total_batches: int):
    """Process a batch of views with multithreading"""
    print(f"\n🔄 Processing batch {batch_num}/{total_batches} ({len(views_batch)} views)")
    
    # Prepare view data with indices
    view_data = [(view, i + ((batch_num - 1) * BATCH_SIZE), len(views_batch)) 
                 for i, view in enumerate(views_batch, 1)]
    
    batch_success = 0
    batch_errors = 0
    
    # Process batch with threading
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all view tasks
        future_to_view = {
            executor.submit(backfill_view, data): data[0]["name"] 
            for data in view_data
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_view):
            view_name = future_to_view[future]
            try:
                success, index, name, message = future.result()
                
                if success:
                    batch_success += 1
                    print(f"  ✅ [{index}] Created view: {name}")
                else:
                    batch_errors += 1
                    print(f"  ❌ [{index}] Failed for {name}: {message}")
                    
            except Exception as e:
                batch_errors += 1
                print(f"  ❌ Error processing view {view_name}: {e}")
    
    # Batch summary
    total_success, total_errors = get_counts()
    print(f"📊 Batch {batch_num} complete: {batch_success} success, {batch_errors} errors")
    print(f"📈 Overall progress: {total_success} success, {total_errors} errors, {total_success + total_errors} total")

def validate_views(views):
    """Validate views have required fields"""
    valid_views = []
    invalid_count = 0
    
    for i, view in enumerate(views):
        if not isinstance(view, dict):
            print(f"⚠️ View {i+1} is not a dictionary, skipping")
            invalid_count += 1
            continue
            
        required_fields = ["name", "project_name", "workspace_slug"]
        missing_fields = [field for field in required_fields if not view.get(field)]
        
        if missing_fields:
            print(f"⚠️ View {i+1} missing required fields: {missing_fields}, skipping")
            invalid_count += 1
            continue
            
        valid_views.append(view)
    
    if invalid_count > 0:
        print(f"⚠️ Skipped {invalid_count} invalid views")
    
    return valid_views

# ------------------------
# Main
# ------------------------
def main():
    print("🚀 Starting multithreaded view backfill...")
    
    # Check if views file exists
    if not VIEWS_FILE.exists():
        raise SystemExit(f"❌ File not found: {VIEWS_FILE}")

    # Load views
    with VIEWS_FILE.open("r", encoding="utf-8") as f:
        views = json.load(f)

    print(f"📄 Found {len(views)} views to backfill.")
    
    if not views:
        print("ℹ️ No views to process.")
        return

    # Validate views
    valid_views = validate_views(views)
    if not valid_views:
        print("❌ No valid views found. Cannot proceed.")
        return
    
    if len(valid_views) != len(views):
        print(f"📊 Processing {len(valid_views)} valid views out of {len(views)} total")

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

    # Show configuration
    print(f"\n⚙️ Configuration:")
    print(f"   - Max workers: {MAX_WORKERS}")
    print(f"   - API rate limit: {API_RATE_LIMIT} req/sec")
    print(f"   - Batch size: {BATCH_SIZE}")
    print(f"   - Total views: {len(valid_views)}")

    # Process in batches
    start_time = time.time()
    total_batches = (len(valid_views) + BATCH_SIZE - 1) // BATCH_SIZE
    
    for batch_num in range(1, total_batches + 1):
        start_idx = (batch_num - 1) * BATCH_SIZE
        end_idx = min(start_idx + BATCH_SIZE, len(valid_views))
        batch_views = valid_views[start_idx:end_idx]
        
        process_views_batch(batch_views, batch_num, total_batches)
        
        # Small delay between batches to be respectful
        if batch_num < total_batches:
            time.sleep(1)

    # Final summary
    elapsed_time = time.time() - start_time
    final_success, final_errors = get_counts()
    total_processed = final_success + final_errors
    
    print(f"\n🎉 View backfill complete!")
    print(f"📊 Final Statistics:")
    print(f"   - Total processed: {total_processed}/{len(valid_views)}")
    print(f"   - Successful: {final_success} ({final_success/total_processed*100:.1f}%)" if total_processed > 0 else "")
    print(f"   - Failed: {final_errors} ({final_errors/total_processed*100:.1f}%)" if total_processed > 0 else "")
    print(f"   - Time elapsed: {elapsed_time:.1f} seconds")
    print(f"   - Average rate: {total_processed/elapsed_time:.1f} views/second" if elapsed_time > 0 else "")
    
    # Show breakdown by grouping
    if final_success > 0:
        grouping_counts = {}
        for view in valid_views:
            group_by = view.get("display_filters", {}).get("group_by", "unknown")
            grouping_counts[group_by] = grouping_counts.get(group_by, 0) + 1
        
        print(f"\n📈 Views created by grouping:")
        for group_by, count in sorted(grouping_counts.items()):
            print(f"   - {group_by}: {count} views")
    
    if final_errors > 0:
        print(f"\n⚠️ {final_errors} views failed to backfill. Check the error messages above.")
    else:
        print(f"\n✅ All views backfilled successfully!")

if __name__ == "__main__":
    main()