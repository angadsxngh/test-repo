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
COMMENTS_FILE = Path("../generated_json/comments.json")
ISSUES_FILE = Path("../generated_json/issues.json")

# Threading configuration
MAX_WORKERS = 6  # Number of concurrent threads
API_RATE_LIMIT = 8  # requests per second
BATCH_SIZE = 25  # Process comments in batches for progress reporting

# Thread-safe variables
api_lock = threading.Lock()
last_api_call = {"time": 0}
results_lock = threading.Lock()
success_count = {"value": 0}
error_count = {"value": 0}

# Cache for mapping
_cache = {
    "workspaces": None,
    "projects": {},
    "issues": {},
    "issue_mapping": None
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

def get_issues_for_project(workspace_slug, project_id):
    """Get all issues for a project and cache them"""
    cache_key = f"{workspace_slug}/{project_id}"
    if cache_key not in _cache["issues"]:
        session = get_thread_session()
        rate_limit_api()
        r = session.get(f"{BASE_URL}/workspaces/{workspace_slug}/projects/{project_id}/issues/")
        if r.status_code == 200:
            issues = try_get_json(r)
            _cache["issues"][cache_key] = issues if issues else []
        else:
            _cache["issues"][cache_key] = []
    return _cache["issues"][cache_key]

def build_issue_mapping():
    """Build mapping from issue names to actual issue info"""
    if _cache["issue_mapping"] is not None:
        return _cache["issue_mapping"]
    
    print("🔍 Building issue name to ID mapping...")
    
    # Get all real issues
    workspaces = get_workspaces()
    issue_mapping = {}  # issue_name -> {"issue_id": ..., "workspace_slug": ..., "project_id": ...}
    total_issues = 0
    
    for workspace in workspaces:
        ws_slug = workspace["slug"]
        projects = get_projects_for_workspace(ws_slug)
        
        for project in projects:
            project_id = project["id"]
            project_name = project.get("name", "")
            
            issues = get_issues_for_project(ws_slug, project_id)
            
            for issue in issues:
                issue_name = issue.get("name", "")
                issue_id = issue.get("id", "")
                
                if issue_name and issue_id:
                    # Use issue name as key (might have duplicates across projects)
                    key = f"{project_name}::{issue_name}"  # Make unique with project name
                    issue_mapping[key] = {
                        "issue_id": issue_id,
                        "workspace_slug": ws_slug,
                        "project_id": project_id,
                        "project_name": project_name
                    }
                    total_issues += 1
            
            print(f"  📌 Mapped {len(issues)} issues from {project_name}")
    
    _cache["issue_mapping"] = issue_mapping
    print(f"✅ Issue mapping complete: {total_issues} issues mapped")
    return issue_mapping

def resolve_comment_data(comment_data):
    """Convert comment data with issue name to actual issue UUID"""
    issue_mapping = build_issue_mapping()
    
    issue_name = comment_data.get("issue_name", "")
    project_slug = comment_data.get("project_slug", "")
    
    # Try to find issue by name
    # First try with project context
    project_name = project_slug.split("/")[-1] if "/" in project_slug else project_slug
    issue_key = f"{project_name}::{issue_name}"
    
    if issue_key in issue_mapping:
        issue_info = issue_mapping[issue_key]
        print(f"✅ Found issue by project context: '{issue_name}' -> {issue_info['issue_id'][:8]}...")
        return issue_info
    
    # Try to find any issue with this name
    for key, issue_info in issue_mapping.items():
        if key.endswith(f"::{issue_name}"):
            print(f"✅ Found issue by name: '{issue_name}' -> {issue_info['issue_id'][:8]}...")
            return issue_info
    
    # No match found
    print(f"⚠️ No issue found for name: '{issue_name}'")
    return None

# ------------------------
# Comment backfill functions
# ------------------------
def backfill_comment(comment_data: tuple) -> tuple:
    """Backfill a single comment (runs in thread)"""
    comment, index, total = comment_data
    issue_name = comment.get("issue_name", "Unknown")
    comment_html = comment.get("comment_html", "")
    
    if not issue_name or not comment_html:
        error_msg = "Missing issue_name or comment_html"
        update_counters(error=True)
        return (False, index, issue_name, error_msg)

    try:
        # Resolve the comment data (convert issue name to UUID)
        issue_info = resolve_comment_data(comment)
        if not issue_info:
            error_msg = "Issue not found in system"
            update_counters(error=True)
            return (False, index, issue_name, error_msg)
        
        issue_id = issue_info["issue_id"]
        workspace_slug = issue_info["workspace_slug"]
        project_id = issue_info["project_id"]
        project_name = issue_info["project_name"]

        # Create new session for this thread
        session = get_thread_session()
        url = f"{BASE_URL}/workspaces/{workspace_slug}/projects/{project_id}/issues/{issue_id}/comments/"
        
        # Prepare payload
        payload = {
            "comment_html": comment_html
        }

        rate_limit_api()
        
        headers = {
            "Content-Type": "application/json",
            "X-CSRFToken": session.cookies.get("csrftoken", ""),
            "Referer": f"http://localhost:8000/workspaces/{workspace_slug}/projects/{project_id}/"
        }
        
        resp = session.post(url, json=payload, headers=headers)
        
        if resp.status_code in (200, 201):
            update_counters(success=True)
            # Extract first few words of comment for display
            clean_comment = comment_html.replace('<p class="editor-paragraph-block">', '').replace('</p>', '')
            preview = clean_comment[:60] + "..." if len(clean_comment) > 60 else clean_comment
            return (True, index, issue_name, f"Success - {project_name}: {preview}")
        else:
            error_msg = f"HTTP {resp.status_code}: {resp.text[:100]}"
            update_counters(error=True)
            return (False, index, issue_name, error_msg)
            
    except Exception as e:
        error_msg = f"Exception: {str(e)[:100]}"
        update_counters(error=True)
        return (False, index, issue_name, error_msg)

def process_comments_batch(comments_batch: list, batch_num: int, total_batches: int):
    """Process a batch of comments with multithreading"""
    print(f"\n🔄 Processing batch {batch_num}/{total_batches} ({len(comments_batch)} comments)")
    
    # Prepare comment data with indices
    comment_data = [(comment, i + ((batch_num - 1) * BATCH_SIZE), len(comments_batch)) 
                    for i, comment in enumerate(comments_batch, 1)]
    
    batch_success = 0
    batch_errors = 0
    
    # Process batch with threading
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all comment tasks
        future_to_comment = {
            executor.submit(backfill_comment, data): data[0].get("issue_name", "unknown")
            for data in comment_data
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_comment):
            issue_name = future_to_comment[future]
            try:
                success, index, comment_issue_name, message = future.result()
                
                if success:
                    batch_success += 1
                    print(f"  ✅ [{index}] {message}")
                else:
                    batch_errors += 1
                    print(f"  ❌ [{index}] Issue '{comment_issue_name}': {message}")
                    
            except Exception as e:
                batch_errors += 1
                print(f"  ❌ Error processing comment for issue {issue_name}: {e}")
    
    # Batch summary
    total_success, total_errors = get_counts()
    print(f"📊 Batch {batch_num} complete: {batch_success} success, {batch_errors} errors")
    print(f"📈 Overall progress: {total_success} success, {total_errors} errors, {total_success + total_errors} total")

def validate_comments(comments):
    """Validate comments have required fields"""
    valid_comments = []
    invalid_count = 0
    
    for i, comment in enumerate(comments):
        if not isinstance(comment, dict):
            print(f"⚠️ Comment {i+1} is not a dictionary, skipping")
            invalid_count += 1
            continue
            
        required_fields = ["issue_name", "comment_html"]
        missing_fields = [field for field in required_fields if not comment.get(field)]
        
        if missing_fields:
            print(f"⚠️ Comment {i+1} missing required fields: {missing_fields}, skipping")
            invalid_count += 1
            continue
            
        valid_comments.append(comment)
    
    if invalid_count > 0:
        print(f"⚠️ Skipped {invalid_count} invalid comments")
    
    return valid_comments

# ------------------------
# Main
# ------------------------
def main():
    print("🚀 Starting multithreaded comment backfill...")
    
    # Check if comments file exists
    if not COMMENTS_FILE.exists():
        raise SystemExit(f"❌ File not found: {COMMENTS_FILE}")

    # Load comments
    with COMMENTS_FILE.open("r", encoding="utf-8") as f:
        comments = json.load(f)

    print(f"💬 Found {len(comments)} comments to backfill.")
    
    if not comments:
        print("ℹ️ No comments to process.")
        return

    # Validate comments
    valid_comments = validate_comments(comments)
    if not valid_comments:
        print("❌ No valid comments found. Cannot proceed.")
        return
    
    if len(valid_comments) != len(comments):
        print(f"📊 Processing {len(valid_comments)} valid comments out of {len(comments)} total")

    # Show available projects for context
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

    # Build issue mapping
    issue_mapping = build_issue_mapping()
    if not issue_mapping:
        print("❌ Failed to build issue mapping. Cannot proceed.")
        return

    # Show configuration
    print(f"\n⚙️ Configuration:")
    print(f"   - Max workers: {MAX_WORKERS}")
    print(f"   - API rate limit: {API_RATE_LIMIT} req/sec")
    print(f"   - Batch size: {BATCH_SIZE}")
    print(f"   - Total comments to process: {len(valid_comments)}")
    print(f"   - Issues mapped: {len(issue_mapping)}")

    # Process in batches
    start_time = time.time()
    total_batches = (len(valid_comments) + BATCH_SIZE - 1) // BATCH_SIZE
    
    for batch_num in range(1, total_batches + 1):
        start_idx = (batch_num - 1) * BATCH_SIZE
        end_idx = min(start_idx + BATCH_SIZE, len(valid_comments))
        batch_comments = valid_comments[start_idx:end_idx]
        
        process_comments_batch(batch_comments, batch_num, total_batches)
        
        # Small delay between batches to be respectful
        if batch_num < total_batches:
            time.sleep(1)

    # Final summary
    elapsed_time = time.time() - start_time
    final_success, final_errors = get_counts()
    total_processed = final_success + final_errors
    
    print(f"\n🎉 Comment backfill complete!")
    print(f"📊 Final Statistics:")
    print(f"   - Total processed: {total_processed}/{len(valid_comments)}")
    print(f"   - Successful: {final_success} ({final_success/total_processed*100:.1f}%)" if total_processed > 0 else "")
    print(f"   - Failed: {final_errors} ({final_errors/total_processed*100:.1f}%)" if total_processed > 0 else "")
    print(f"   - Time elapsed: {elapsed_time:.1f} seconds")
    print(f"   - Average rate: {total_processed/elapsed_time:.1f} comments/second" if elapsed_time > 0 else "")
    
    if final_errors > 0:
        print(f"\n⚠️ {final_errors} comments failed to backfill. Check the error messages above.")
    else:
        print(f"\n✅ All comments backfilled successfully!")

if __name__ == "__main__":
    main()