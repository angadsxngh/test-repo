#!/usr/bin/env python3
import os
import sys
import json
import re
import time
import random
from pathlib import Path
from dotenv import load_dotenv
import anthropic
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Add backfill folder to path so we can import your auth module
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../backfill")))
from auth import get_authenticated_session

load_dotenv()

# =========================
# CONFIG
# =========================
BASE_URL = "http://localhost:8000/api"

CLAUDE_API_KEY = os.getenv("ANTHROPIC_API_KEY")
if not CLAUDE_API_KEY:
    raise SystemExit("❌ Missing ANTHROPIC_API_KEY in .env")

client = anthropic.Anthropic(api_key=CLAUDE_API_KEY)

# Configuration
MAX_WORKERS = 4
CLAUDE_RATE_LIMIT = 2  # requests per second
API_RATE_LIMIT = 8     # requests per second

# Thread-safe locks
claude_lock = threading.Lock()
api_lock = threading.Lock()
last_claude_call = {"time": 0}
last_api_call = {"time": 0}

# Relation types available
RELATION_TYPES = ["relates_to", "blocked_by", "duplicate", "blocks"]

# =========================
# RATE LIMITING
# =========================
def rate_limit_claude():
    """Ensure we don't exceed Claude API rate limits"""
    with claude_lock:
        now = time.time()
        time_since_last = now - last_claude_call["time"]
        min_interval = 1.0 / CLAUDE_RATE_LIMIT
        
        if time_since_last < min_interval:
            sleep_time = min_interval - time_since_last
            time.sleep(sleep_time)
        
        last_claude_call["time"] = time.time()

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

# =========================
# UTILITY FUNCTIONS
# =========================
def clean_json_output(text: str) -> str:
    """Strip code fences and odd leading/trailing text."""
    text = text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    return text.strip()

def safe_json_parse(text: str):
    """Safely parse JSON with fallbacks"""
    try:
        return json.loads(text)
    except Exception:
        # Try to extract JSON object from text
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except Exception:
                pass
        return None

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

def create_issue_relation(session, workspace_slug, project_id, issue_id, related_issue_ids, relation_type):
    """Create relation between issues"""
    url = f"{BASE_URL}/workspaces/{workspace_slug}/projects/{project_id}/issues/{issue_id}/issue-relation/"
    
    payload = {
        "relation_type": relation_type,
        "issues": related_issue_ids
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
        print(f"      ❌ Failed to create relation for issue {issue_id}: {resp.status_code} - {resp.text[:100]}")
        return False

def add_sub_issues(session, workspace_slug, project_id, parent_issue_id, sub_issue_ids):
    """Add sub-issues to a parent issue"""
    url = f"{BASE_URL}/workspaces/{workspace_slug}/projects/{project_id}/issues/{parent_issue_id}/sub-issues/"
    
    payload = {
        "sub_issue_ids": sub_issue_ids
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
        print(f"      ❌ Failed to add sub-issues to {parent_issue_id}: {resp.status_code} - {resp.text[:100]}")
        return False

# =========================
# CLAUDE ANALYSIS
# =========================
def analyze_issues_with_claude(project_name, project_description, issues):
    """Use Claude to analyze issues and suggest relationships and hierarchies"""
    
    # Prepare issue data for Claude with API-fetched IDs and names
    issue_summaries = []
    for issue in issues[:20]:  # Limit to first 20 issues to avoid token limits
        # Create a simplified reference using issue name for Claude analysis
        issue_ref = f"issue_{len(issue_summaries) + 1}"  # Simple sequential reference
        issue_summaries.append({
            "reference": issue_ref,
            "api_id": issue.get("id"),  # Store the actual API ID separately
            "name": issue.get("name", ""),
            "description": issue.get("description_html", "").replace('<p class="editor-paragraph-block">', '').replace('</p>', '')[:200]
        })
    
    # Create mapping for Claude to use simple references
    claude_issues = []
    for issue in issue_summaries:
        claude_issues.append({
            "reference": issue["reference"],
            "name": issue["name"],
            "description": issue["description"]
        })
    
    prompt = f"""
Analyze these work items from project "{project_name}" and suggest logical relationships and hierarchies.

Project Description: {project_description}

Work Items:
{json.dumps(claude_issues, indent=2)}

Please suggest:
1. Parent-child relationships (which items should be sub-items of others)
2. Related items (which items relate to each other)
3. Blocking relationships (which items might block others)

Rules:
- Each parent should have 1-4 sub-items maximum
- Not every item needs to be a parent or child
- Related items should have logical connections
- Be realistic about blocking relationships
- Consider typical software development workflows
- Use the "reference" field (like "issue_1", "issue_2") to identify items

Return exactly this JSON structure:
{{
  "sub_items": [
    {{
      "parent_reference": "issue_1",
      "children_references": ["issue_2", "issue_3"]
    }}
  ],
  "relations": [
    {{
      "issue_reference": "issue_1",
      "related_issue_references": ["issue_2", "issue_3"],
      "relation_type": "relates_to"
    }}
  ]
}}

Available relation types: relates_to, blocked_by, blocks, duplicate
"""

    try:
        rate_limit_claude()
        response = client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=2000,
            temperature=0.7,
            system="You are an expert in project management and software development workflows. Analyze work items and suggest logical relationships and hierarchies.",
            messages=[{"role": "user", "content": [{"type": "text", "text": prompt}]}]
        )
    except Exception as e:
        print(f"⚠️ Claude request failed: {e}")
        return None

    raw = response.content[0].text if response and response.content else ""
    raw = clean_json_output(raw)

    # Parse JSON
    parsed = safe_json_parse(raw)
    if parsed and isinstance(parsed, dict):
        # Convert Claude's references back to actual API IDs
        return convert_references_to_ids(parsed, issue_summaries)
    else:
        print(f"⚠️ Could not parse Claude analysis for project {project_name}")
        return None

def convert_references_to_ids(claude_analysis, issue_summaries):
    """Convert Claude's references back to actual API IDs"""
    # Create mapping from reference to API ID
    ref_to_id = {issue["reference"]: issue["api_id"] for issue in issue_summaries}
    
    converted_analysis = {
        "sub_items": [],
        "relations": []
    }
    
    # Convert sub_items
    for sub_item in claude_analysis.get("sub_items", []):
        parent_ref = sub_item.get("parent_reference")
        children_refs = sub_item.get("children_references", [])
        
        parent_id = ref_to_id.get(parent_ref)
        children_ids = [ref_to_id.get(ref) for ref in children_refs if ref_to_id.get(ref)]
        
        if parent_id and children_ids:
            converted_analysis["sub_items"].append({
                "parent_id": parent_id,
                "children_ids": children_ids
            })
    
    # Convert relations
    for relation in claude_analysis.get("relations", []):
        issue_ref = relation.get("issue_reference")
        related_refs = relation.get("related_issue_references", [])
        relation_type = relation.get("relation_type", "relates_to")
        
        issue_id = ref_to_id.get(issue_ref)
        related_ids = [ref_to_id.get(ref) for ref in related_refs if ref_to_id.get(ref)]
        
        if issue_id and related_ids:
            converted_analysis["relations"].append({
                "issue_id": issue_id,
                "related_issue_ids": related_ids,
                "relation_type": relation_type
            })
    
    return converted_analysis

# =========================
# RELATIONSHIP PROCESSING
# =========================
def process_project_relationships(project_data):
    """Process relationships for a single project (runs in thread)"""
    workspace_slug, project, session = project_data
    project_id = project["id"]
    project_name = project["name"]
    project_description = project.get("description", "")
    
    print(f"  🔄 Processing relationships for project: {project_name}")
    
    # Get all issues in the project (these will have API-assigned IDs)
    issues = get_project_issues(session, workspace_slug, project_id)
    
    if len(issues) < 2:
        print(f"    ⚠️ Project {project_name} has less than 2 issues, skipping relationships")
        return {"sub_items": 0, "relations": 0}
    
    print(f"    📋 Found {len(issues)} issues with API IDs, analyzing with Claude...")
    
    # Log some issue IDs for debugging
    if issues:
        sample_ids = [issue.get("id", "N/A") for issue in issues[:3]]
        print(f"    🔍 Sample issue IDs: {sample_ids}")
    
    # Use Claude to analyze and suggest relationships
    analysis = analyze_issues_with_claude(project_name, project_description, issues)
    
    if not analysis:
        print(f"    ❌ Failed to get analysis for project {project_name}")
        return {"sub_items": 0, "relations": 0}
    
    sub_items_created = 0
    relations_created = 0
    
    # Create sub-item relationships
    sub_items_suggestions = analysis.get("sub_items", [])
    print(f"    🔗 Processing {len(sub_items_suggestions)} sub-item suggestions...")
    
    for sub_item in sub_items_suggestions:
        parent_id = sub_item.get("parent_id")
        children_ids = sub_item.get("children_ids", [])
        
        if not parent_id or not children_ids:
            continue
            
        # Verify all IDs exist in the project
        issue_ids = [i.get("id") for i in issues]
        if parent_id not in issue_ids:
            print(f"      ⚠️ Parent ID {parent_id} not found in project issues")
            continue
            
        valid_children = [cid for cid in children_ids if cid in issue_ids]
        if not valid_children:
            print(f"      ⚠️ No valid children found for parent {parent_id}")
            continue
        
        parent_name = next((i.get("name", str(parent_id)) for i in issues if i.get("id") == parent_id), str(parent_id))
        print(f"      🔗 Adding {len(valid_children)} sub-items to '{parent_name}' (ID: {parent_id})")
        
        if add_sub_issues(session, workspace_slug, project_id, parent_id, valid_children):
            sub_items_created += len(valid_children)
            print(f"      ✅ Successfully added sub-items to '{parent_name}'")
        
        time.sleep(0.2)  # Small delay between operations
    
    # Create issue relations
    relations_suggestions = analysis.get("relations", [])
    print(f"    🔗 Processing {len(relations_suggestions)} relation suggestions...")
    
    for relation in relations_suggestions:
        issue_id = relation.get("issue_id")
        related_ids = relation.get("related_issue_ids", [])
        relation_type = relation.get("relation_type", "relates_to")
        
        if not issue_id or not related_ids:
            continue
            
        # Verify all IDs exist and relation type is valid
        issue_ids = [i.get("id") for i in issues]
        if issue_id not in issue_ids or relation_type not in RELATION_TYPES:
            if issue_id not in issue_ids:
                print(f"      ⚠️ Issue ID {issue_id} not found in project issues")
            if relation_type not in RELATION_TYPES:
                print(f"      ⚠️ Invalid relation type: {relation_type}")
            continue
            
        valid_related = [rid for rid in related_ids if rid in issue_ids and rid != issue_id]
        if not valid_related:
            print(f"      ⚠️ No valid related issues found for {issue_id}")
            continue
        
        issue_name = next((i.get("name", str(issue_id)) for i in issues if i.get("id") == issue_id), str(issue_id))
        print(f"      🔗 Creating {relation_type} relation for '{issue_name}' (ID: {issue_id}) with {len(valid_related)} items")
        
        if create_issue_relation(session, workspace_slug, project_id, issue_id, valid_related, relation_type):
            relations_created += len(valid_related)
            print(f"      ✅ Successfully created relations for '{issue_name}'")
        
        time.sleep(0.2)  # Small delay between operations
    
    print(f"  ✅ Completed {project_name}: {sub_items_created} sub-items, {relations_created} relations")
    return {"sub_items": sub_items_created, "relations": relations_created}

# =========================
# MAIN SCRIPT
# =========================
def main():
    session = get_authenticated_session()
    print("✅ Authenticated successfully.")
    
    # Get all workspaces and projects
    workspaces = get_workspaces(session)
    print(f"📂 Found {len(workspaces)} workspaces")
    
    all_project_data = []
    
    for workspace in workspaces:
        workspace_slug = workspace["slug"]
        print(f"\n📂 Processing workspace: {workspace_slug}")
        
        projects = get_projects(session, workspace_slug)
        print(f"  📌 Found {len(projects)} projects")
        
        for project in projects:
            project_name = project["name"]
            print(f"    📋 Queued: {project_name}")
            # Each thread needs its own session
            all_project_data.append((workspace_slug, project, get_authenticated_session()))
    
    if not all_project_data:
        print("⚠️ No projects found to process relationships for.")
        return
    
    print(f"\n🚀 Starting multithreaded relationship processing for {len(all_project_data)} projects...")
    print(f"Using {MAX_WORKERS} worker threads with rate limiting")
    
    total_sub_items = 0
    total_relations = 0
    
    # Process projects in parallel
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all project tasks
        future_to_project = {
            executor.submit(process_project_relationships, project_data): project_data[1]["name"] 
            for project_data in all_project_data
        }
        
        # Collect results as they complete
        completed_projects = 0
        for future in as_completed(future_to_project):
            project_name = future_to_project[future]
            try:
                result = future.result()
                total_sub_items += result["sub_items"]
                total_relations += result["relations"]
                
                completed_projects += 1
                print(f"📊 Progress: {completed_projects}/{len(all_project_data)} projects completed")
                
            except Exception as e:
                print(f"❌ Error processing project {project_name}: {e}")
                completed_projects += 1

    print(f"\n🎉 Relationship processing complete!")
    print(f"📊 Final Statistics:")
    print(f"   - Projects processed: {len(all_project_data)}")
    print(f"   - Total sub-items created: {total_sub_items}")
    print(f"   - Total relations created: {total_relations}")
    print(f"   - Average sub-items per project: {total_sub_items/len(all_project_data):.1f}" if all_project_data else "0")
    print(f"   - Average relations per project: {total_relations/len(all_project_data):.1f}" if all_project_data else "0")

if __name__ == "__main__":
    main()