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
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

load_dotenv()

# =========================
# CONFIG
# =========================
OUTPUT_DIR = Path("../generated_json")
OUTPUT_DIR.mkdir(exist_ok=True)
PROJECTS_FILE = OUTPUT_DIR / "projects.json"

CLAUDE_API_KEY = os.getenv("ANTHROPIC_API_KEY")
if not CLAUDE_API_KEY:
    raise SystemExit("❌ Missing ANTHROPIC_API_KEY in .env")

client = anthropic.Anthropic(api_key=CLAUDE_API_KEY)

# Configuration
VIEWS_PER_PROJECT = 4  # Generate 4 views per project
MAX_WORKERS = 4
CLAUDE_RATE_LIMIT = 2  # requests per second

# Thread-safe locks
claude_lock = threading.Lock()
last_claude_call = {"time": 0}

# View grouping options - all 4 options
GROUP_BY_OPTIONS = ["state", "priority", "cycle", "module"]

# Base view template
BASE_VIEW_TEMPLATE = {
    "access": 1,
    "display_filters": {
        "calendar": {
            "show_weekends": False,
            "layout": "month"
        },
        "layout": "kanban",
        "order_by": "sort_order",
        "group_by": "state",  # This will be changed
        "show_empty_groups": False,
        "sub_group_by": None,
        "sub_issue": False,
        "type": None
    },
    "display_properties": {
        "assignee": True,
        "attachment_count": True,
        "created_on": True,
        "cycle": True,
        "due_date": True,
        "estimate": True,
        "issue_type": True,
        "key": True,
        "labels": True,
        "link": True,
        "modules": True,
        "priority": True,
        "start_date": True,
        "state": True,
        "sub_issue_count": True,
        "updated_on": True
    }
}

def load_projects_from_file():
    """Load projects from the generated projects.json file"""
    if not PROJECTS_FILE.exists():
        raise SystemExit(f"❌ Projects file not found: {PROJECTS_FILE}")
    
    try:
        with PROJECTS_FILE.open("r", encoding="utf-8") as f:
            projects_data = json.load(f)
        
        if not isinstance(projects_data, list):
            raise SystemExit(f"❌ Invalid projects file format. Expected list, got {type(projects_data)}")
        
        print(f"✅ Loaded {len(projects_data)} projects from {PROJECTS_FILE}")
        for i, proj in enumerate(projects_data, 1):
            print(f"  {i}. {proj.get('name', 'Unknown')} ({proj.get('identifier', 'Unknown')})")
        
        return projects_data
        
    except json.JSONDecodeError as e:
        raise SystemExit(f"❌ Invalid JSON in projects file: {e}")
    except Exception as e:
        raise SystemExit(f"❌ Error reading projects file: {e}")

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
        # Try to extract JSON array from text
        match = re.search(r'\[.*\]', text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except Exception:
                pass
        return None

# =========================
# CLAUDE GENERATION
# =========================
def generate_view_names_and_descriptions(project_name, project_description, group_by_fields):
    """Generate realistic view names and descriptions using Claude"""
    
    prompt = f"""
You are generating project management view names and descriptions for a project.

Project Information:
- Name: "{project_name}"
- Description: "{project_description}"

I need {len(group_by_fields)} views, each grouped by a different field:
{', '.join([f'"{field}"' for field in group_by_fields])}

Requirements:
1. Generate realistic view names and descriptions that would actually be used by project teams
2. Each view should reflect what someone would use that grouping for
3. Names should be 2-4 words, professional and clear
4. Descriptions should be 1-2 sentences explaining the view's purpose

Examples of good view names:
- "Status Board" (for state grouping)
- "Priority Matrix" (for priority grouping) 
- "Sprint Overview" (for cycle grouping)
- "Module Breakdown" (for module grouping)

Return exactly this JSON structure (no extra text):
[
  {{
    "name": "View name here",
    "description": "Description explaining what this view is used for and why someone would use this grouping.",
    "group_by": "{group_by_fields[0]}"
  }},
  {{
    "name": "View name here", 
    "description": "Description explaining what this view is used for and why someone would use this grouping.",
    "group_by": "{group_by_fields[1]}"
  }},
  {{
    "name": "View name here",
    "description": "Description explaining what this view is used for and why someone would use this grouping.", 
    "group_by": "{group_by_fields[2]}"
  }},
  {{
    "name": "View name here",
    "description": "Description explaining what this view is used for and why someone would use this grouping.", 
    "group_by": "{group_by_fields[3]}"
  }}
]
"""

    try:
        rate_limit_claude()
        response = client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=1000,
            temperature=0.7,
            system="You generate realistic project management view configurations in JSON format. Output only valid JSON arrays.",
            messages=[{"role": "user", "content": [{"type": "text", "text": prompt}]}]
        )
    except Exception as e:
        print(f"⚠️ Claude request failed: {e}")
        return None

    raw = response.content[0].text if response and response.content else ""
    raw = clean_json_output(raw)

    # Parse JSON
    parsed = safe_json_parse(raw)
    if isinstance(parsed, list) and len(parsed) == len(group_by_fields):
        return parsed
    else:
        print(f"⚠️ Could not parse Claude output for project {project_name}")
        return None

# =========================
# VIEW GENERATION
# =========================
def create_view_object(view_data, project_name, workspace_slug, project_identifier):
    """Create a complete view object from Claude-generated data with NO UUIDs"""
    view = BASE_VIEW_TEMPLATE.copy()
    view["display_filters"] = BASE_VIEW_TEMPLATE["display_filters"].copy()
    view["display_properties"] = BASE_VIEW_TEMPLATE["display_properties"].copy()
    
    # Set the specific fields
    view["name"] = view_data["name"]
    view["description"] = view_data["description"]
    view["display_filters"]["group_by"] = view_data["group_by"]
    
    # Adjust layout based on group_by for better UX
    if view_data["group_by"] == "priority":
        view["display_filters"]["layout"] = "list"  # Priority works better in list view
    elif view_data["group_by"] == "cycle":
        view["display_filters"]["layout"] = "kanban"  # Cycles work well in kanban
    
    # Store project info for mapping later (NO UUIDs)
    view["project_name"] = project_name
    view["project_identifier"] = project_identifier
    view["workspace_slug"] = workspace_slug
    
    return view

def generate_views_for_project(project_data):
    """Generate views for a single project (runs in thread)"""
    project = project_data
    project_name = project["name"]
    project_description = project.get("description", "")
    workspace_slug = project.get("workspace_slug", "unknown")
    project_identifier = project.get("identifier", "PROJ")
    
    print(f"  🔄 Processing project: {project_name}")
    
    # Use all 4 group_by options for this project (state, priority, cycle, module)
    selected_groups = GROUP_BY_OPTIONS.copy()
    
    print(f"    📊 Generating views grouped by: {', '.join(selected_groups)}")
    
    # Generate view metadata using Claude
    view_data = generate_view_names_and_descriptions(
        project_name, 
        project_description, 
        selected_groups
    )
    
    if not view_data:
        print(f"    ❌ Failed to generate view data for {project_name}")
        return []
    
    # Create view objects
    views = []
    for data in view_data:
        view_obj = create_view_object(data, project_name, workspace_slug, project_identifier)
        views.append(view_obj)
        
        print(f"    ✅ Generated view: '{data['name']}' (grouped by {data['group_by']})")
    
    print(f"  ✅ Completed {project_name}: {len(views)} views generated")
    return views

# =========================
# MAIN SCRIPT
# =========================
def main():
    print("🚀 Starting standalone view generation (no API connection needed)...")
    
    # Load projects from file
    projects = load_projects_from_file()
    
    print(f"\n🚀 Starting multithreaded view generation for {len(projects)} projects...")
    print(f"Generating {VIEWS_PER_PROJECT} views per project using {MAX_WORKERS} worker threads")
    print(f"Each project will get views for: {', '.join(GROUP_BY_OPTIONS)}")
    
    all_views = []
    
    # Process projects in parallel
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all project tasks
        future_to_project = {
            executor.submit(generate_views_for_project, project): project["name"] 
            for project in projects
        }
        
        # Collect results as they complete
        completed_projects = 0
        for future in as_completed(future_to_project):
            project_name = future_to_project[future]
            try:
                project_views = future.result()
                if project_views:
                    all_views.extend(project_views)
                
                completed_projects += 1
                print(f"📊 Progress: {completed_projects}/{len(projects)} projects completed")
                
            except Exception as e:
                print(f"❌ Error processing project {project_name}: {e}")
                completed_projects += 1

    # Save results
    output_file = OUTPUT_DIR / "views.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_views, f, indent=2, ensure_ascii=False)

    print(f"\n🎉 View generation complete!")
    print(f"📊 Final Statistics:")
    print(f"   - Projects processed: {len(projects)}")
    print(f"   - Total views generated: {len(all_views)}")
    print(f"   - Average views per project: {len(all_views)/len(projects):.1f}")
    print(f"📄 Views saved to: {output_file}")
    print("💡 Run the backfill script when your API server is running to import these views")
    
    # Show breakdown by group_by
    group_by_counts = {}
    for view in all_views:
        group_by = view.get("display_filters", {}).get("group_by", "unknown")
        group_by_counts[group_by] = group_by_counts.get(group_by, 0) + 1
    
    print(f"\n📈 Views by grouping:")
    for group_by, count in sorted(group_by_counts.items()):
        print(f"   - {group_by}: {count} views")

if __name__ == "__main__":
    main()