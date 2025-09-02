import os
import sys
import json
import re
import random
import time
import threading
from pathlib import Path
from dotenv import load_dotenv
import anthropic
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# ------------------------
# Config
# ------------------------
load_dotenv()
OUTPUT_DIR = Path("../generated_json")
OUTPUT_DIR.mkdir(exist_ok=True)
PROJECTS_FILE = OUTPUT_DIR / "projects.json"

CLAUDE_API_KEY = os.getenv("ANTHROPIC_API_KEY")
if not CLAUDE_API_KEY:
    raise SystemExit("❌ Missing ANTHROPIC_API_KEY in .env")

client = anthropic.Anthropic(api_key=CLAUDE_API_KEY)

PRIORITIES = ["none", "low", "medium", "high", "urgent"]

# Rate limiting config
CLAUDE_RATE_LIMIT = 5  # requests per second for Claude API
MAX_WORKERS = 4        # number of concurrent threads

# Thread-safe locks
claude_lock = Lock()
last_claude_call = {"time": 0}

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

# ------------------------
# Rate Limiting Helpers
# ------------------------
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

# ------------------------
# Helpers
# ------------------------
def clean_json_output(text: str) -> str:
    """Strip code fences and odd leading/trailing text."""
    text = text.strip()
    # remove triple backticks fences
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    return text.strip()

# ------------------------
# Generation
# ------------------------
def generate_issue_with_claude(project_name, project_description, workspace_slug, project_identifier):
    """Generate a single issue using Claude API with rate limiting"""
    
    # Use constant placeholder values instead of UUIDs
    sample_schema = {
        "project_name": project_name,  # Store project name instead of UUID
        "project_identifier": project_identifier,  # Store project identifier
        "workspace_slug": workspace_slug,  # Store workspace slug
        "type_id": None,
        "name": "SHORT TITLE HERE",
        "description_html": '<p class="editor-paragraph-block">DESCRIPTION</p>',
        "assignee_count": random.randint(1, 3),  # Number of assignees instead of IDs
        "cycle_index": None,  # Index into available cycles (will be set randomly)
        "estimate_point": None,
        "label_ids": [],
        "module_index": None,  # Index into available modules (will be set randomly)
        "parent_id": None,
        "priority": random.choice(PRIORITIES),
        "start_date": None,
        "state_index": None,  # Index into available states (will be set randomly)
        "target_date": None,
        "type_id": None
    }

    prompt = f"""
You are an API that outputs exactly one JSON object (no explanation, no markdown, no extra text).
Take this template object and replace the values "SHORT TITLE HERE" and DESCRIPTION with a
realistic short title and a one-paragraph HTML description appropriate for the project.
Give only unique names and descriptions.
Keep all other fields exactly as provided - do not change any numeric values, indices, or null values.

Template object (replace only the name and description_html placeholders):
{json.dumps(sample_schema, indent=2)}

Project Information:
- Name: "{project_name}"
- Description: "{project_description}"

Return exactly the completed JSON object. No surrounding text.
"""

    try:
        rate_limit_claude()  # Apply rate limiting before Claude call
        response = client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=1000,
            temperature=0.7,
            system="You only output JSON — never explanations.",
            messages=[{"role": "user", "content": [{"type": "text", "text": prompt}]}]
        )
    except Exception as e:
        print(f"⚠️ Claude request failed: {e}")
        return None

    raw = response.content[0].text if response and response.content else ""
    raw = clean_json_output(raw)

    # Parse JSON
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass

    # Try extract first {...} block
    m = re.search(r"\{.*\}", raw, re.DOTALL)
    if m:
        candidate = m.group(0)
        try:
            obj = json.loads(candidate)
            if isinstance(obj, dict):
                return obj
        except Exception:
            pass

    print(f"⚠️ Could not parse Claude output for project {project_name}")
    return None

def generate_issues_for_project(project_data):
    """Generate all issues for a single project (runs in thread)"""
    project = project_data
    project_name = project.get("name", "Unknown Project")
    project_description = project.get("description", "")
    workspace_slug = project.get("workspace_slug", "unknown")
    project_identifier = project.get("identifier", "PROJ")
    
    print(f"  🔄 Processing project: {project_name} ({project_identifier})")
    
    issues = []
    
    # Generate N issues per project
    N = 50  # Generate 50 issues per project
    for i in range(N):
        issue_obj = generate_issue_with_claude(
            project_name=project_name,
            project_description=project_description,
            workspace_slug=workspace_slug,
            project_identifier=project_identifier
        )

        if issue_obj:
            # Randomize indices for variety
            issue_obj["cycle_index"] = random.randint(0, 2)  # Assume max 3 cycles
            issue_obj["state_index"] = random.randint(0, 4)  # Assume max 5 states
            issue_obj["module_index"] = random.randint(0, 2)  # Assume max 3 modules
            
            issues.append(issue_obj)
            print(f"    ✅ Created issue {i+1}/{N} for {project_name}")
        else:
            print(f"    ❌ Failed to create issue {i+1}/{N} for {project_name}")
    
    return issues

# ------------------------
# Main orchestration
# ------------------------
def main():
    print("🚀 Starting standalone issue generation (no API connection needed)...")
    
    # Load projects from file
    projects = load_projects_from_file()
    
    print(f"Using {MAX_WORKERS} worker threads")
    
    all_issues = []
    
    # Process projects in parallel using projects from file
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all project tasks
        future_to_project = {
            executor.submit(generate_issues_for_project, project): project.get("name", "Unknown") 
            for project in projects
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_project):
            project_name = future_to_project[future]
            try:
                project_issues = future.result()
                all_issues.extend(project_issues)
                print(f"✅ Completed project: {project_name} ({len(project_issues)} issues)")
            except Exception as e:
                print(f"❌ Error processing project {project_name}: {e}")

    # Save results
    out_file = OUTPUT_DIR / "issues.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(all_issues, f, indent=2, ensure_ascii=False)

    print(f"\n🎉 Successfully saved {len(all_issues)} issues to {out_file}")
    print(f"Generated issues for {len(projects)} projects using multithreading")
    print("💡 Run the backfill script when your API server is running to import these issues")

if __name__ == "__main__":
    main()