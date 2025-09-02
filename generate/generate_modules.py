import os
import sys
import json
import random
import re
import time
from pathlib import Path
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import anthropic
from typing import Dict, List

# ------------------------
# Config
# ------------------------
load_dotenv()
OUTPUT_DIR = Path("../generated_json")
OUTPUT_DIR.mkdir(exist_ok=True)

CLAUDE_API_KEY = os.getenv("ANTHROPIC_API_KEY")
if not CLAUDE_API_KEY:
    raise SystemExit("❌ Missing ANTHROPIC_API_KEY in .env")

# File paths
PROJECTS_FILE = OUTPUT_DIR / "projects.json"
OUT_FILE = OUTPUT_DIR / "modules.json"

# Realistic module counts based on project size/type
MIN_MODULES_PER_PROJECT = 3
MAX_MODULES_PER_PROJECT = 5

# Threading configuration
MAX_WORKERS = 6
MAX_CONCURRENT_CLAUDE = 2
CLAUDE_RATE_LIMIT = 2

client = anthropic.Anthropic(api_key=CLAUDE_API_KEY)
claude_semaphore = threading.Semaphore(MAX_CONCURRENT_CLAUDE)
claude_lock = threading.Lock()
save_lock = threading.Lock()
last_claude_call = {"time": 0}

# Track generated module names to avoid duplicates
generated_names_lock = threading.Lock()
all_generated_names = set()

def load_projects_from_file():
    """Load projects from the generated projects.json file"""
    if not PROJECTS_FILE.exists():
        raise SystemExit(f"❌ Projects file not found: {PROJECTS_FILE}")
    
    try:
        with PROJECTS_FILE.open("r", encoding="utf-8") as f:
            projects_data = json.load(f)
        
        if not isinstance(projects_data, list):
            raise SystemExit(f"❌ Invalid projects file format. Expected list, got {type(projects_data)}")
        
        # Use projects as-is, just add member_count for generation variety
        projects = []
        for proj in projects_data:
            if not isinstance(proj, dict):
                continue
            
            project = {
                "name": proj.get("name", "Unknown Project"),
                "description": proj.get("description", ""),
                "identifier": proj.get("identifier", "PROJ"),
                "workspace_slug": proj.get("workspace_slug", "unknown"),
                "member_count": random.randint(3, 8)  # Generate random for variety
            }
            projects.append(project)
        
        if not projects:
            raise SystemExit("❌ No valid projects found in projects.json")
        
        print(f"✅ Loaded {len(projects)} projects from {PROJECTS_FILE}")
        for i, proj in enumerate(projects, 1):
            print(f"  {i}. {proj['name']} ({proj['identifier']})")
        
        return projects
        
    except json.JSONDecodeError as e:
        raise SystemExit(f"❌ Invalid JSON in projects file: {e}")
    except Exception as e:
        raise SystemExit(f"❌ Error reading projects file: {e}")

# ------------------------
# Rate limiting and utility functions
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

def clean_json_output(text: str) -> str:
    text = re.sub(r"^```(?:json)?\s*", "", text.strip())
    text = re.sub(r"\s*```$", "", text)
    return text.strip()

def add_generated_name(name):
    """Thread-safe way to add module name to global set"""
    with generated_names_lock:
        all_generated_names.add(name.lower())

def get_generated_names_sample():
    """Get a sample of already generated names to avoid duplicates"""
    with generated_names_lock:
        return list(all_generated_names)[-30:]

def load_existing_modules():
    if OUT_FILE.exists():
        try:
            with OUT_FILE.open("r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    existing_names = {m.get("name", "").lower() for m in data if isinstance(m, dict)}
                    with generated_names_lock:
                        all_generated_names.update(existing_names)
                    return data
        except Exception:
            pass
    return []

def save_modules(modules_list):
    with save_lock:
        tmp = OUT_FILE.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(modules_list, f, indent=2, ensure_ascii=False)
        tmp.replace(OUT_FILE)

def determine_module_count(project_name: str, member_count: int) -> int:
    """Determine realistic number of modules based on project characteristics"""
    if member_count <= 3:
        base_count = random.randint(2, 4)
    elif member_count <= 6:
        base_count = random.randint(3, 6)
    elif member_count <= 10:
        base_count = random.randint(4, 7)
    else:
        base_count = random.randint(5, 8)
    
    name_lower = project_name.lower()
    
    if any(keyword in name_lower for keyword in ['platform', 'infrastructure', 'system', 'architecture']):
        base_count += random.randint(1, 2)
    elif any(keyword in name_lower for keyword in ['research', 'innovation', 'lab', 'experiment']):
        base_count += random.randint(0, 2)
    elif any(keyword in name_lower for keyword in ['support', 'operations', 'maintenance']):
        base_count -= random.randint(0, 1)
    
    return max(MIN_MODULES_PER_PROJECT, min(MAX_MODULES_PER_PROJECT, base_count))

# ------------------------
# Claude generation
# ------------------------
def generate_module_batch(project_name, project_description, member_count, batch_size):
    """Generate a batch of modules"""
    existing_names = get_generated_names_sample()
    
    prompt = f"""
You are generating realistic project modules for a team project.

Project Information:
- Name: "{project_name}"
- Description: "{project_description}"
- Team Size: {member_count} members

Requirements:
1. Generate {batch_size} unique, realistic modules for this specific project
2. Each module should be relevant to the project's domain and description
3. Avoid these already used names: {existing_names[-15:] if existing_names else "none"}
4. Module names should be 2-5 words, professional and specific

For each module, provide:
- "name": Concise, specific module name (2-5 words)
- "description": 2-3 sentence description of what this module covers

Return exactly this JSON structure (no extra text):
[
  {{
    "name": "Module name here",
    "description": "Detailed description explaining what this module encompasses and its purpose within the project."
  }}
]
"""

    try:
        rate_limit_claude()
        with claude_semaphore:
            resp = client.messages.create(
                model="claude-3-5-sonnet-20241022",
                max_tokens=1500,
                temperature=0.7,
                system="You generate realistic project modules in JSON format. Output only valid JSON arrays.",
                messages=[{"role": "user", "content": [{"type": "text", "text": prompt}]}]
            )
    except Exception as e:
        print(f"⚠️ Claude request failed: {e}")
        return []

    raw = resp.content[0].text if resp.content else ""
    raw = clean_json_output(raw)

    try:
        modules_data = json.loads(raw)
        if not isinstance(modules_data, list):
            return []
    except Exception:
        match = re.search(r'\[.*\]', raw, re.DOTALL)
        if match:
            try:
                modules_data = json.loads(match.group(0))
                if not isinstance(modules_data, list):
                    return []
            except Exception:
                print(f"⚠️ Could not parse Claude output for project {project_name}")
                return []
        else:
            print(f"⚠️ Could not find JSON array in Claude output for project {project_name}")
            return []

    # Convert to module objects with placeholders (NO UUIDs)
    modules = []
    generated_names_this_batch = []
    
    for module_data in modules_data:
        if not isinstance(module_data, dict) or 'name' not in module_data or 'description' not in module_data:
            continue
        
        status = random.choice(["backlog", "planned", "in-progress", "paused", "completed", "cancelled"])
        
        # Use simple placeholders - NO UUIDs
        module_obj = {
            "name": module_data["name"],
            "description": module_data["description"],
            "status": status,
            "lead_index": random.randint(0, member_count - 1),  # Index for lead
            "member_count": random.randint(1, min(3, member_count)),  # Count of members
            "project_name": project_name  # Just store project name for mapping later
        }
        
        modules.append(module_obj)
        generated_names_this_batch.append(module_data["name"])
    
    for name in generated_names_this_batch:
        add_generated_name(name)
    
    return modules

def generate_modules_for_project(project_data):
    """Generate all modules for a single project"""
    project = project_data
    project_name = project["name"]
    project_description = project["description"]
    member_count = project["member_count"]
    
    print(f"  🔄 Processing project: {project_name}")
    
    module_count = determine_module_count(project_name, member_count)
    print(f"    📊 Generating {module_count} modules for {project_name}")
    
    batch_size = min(3, module_count)
    all_modules = []
    batches_needed = (module_count + batch_size - 1) // batch_size
    
    for batch_num in range(batches_needed):
        remaining_modules = module_count - len(all_modules)
        current_batch_size = min(batch_size, remaining_modules)
        
        if current_batch_size <= 0:
            break
        
        print(f"    🔄 Generating batch {batch_num + 1}/{batches_needed} ({current_batch_size} modules)")
        
        batch_modules = generate_module_batch(
            project_name=project_name,
            project_description=project_description,
            member_count=member_count,
            batch_size=current_batch_size
        )
        
        if batch_modules:
            all_modules.extend(batch_modules)
            print(f"    ✅ Generated {len(batch_modules)} modules in batch {batch_num + 1}")
        else:
            print(f"    ❌ Failed to generate batch {batch_num + 1} for {project_name}")
        
        if batch_num < batches_needed - 1:
            time.sleep(0.3)
    
    print(f"  ✅ Completed {project_name}: {len(all_modules)}/{module_count} modules generated")
    return all_modules

# ------------------------
# Main
# ------------------------
def main():
    print("🚀 Starting standalone module generation (no API connection needed)...")
    
    projects = load_projects_from_file()
    modules = load_existing_modules()
    print(f"📦 Loaded {len(modules)} existing modules")
    
    print(f"\n🚀 Starting multithreaded module generation for {len(projects)} projects...")
    print(f"Using {MAX_WORKERS} worker threads")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_project = {
            executor.submit(generate_modules_for_project, project): project["name"] 
            for project in projects
        }
        
        completed_projects = 0
        for future in as_completed(future_to_project):
            project_name = future_to_project[future]
            try:
                project_modules = future.result()
                if project_modules:
                    modules.extend(project_modules)
                    save_modules(modules)
                    print(f"✅ Saved modules for {project_name} - Total: {len(modules)} modules")
                
                completed_projects += 1
                print(f"📊 Progress: {completed_projects}/{len(projects)} projects completed")
                
            except Exception as e:
                print(f"❌ Error processing project {project_name}: {e}")
                completed_projects += 1

    print(f"\n🎉 Module generation complete!")
    print(f"📊 Final Statistics:")
    print(f"   - Projects processed: {len(projects)}")
    print(f"   - Total modules generated: {len(modules)}")
    print(f"   - Average modules per project: {len(modules)/len(projects):.1f}")
    print(f"📦 All modules saved to: {OUT_FILE}")
    print("💡 Run the backfill script when your API server is running to import these modules")

if __name__ == "__main__":
    main()