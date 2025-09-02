#!/usr/bin/env python3
import os
import sys
import json
import re
from pathlib import Path
from dotenv import load_dotenv
import anthropic

load_dotenv()

# =========================
# CONFIG
# =========================
OUTPUT_DIR = Path("../generated_json")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "cycles.json"
PROJECTS_FILE = OUTPUT_DIR / "projects.json"

CLAUDE_API_KEY = os.getenv("ANTHROPIC_API_KEY")
if not CLAUDE_API_KEY:
    print("❌ Missing ANTHROPIC_API_KEY in .env")
    sys.exit(1)

client = anthropic.Anthropic(api_key=CLAUDE_API_KEY)

# How many cycles per project to generate
CYCLES_PER_PROJECT = int(os.getenv("CYCLES_PER_PROJECT", "5"))

# =========================
# HELPERS
# =========================
def clean_json_output(text: str) -> str:
    """Remove Markdown code-fences if present, and trim."""
    text = text.strip()
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text, flags=re.IGNORECASE | re.MULTILINE).strip()
    return text

def extract_first_json_array(text: str):
    """Try to extract the first JSON array substring from text (robust fallback)."""
    m = re.search(r"\[.*\]", text, re.DOTALL)
    if not m:
        return None
    candidate = m.group(0)
    try:
        return json.loads(candidate)
    except Exception:
        return None

def safe_json_loads(text: str):
    """Attempt robust JSON parsing with progressive fallbacks."""
    cleaned = clean_json_output(text)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        arr = extract_first_json_array(cleaned)
        if arr is not None:
            return arr
        sanitized = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", cleaned)
        try:
            return json.loads(sanitized)
        except Exception:
            return None

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
# CLAUDE: generate cycle metadata (name + description)
# =========================
def generate_cycles_meta(project_name: str, project_description: str, count: int):
    """
    Ask Claude to produce exactly `count` cycle objects for given project.
    Each object must contain `name` and `description` only.
    """
    prompt = f"""
You are a JSON generator. Output EXACTLY {count} unique cycle objects in a JSON array (no extra text).
Each object must have exactly 2 keys: "name" and "description".
- "name": a short, unique cycle name (max 60 characters) relevant to this project.
- "description": 1-2 sentence natural-language description of the cycle's focus.
Do NOT include dates or project IDs. Do NOT include any additional keys.

Project Information:
- Name: "{project_name}"
- Description: "{project_description}"

Generate cycles that would make sense for this specific project. Examples: "Phase 1 - Planning", "MVP Development", "User Testing", etc.

Return ONLY valid JSON (a single JSON array). Example output:
[{{"name": "Cycle 1 - ...", "description": "..." }}, ...]
"""
    
    # Attempt up to 3 times
    for attempt in range(1, 4):
        try:
            temperature = 0.6 if attempt == 1 else 0.0
            message = client.messages.create(
                model="claude-3-5-sonnet-20241022",
                max_tokens=800,
                temperature=temperature,
                system="You are a precise JSON generator. Output only valid JSON.",
                messages=[{"role": "user", "content": [{"type": "text", "text": prompt}]}],
            )
        except Exception as e:
            print(f"⚠️ Claude API error on attempt {attempt}: {e}")
            continue

        raw = message.content[0].text if message.content and len(message.content) > 0 else ""
        parsed = safe_json_loads(raw)
        if isinstance(parsed, list) and len(parsed) == count and all(isinstance(x, dict) for x in parsed):
            # Validate objects contain name & description
            valid = True
            for obj in parsed:
                if "name" not in obj or "description" not in obj:
                    valid = False
                    break
                # Force both values to strings
                obj["name"] = str(obj["name"]).strip()
                obj["description"] = str(obj["description"]).strip()
            if valid:
                return parsed
        
        print(f"⚠️ Claude output invalid on attempt {attempt} for project '{project_name}'. Retrying...")
        preview = raw[:400].replace("\n", " ")
        print(f"   preview: {preview}...")
    
    print(f"❌ Failed to get valid cycles meta from Claude for project '{project_name}' after retries.")
    return []

# =========================
# MAIN generation (no API calls)
# =========================
def main():
    print("🚀 Starting standalone cycle generation (no API connection needed)...")
    
    # Load projects from file
    projects = load_projects_from_file()
    
    all_cycles = []  # final list of cycles to dump to JSON

    for project in projects:
        project_name = project.get("name", "Unknown Project")
        project_description = project.get("description", "")
        workspace_slug = project.get("workspace_slug", "unknown")
        identifier = project.get("identifier", "PROJ")
        
        print(f"\n▶ Project: {project_name} ({identifier})")

        # Generate metadata via Claude
        metas = generate_cycles_meta(project_name, project_description, CYCLES_PER_PROJECT)
        if not metas:
            print(f"    ⚠️ Skipping project {project_name} — no cycle metadata generated.")
            continue

        # Create cycle objects with only constants (NO UUIDs)
        for meta in metas:
            cycle_obj = {
                "workspace_slug": workspace_slug,
                "project_name": project_name,  # Store name instead of UUID
                "project_identifier": identifier,  # Store identifier for mapping
                "name": meta["name"],
                "description": meta["description"],
            }
            all_cycles.append(cycle_obj)

        print(f"    ✅ Generated {len(metas)} cycles for project {project_name}")

    # Save to a single JSON file
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_cycles, f, indent=2, ensure_ascii=False)

    print(f"\n🎉 Cycle generation complete!")
    print(f"📊 Final Statistics:")
    print(f"   - Projects processed: {len(projects)}")
    print(f"   - Total cycles generated: {len(all_cycles)}")
    print(f"   - Average cycles per project: {len(all_cycles)/len(projects):.1f}")
    print(f"📄 Saved {len(all_cycles)} generated cycles to {OUTPUT_FILE}")
    print("💡 Run the backfill script when your API server is running to import these cycles")

if __name__ == "__main__":
    main()