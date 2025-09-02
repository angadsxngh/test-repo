import json
import random
import string
import os
from dotenv import load_dotenv
from pathlib import Path
import anthropic
load_dotenv()

client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

NUM_PROJECTS_PER_WORKSPACE = 6
WORKSPACE_JSON_PATH = Path("../generated_json/workspace.json")
OUTPUT_JSON_PATH = Path("../generated_json/projects.json")

COVER_IMAGES = [
    "https://images.unsplash.com/photo-1542202229-7d93c33f5d07?auto=format&fit=crop&q=80&ixlib=rb-4.0.3&w=870",
    "https://images.unsplash.com/photo-1507525428034-b723cf961d3e?auto=format&fit=crop&q=80&ixlib=rb-4.0.3&w=870",
    "https://images.unsplash.com/photo-1488521787991-ed7bbaae773c?auto=format&fit=crop&q=80&ixlib=rb-4.0.3&w=870"
]
EMOJIS = ["🚀", "🌟", "🔥", "🎯", "🧠", "💡", "🌱", "🎨", "📦", "🔧"]

# Engineering team categories to ensure variety
ENGINEERING_TEAMS = [
    "Backend Engineering",
    "Frontend Engineering", 
    "Mobile Engineering",
    "Platform Engineering",
    "Data Engineering",
    "DevOps/Infrastructure",
    "Security Engineering",
    "QA/Testing",
    "Machine Learning",
    "API/Integrations"
]

def generate_identifier(name):
    return ''.join(filter(str.isupper, name.upper()))[:5] + ''.join(random.choices(string.ascii_uppercase, k=2))

def ask_claude_for_project_name_and_desc(used_names=None, team_type=None):
    if used_names is None:
        used_names = []
    
    # Create a more specific prompt for engineering teams
    avoid_names_text = f"AVOID these already used names: {', '.join(used_names[-15:])}" if used_names else ""
    
    prompt = f"""You work at a real tech company. Generate a realistic engineering team/project name.

Team type focus: {team_type}

Examples of good names:
- Backend Engineering, API Services, Core Platform
- Frontend Web, Mobile iOS, React Components  
- Data Pipeline, Analytics Platform, ML Models
- DevOps Infrastructure, Cloud Platform, Site Reliability
- Security Team, Auth Services, Compliance
- QA Automation, Testing Framework, Release Engineering

Requirements:
- Must sound like a real engineering team name (not AI-generated)
- Use natural, industry-standard terminology
- Keep it concise and professional
- {avoid_names_text}

Return ONLY valid JSON: {{"name": "Team Name", "description": "Brief one-line description of what this team builds/maintains"}}"""

    response = client.messages.create(
        model="claude-3-5-sonnet-20241022",
        max_tokens=800,
        temperature=0.7,  # Balanced for realistic but varied results
        system="You are a tech company employee naming real engineering teams. Use authentic, professional naming conventions that real companies use.",
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": prompt
                    }
                ]
            }
        ]
    )
    content = response.content[0].text.strip()
    try:
        return json.loads(content)
    except:
        print(f"⚠️  Fallback: JSON parse error for {team_type}, returning default.")
        fallback_names = {
            "Backend Engineering": "Core backend services and APIs",
            "Frontend Engineering": "User-facing web applications",
            "Mobile Engineering": "iOS and Android mobile apps",
            "Platform Engineering": "Internal developer tools and infrastructure",
            "Data Engineering": "Data pipelines and analytics infrastructure",
            "DevOps/Infrastructure": "Cloud infrastructure and deployment systems"
        }
        fallback_name = team_type if team_type in fallback_names else f"Engineering Team {len(used_names) + 1}"
        fallback_desc = fallback_names.get(team_type, "Engineering team focused on core product development")
        
        return {
            "name": fallback_name,
            "description": fallback_desc
        }

def generate_project(name, description, cover, emoji, workspace_slug):
    return {
        "name": name,
        "identifier": generate_identifier(name),
        "description": description,
        "cover_image": cover,
        "cover_image_asset": None,
        "cover_image_url": cover,
        "logo_props": {
            "in_use": "emoji",
            "emoji": {"value": str(128204)}
        },
        "network": 2,
        "project_lead": None,
        "csrfmiddlewaretoken": ''.join(random.choices(string.ascii_letters + string.digits, k=32)),
        "workspace_slug": workspace_slug
    }

print("🚀 Starting engineering project generation...")

# Load workspaces
with open(WORKSPACE_JSON_PATH) as f:
    workspaces = json.load(f)
print(f"✅ Loaded {len(workspaces)} workspaces.")

projects = []
all_used_names = []  # Track all generated names across workspaces

for ws in workspaces:
    print(f"\n📁 Generating engineering projects for workspace: {ws['slug']}")
    
    # Shuffle engineering teams for variety per workspace
    available_teams = ENGINEERING_TEAMS.copy()
    random.shuffle(available_teams)
    
    for i in range(NUM_PROJECTS_PER_WORKSPACE):
        # Use different team types to ensure variety
        team_type = available_teams[i % len(available_teams)]
        
        # Generate with specific team type
        data = ask_claude_for_project_name_and_desc(all_used_names, team_type)
        name = data["name"]
        description = data["description"]
        
        # Check if name is already used (extra safety)
        if name in all_used_names:
            print(f"  ⚠️  Duplicate detected: {name}, regenerating...")
            # Try with a different team type
            backup_team_type = random.choice([t for t in available_teams if t != team_type])
            data = ask_claude_for_project_name_and_desc(all_used_names + [name], backup_team_type)
            name = data["name"]
            description = data["description"]
        
        all_used_names.append(name)
        
        cover = random.choice(COVER_IMAGES)
        emoji = random.choice(EMOJIS)
        project = generate_project(name, description, cover, emoji, ws["slug"])
        projects.append(project)
        print(f"  ✅ Project {i+1} ({team_type}): {name}")

# Save to file
OUTPUT_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
with open(OUTPUT_JSON_PATH, "w") as f:
    json.dump(projects, f, indent=2)

print(f"\n💾 Saved {len(projects)} engineering projects to: {OUTPUT_JSON_PATH.resolve()}")
print(f"📊 Generated {len(set(all_used_names))} unique names out of {len(all_used_names)} total")
print("🎉 Engineering project generation complete!")