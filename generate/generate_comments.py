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
OUTPUT_FILE = OUTPUT_DIR / "comments.json"
ISSUES_FILE = OUTPUT_DIR / "issues.json"

CLAUDE_API_KEY = os.getenv("ANTHROPIC_API_KEY")
if not CLAUDE_API_KEY:
    raise SystemExit("❌ Missing ANTHROPIC_API_KEY in .env")

client = anthropic.Anthropic(api_key=CLAUDE_API_KEY)

# Configuration
COMMENTS_PER_ISSUE = 1  
MAX_WORKERS = 4
CLAUDE_RATE_LIMIT = 2  # requests per second

# Thread-safe locks
claude_lock = threading.Lock()
last_claude_call = {"time": 0}

# Global storage for comments
all_comments = []
comments_lock = threading.Lock()

def load_issues_from_file():
    """Load issues from the generated issues.json file"""
    if not ISSUES_FILE.exists():
        raise SystemExit(f"❌ Issues file not found: {ISSUES_FILE}")
    
    try:
        with ISSUES_FILE.open("r", encoding="utf-8") as f:
            issues_data = json.load(f)
        
        if not isinstance(issues_data, list):
            raise SystemExit(f"❌ Invalid issues file format. Expected list, got {type(issues_data)}")
        
        print(f"✅ Loaded {len(issues_data)} issues from {ISSUES_FILE}")
        return issues_data
        
    except json.JSONDecodeError as e:
        raise SystemExit(f"❌ Invalid JSON in issues file: {e}")
    except Exception as e:
        raise SystemExit(f"❌ Error reading issues file: {e}")

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

def save_comments_batch(comments_batch):
    """Thread-safe way to add comments to global list"""
    with comments_lock:
        all_comments.extend(comments_batch)
        # Save incrementally
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(all_comments, f, indent=2, ensure_ascii=False)

# =========================
# CLAUDE COMMENT GENERATION
# =========================
def generate_comments_with_claude(issue_data, project_slug):
    """Generate realistic comments for an issue using Claude"""
    
    issue_name = issue_data.get("name", "Untitled Issue")
    issue_description = issue_data.get("description_html", "").replace('<p class="editor-paragraph-block">', '').replace('</p>', '')[:300]
    issue_priority = issue_data.get("priority", "none")
    
    # Determine number of comments for this issue
    num_comments = 1
    
    prompt = f"""
Generate {num_comments} realistic comments for a project management issue. These should feel like real team conversations.

Issue Details:
- Name: "{issue_name}"
- Description: "{issue_description}"
- Priority: {issue_priority}
- Project: {project_slug}

Generate diverse comment types:
1. Progress updates from team members
2. Questions about requirements or implementation
3. Technical discussions or suggestions
4. Status updates or blockers
5. Code review feedback or technical details
6. Testing updates or bug reports
7. Final completion confirmations

Requirements:
- Each comment should be 1-3 sentences
- Make them feel like real developer/team conversations
- Use appropriate technical language for the project type
- Vary the tone and perspective (different team members)
- Include realistic details relevant to the issue

Return exactly this JSON format:
[
  {{
    "comment_html": "<p class=\\"editor-paragraph-block\\">First realistic comment here</p>"
  }},
  {{
    "comment_html": "<p class=\\"editor-paragraph-block\\">Second realistic comment here</p>"
  }}
]

NO markdown, NO extra text, ONLY the JSON array.
"""

    try:
        rate_limit_claude()
        response = client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=1500,
            temperature=0.8,
            system="You generate realistic project management comments that feel like authentic team conversations. Always return valid JSON arrays.",
            messages=[{"role": "user", "content": [{"type": "text", "text": prompt}]}]
        )
    except Exception as e:
        print(f"⚠️ Claude request failed for issue {issue_name}: {e}")
        return []

    raw = response.content[0].text if response and response.content else ""
    raw = clean_json_output(raw)

    # Parse JSON
    parsed = safe_json_parse(raw)
    if isinstance(parsed, list):
        # Add issue name for mapping later (NO UUIDs)
        comments = []
        for comment in parsed:
            if isinstance(comment, dict) and 'comment_html' in comment:
                comment_obj = {
                    "issue_name": issue_name,  # Store issue name instead of UUID
                    "project_slug": project_slug,  # Store project slug for context
                    "comment_html": comment["comment_html"]
                }
                comments.append(comment_obj)
        return comments
    else:
        print(f"⚠️ Could not parse Claude output for issue {issue_name}")
        return []

# =========================
# COMMENT PROCESSING
# =========================
def process_issue_comments(issue_data):
    """Process comments for a single issue (runs in thread)"""
    issue, project_slug = issue_data
    
    issue_name = issue.get("name", "Untitled")
    
    print(f"    🔄 Generating comments for: {issue_name}")
    
    # Generate comments using Claude
    comments = generate_comments_with_claude(issue, project_slug)
    
    if comments:
        print(f"    ✅ Generated {len(comments)} comments for: {issue_name}")
        return comments
    else:
        print(f"    ❌ Failed to generate comments for: {issue_name}")
        return []

def process_project_issues(project_issues_data):
    """Process all issues for a project slug"""
    project_slug, issues = project_issues_data
    
    print(f"  🔄 Processing project: {project_slug}")
    print(f"    📋 Found {len(issues)} issues in {project_slug}")
    
    # Process issues in smaller batches to manage Claude API rate limiting
    batch_size = 3  # Process 3 issues at a time
    total_comments = 0
    
    for i in range(0, len(issues), batch_size):
        batch_issues = issues[i:i+batch_size]
        batch_comments = []
        
        print(f"    📝 Processing issues batch {i//batch_size + 1}/{(len(issues) + batch_size - 1)//batch_size}")
        
        # Process each issue in the batch
        for issue in batch_issues:
            issue_data = (issue, project_slug)
            comments = process_issue_comments(issue_data)
            batch_comments.extend(comments)
        
        # Save this batch of comments
        if batch_comments:
            save_comments_batch(batch_comments)
            total_comments += len(batch_comments)
            print(f"    💾 Saved {len(batch_comments)} comments from batch")
        
        # Small delay between batches to respect rate limits
        time.sleep(1)
    
    print(f"  ✅ Completed {project_slug}: {total_comments} total comments generated")
    return total_comments

# =========================
# MAIN SCRIPT
# =========================
def main():
    print("🚀 Starting standalone comment generation (no API connection needed)...")
    
    # Load issues from file
    issues = load_issues_from_file()
    
    # Group issues by project_slug
    project_issues = {}
    for issue in issues:
        project_slug = issue.get("project_slug", "unknown/unknown")
        if project_slug not in project_issues:
            project_issues[project_slug] = []
        project_issues[project_slug].append(issue)
    
    print(f"📊 Issues grouped by project:")
    for project_slug, project_issue_list in project_issues.items():
        print(f"  📋 {project_slug}: {len(project_issue_list)} issues")
    
    print(f"\n🚀 Starting multithreaded comment generation for {len(project_issues)} projects...")
    print(f"Using {MAX_WORKERS} worker threads with rate limiting")
    
    total_comments_generated = 0
    
    # Process projects in parallel
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all project tasks
        future_to_project = {
            executor.submit(process_project_issues, (project_slug, issues_list)): project_slug 
            for project_slug, issues_list in project_issues.items()
        }
        
        # Collect results as they complete
        completed_projects = 0
        for future in as_completed(future_to_project):
            project_slug = future_to_project[future]
            try:
                comments_count = future.result()
                total_comments_generated += comments_count
                
                completed_projects += 1
                print(f"📊 Progress: {completed_projects}/{len(project_issues)} projects completed")
                
            except Exception as e:
                print(f"❌ Error processing project {project_slug}: {e}")
                completed_projects += 1

    print(f"\n🎉 Comment generation complete!")
    print(f"📊 Final Statistics:")
    print(f"   - Projects processed: {len(project_issues)}")
    print(f"   - Total issues processed: {len(issues)}")
    print(f"   - Total comments generated: {total_comments_generated}")
    print(f"   - Average comments per issue: {total_comments_generated/len(issues):.1f}" if issues else "0")
    print(f"📄 Comments saved to: {OUTPUT_FILE}")
    print("💡 Run the backfill script when your API server is running to import these comments")
    
    # Show sample of generated comments
    if all_comments:
        print(f"\n💬 Sample comments generated:")
        sample_size = min(3, len(all_comments))
        for i, comment in enumerate(all_comments[:sample_size]):
            # Extract text from HTML for display
            html_text = comment["comment_html"]
            clean_text = re.sub(r'<[^>]+>', '', html_text)
            print(f"   {i+1}. {comment['issue_name']}: {clean_text[:80]}...")

if __name__ == "__main__":
    main()