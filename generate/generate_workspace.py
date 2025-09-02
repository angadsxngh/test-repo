import json
import os
from openai import OpenAI
from dotenv import load_dotenv
import anthropic

load_dotenv()

api_key = os.getenv("ANTHROPIC_API_KEY")
if not api_key:
    print("❌ OPENAI_API_KEY not found in .env file")
    exit(1)

# client = OpenAI(api_key=api_key)
client = anthropic.Anthropic(api_key=api_key)
NUM_WORKSPACES = 1
OUTPUT_FILE = "../generated_json/workspace.json"

def parse_json_response(text: str) -> dict:
    text = text.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        # Remove the first and last triple backtick lines
        lines = [line for line in lines if not line.strip().startswith("```")]
        text = "\n".join(lines).strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        print(f"❌ Failed to parse JSON:\n{text}\n\nError: {e}")
        return None

def generate_workspace():
    print("🤖 [GEN] Generating Workspace...")

    prompt = (
        "You need to generate data for a series B company"
        "Generate a realistic unique company name and slug. "
        "The name should be a company name and the slug should be a unique slug for the company. "
        "Return only a JSON object with keys: name, slug and organization_size which can have values as: [Just myself, 2-10,11-50,51-200,201-500,500+]. "
        "No extra commentary or markdown formatting."
    )

    try:
        # response = client.chat.completions.create(
        #     model="gpt-4o-mini",
        #     messages=[
        #         {"role": "system", "content": "You are a helpful assistant."},
        #         {"role": "user", "content": prompt}
        #     ]
        # )

        message = client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=1000,
            temperature=1,
            system="You are a generator of realistic unique data workspace data which feels authentic.",
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
    except Exception as e:
        print(f"❌ Error calling Claude API: {e}")
        return

    # Check if the response contains choices
    if not message.content[0].text.strip():
        print("❌ No valid response from Claude.")
        return

    text = message.content[0].text.strip()
    print(f"🧾 Claude raw response:\n{text}")

    data = parse_json_response(text)
    if not data:
        print("❌ JSON parsing failed. No file saved.")
        return None

    return data

# Run the function
# generate_workspace()

def main():
    print(f"👥 Generating {NUM_WORKSPACES} workspaces...")
    workspaces = []

    for i in range(NUM_WORKSPACES):
        print(f"🔄 Generating workspace {i + 1}...")

        workspace = generate_workspace()
        print("📤 Generated workspace:", workspace)

        if workspace:
            workspaces.append(workspace)
        else:
            print(f"⚠️ Skipping workspace {i + 1} due to error.")

    print(f"✅ Saving {len(workspaces)} workspaces to {OUTPUT_FILE}")
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    with open(OUTPUT_FILE, "w") as f:
        json.dump(workspaces, f, indent=2)

    print("🎉 Done!")



if __name__ == "__main__":
    main()

