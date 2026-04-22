import requests
import base64
import json

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────
EMAIL = "your-email@domain.com"
API_TOKEN = "your-api-token"
WORKSPACE_ID = "your-workspace-id"
SCHEMA_ID = "your-schema-id"

BASE_URL = f"https://api.atlassian.com/jsm/assets/workspace/{WORKSPACE_ID}/v1"

credentials = base64.b64encode(f"{EMAIL}:{API_TOKEN}".encode()).decode()
HEADERS = {
    "Authorization": f"Basic {credentials}",
    "Accept": "application/json",
    "Content-Type": "application/json"
}


# ─────────────────────────────────────────
# MAIN FUNCTION
# ─────────────────────────────────────────
def fetch_and_save_assets(schema_id, output_file="assets_output.json"):
    all_objects = []
    start_at = 0
    max_results = 50
    is_last = False

    print(f"Fetching assets for schema ID: {schema_id}...")

    # Paginate through all objects
    while not is_last:
        payload = {
            "qlQuery": f"objectSchemaId = {schema_id}",
            "startAt": start_at,
            "maxResults": max_results
        }

        response = requests.post(
            f"{BASE_URL}/object/aql",
            headers=HEADERS,
            json=payload
        )

        if response.status_code != 200:
            print(f"Error {response.status_code}: {response.text}")
            return

        data = response.json()
        objects = data.get("values", [])
        all_objects.extend(objects)
        is_last = data.get("isLast", True)
        start_at += max_results

        print(f"  Fetched {len(all_objects)} objects so far...")

    # Save to JSON
    with open(output_file, "w") as f:
        json.dump(all_objects, f, indent=2)

    # Print summary
    print(f"\nTotal assets fetched: {len(all_objects)}")
    print(f"Saved to {output_file}")
    print("\nAsset Summary:")
    for obj in all_objects:
        print(f"  ID: {obj.get('id')}  |  Name: {obj.get('name')}  |  Type: {obj.get('objectType', {}).get('name')}")


# ─────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────
if __name__ == "__main__":
    fetch_and_save_assets(SCHEMA_ID)
