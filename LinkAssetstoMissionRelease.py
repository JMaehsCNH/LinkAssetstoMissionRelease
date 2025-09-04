#!/usr/bin/env python3
import os
import sys
import json
import textwrap
import requests

# -------------------------
# ENV VARS (set these)
# -------------------------
JIRA_SITE            = os.environ.get("JIRA_SITE")            # e.g. "https://yourcompany.atlassian.net"
JIRA_EMAIL           = os.environ.get("JIRA_EMAIL")
JIRA_API_TOKEN       = os.environ.get("JIRA_API_TOKEN")
ASSETS_WORKSPACE_ID  = os.environ.get("ASSETS_WORKSPACE_ID")  # e.g. "01hxxxxxxx..." (from Assets settings)

ISSUE_KEY            = os.environ.get("ISSUE_KEY")            # e.g. "PREC-123"

# Example selections you want to add (Category -> Name)
# You can feed these from your UI/automation instead of hardcoding
SELECTIONS = [
    {"category": "Displays",            "name": "11100411"},
    {"category": "PCM Devices",         "name": "217646000000000"},
    {"category": "DeweSoft Computers",  "name": "DB22020784"},
    {"category": "CSS Loggers",         "name": "000005"},
]

# -------------------------
# Helpers
# -------------------------
AUTH = (JIRA_EMAIL, JIRA_API_TOKEN)
JSON_HEADERS = {"Accept": "application/json", "Content-Type": "application/json"}

def die(msg):
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)

def assets_aql_search(category: str, name: str):
    """
    Search Assets for an object matching a simple Category + Name logic.
    Adjust AQL as needed to match your schema/attribute names.
    """
    # Common attribute names are often "Name" or "Serial number" (SN).
    # If your CMDB uses a different attribute name, change it here.
    aql = f'objectType = "{category}" AND (Name = "{name}" OR "Serial Number" = "{name}")'

    url = f"{JIRA_SITE}/jsm/assets/workspace/{ASSETS_WORKSPACE_ID}/v1/object/aql"
    payload = {"qlQuery": aql, "page": 1, "resultPerPage": 2}
    r = requests.post(url, headers=JSON_HEADERS, auth=AUTH, data=json.dumps(payload))
    if r.status_code != 200:
        die(f"Assets AQL search failed ({r.status_code}): {r.text}")

    data = r.json() or {}
    objs = data.get("objectEntries") or []
    return objs[0] if objs else None

def build_asset_url(object_id: int):
    """
    Build a clickable CMDB URL to the asset.
    Validate once by opening an asset in your site UI and copying the path.
    This pattern works on most Jira Cloud sites:
    """
    return f"{JIRA_SITE}/jira/servicedesk/assets/objects/{object_id}"

def create_remote_link(issue_key: str, link_url: str, title: str):
    """
    Add a web (remote) link to the issue with the given title.
    """
    url = f"{JIRA_SITE}/rest/api/3/issue/{issue_key}/remotelink"
    payload = {
        "object": {
            "title": title,
            "url": link_url
        }
    }
    r = requests.post(url, headers=JSON_HEADERS, auth=AUTH, data=json.dumps(payload))
    if r.status_code not in (200, 201):
        die(f"Failed to create remote link ({r.status_code}): {r.text}")

def get_issue_description(issue_key: str):
    url = f"{JIRA_SITE}/rest/api/3/issue/{issue_key}?fields=description"
    r = requests.get(url, headers=JSON_HEADERS, auth=AUTH)
    if r.status_code != 200:
        die(f"Failed to read issue ({r.status_code}): {r.text}")
    fields = r.json().get("fields", {})
    # If your project uses Jira's new rich-text document format,
    # you can still append plain text via 'description' as a string;
    # Jira will convert it. Or you can post Atlassian Document Format (ADF).
    return fields.get("description") or ""

def set_issue_description(issue_key: str, new_text: str):
    url = f"{JIRA_SITE}/rest/api/3/issue/{issue_key}"
    payload = {"fields": {"description": new_text}}
    r = requests.put(url, headers=JSON_HEADERS, auth=AUTH, data=json.dumps(payload))
    if r.status_code != 204:
        die(f"Failed to update description ({r.status_code}): {r.text}")

def append_tree_section(original: str, items_by_category: dict) -> str:
    """
    Append a markdown-like bulleted tree to the existing description.
    Renders like:
      - CSS Loggers
        - 000003
        - 000005
    """
    lines = ["", "## Assets", ""]
    for cat, names in items_by_category.items():
        lines.append(f"- {cat}")
        for nm in names:
            lines.append(f"  - {nm}")
    return (original or "") + "\n" + "\n".join(lines) + "\n"

# -------------------------
# Main
# -------------------------
if __name__ == "__main__":
    for v, n in [(JIRA_SITE, "JIRA_SITE"), (JIRA_EMAIL, "JIRA_EMAIL"),
                 (JIRA_API_TOKEN, "JIRA_API_TOKEN"), (ASSETS_WORKSPACE_ID, "ASSETS_WORKSPACE_ID"),
                 (ISSUE_KEY, "ISSUE_KEY")]:
        if not v:
            die(f"Missing required env var: {n}")

    # Group names by category for the tree we’ll append to Notes/Description
    items_by_cat = {}

    for sel in SELECTIONS:
        cat = sel["category"]
        nm  = sel["name"]

        obj = assets_aql_search(cat, nm)
        if not obj:
            print(f"Skip: No asset found for {cat} / {nm}")
            continue

        object_id = obj.get("id")
        # Prefer to show exactly what the user sees in the notes tree:
        title = f"{cat} - {nm}"
        url   = build_asset_url(object_id)

        create_remote_link(ISSUE_KEY, url, title)
        items_by_cat.setdefault(cat, []).append(nm)

        print(f"Linked: {title} -> {url}")

    # Append the tree to the issue’s Description (or swap to a custom “Notes” field)
    if items_by_cat:
        current = get_issue_description(ISSUE_KEY)
        updated = append_tree_section(current, items_by_cat)
        set_issue_description(ISSUE_KEY, updated)
        print("Description updated with Assets section.")
    else:
        print("No assets linked; description not changed.")
