import os, sys, json, time, re
import requests

# ---- FIXED BASE URL + SCHEMA ----
JIRA_SITE = "https://cnhpd.atlassian.net"
OBJECT_SCHEMA_ID = "3"  # PV Assets

# ---- REQUIRED ENVS ----
JIRA_EMAIL          = os.environ.get("JIRA_EMAIL")
JIRA_API_TOKEN      = os.environ.get("JIRA_API_TOKEN")
ASSETS_WORKSPACE_ID = os.environ.get("ASSETS_WORKSPACE_ID")
JQL                 = os.environ.get("JQL") or (
    'project = PREC AND issuetype in ("Mission/Release") AND status != "Validated (Complete)"'
)

if not all([JIRA_EMAIL, JIRA_API_TOKEN, ASSETS_WORKSPACE_ID]):
    print("Missing env(s): JIRA_EMAIL, JIRA_API_TOKEN, ASSETS_WORKSPACE_ID", file=sys.stderr)
    sys.exit(1)

AUTH = (JIRA_EMAIL, JIRA_API_TOKEN)
H    = {"Accept":"application/json","Content-Type":"application/json"}

def die(msg, r=None):
    if r is not None:
        print(f"{msg} ({r.status_code}): {r.text}", file=sys.stderr)
    else:
        print(msg, file=sys.stderr)
    sys.exit(1)

# ---- Jira Search (enhanced JQL) ----
def search_issues(jql, next_token=None, max_results=100):
    """
    Uses the new enhanced JQL endpoint with cursor pagination.
    Docs: /rest/api/3/search/jql (GET) + nextPageToken
    """
    url = f"{JIRA_SITE}/rest/api/3/search/jql"
    params = {
        "jql": jql,
        "maxResults": max_results,  # Jira may cap/ignore; still pass a sensible value
        # You can also add "fields" here, but Atlassian recommends using 'fields' in a POST body;
        # for simplicity we GET everything and read fields later per issue.
    }
    if next_token:
        params["nextPageToken"] = next_token

    r = requests.get(url, headers=H, auth=AUTH, params=params)
    if r.status_code != 200:
        die("Jira enhanced search failed", r)
    return r.json()

# ---- Main loop (cursor-based) ----
total_scanned = 0
next_token = None

while True:
    page = search_issues(JQL, next_token=next_token, max_results=100)
    issues = page.get("issues", [])

    if not issues:
        break

    for issue in issues:
        key   = issue["key"]
        fields= issue.get("fields") or {}
        proj  = fields.get("project", {}).get("key")
        itype = fields.get("issuetype", {}).get("name")
        status= fields.get("status", {}).get("name")

        # Double-guard
        if proj != "PREC" or itype not in ("Mission","Release") or status == "Validated (Complete)":
            continue

        desc = fields.get("description") or ""
        pairs = extract_pairs(desc if isinstance(desc, str) else get_issue_desc(key))
        if not pairs:
            print(f"{key}: no asset tree detected; skipping")
            continue

        existing = list_remote_links(key)
        for cat, nm in pairs:
            obj = aql_lookup(cat, nm)
            if not obj:
                print(f"{key}: not found in Assets (schema 3) → {cat} / {nm}")
                continue
            oid = obj.get("id")
            url_ = asset_url(oid)
            title = f"{cat} - {nm}"

            if (title, url_) in existing:
                print(f"{key}: link already exists → {title}")
                continue

            create_remote_link(key, title, url_)
            existing.add((title, url_))
            print(f"{key}: linked {title}")

        total_scanned += 1

    # move the cursor forward; stop when no token is returned
    next_token = page.get("nextPageToken")
    if not next_token:
        break

print(f"Done. Issues scanned: {total_scanned}")


def get_issue_desc(key):
    url = f"{JIRA_SITE}/rest/api/3/issue/{key}?fields=description"
    r = requests.get(url, headers=H, auth=AUTH)
    if r.status_code != 200:
        die(f"Failed to read description for {key}", r)
    return r.json().get("fields",{}).get("description") or ""

def list_remote_links(key):
    url = f"{JIRA_SITE}/rest/api/3/issue/{key}/remotelink"
    r = requests.get(url, headers=H, auth=AUTH)
    if r.status_code != 200:
        die(f"Failed to list remote links for {key}", r)
    links = r.json() if isinstance(r.json(), list) else []
    existing = set()
    for l in links:
        obj = l.get("object",{})
        title = obj.get("title") or ""
        url   = obj.get("url") or ""
        if title and url:
            existing.add((title.strip(), url.strip()))
    return existing

def create_remote_link(key, title, url_):
    url = f"{JIRA_SITE}/rest/api/3/issue/{key}/remotelink"
    r = requests.post(url, headers=H, auth=AUTH, json={"object":{"title": title, "url": url_}})
    if r.status_code not in (200,201):
        die(f"Failed to create remote link for {key}", r)

def aql_lookup(category, name):
    aql = (
        f'objectSchemaId = {OBJECT_SCHEMA_ID} '
        f'AND objectType = "{category}" '
        f'AND (Name = "{name}" OR "Serial Number" = "{name}")'
    )
    url = f"{JIRA_SITE}/jsm/assets/workspace/{ASSETS_WORKSPACE_ID}/v1/object/aql"
    r = requests.post(url, headers=H, auth=AUTH, json={"qlQuery": aql, "page":1, "resultPerPage":2})
    if r.status_code != 200:
        die("Assets AQL search failed", r)
    data = r.json() or {}
    objs = data.get("objectEntries") or []
    return objs[0] if objs else None

def asset_url(object_id:int):
    return f"{JIRA_SITE}/jira/servicedesk/assets/objects/{object_id}"

bullet_re = re.compile(r"^(?:-|\*)\s+(?P<cat>.+?)\s*$")
child_re  = re.compile(r"^\s{2,}(?:-|\*)\s+(?P<name>.+?)\s*$")

def extract_pairs(desc_text):
    pairs = []
    current_cat = None
    text = desc_text if isinstance(desc_text, str) else ""
    for raw in (text or "").splitlines():
        line = raw.rstrip()
        m1 = bullet_re.match(line)
        if m1:
            current_cat = m1.group("cat").strip()
            continue
        m2 = child_re.match(line)
        if m2 and current_cat:
            nm = m2.group("name").strip()
            if nm:
                pairs.append((current_cat, nm))
    return pairs

def main():
    start = 0
    total_scanned = 0
    while True:
        page = search_issues(JQL, start_at=start, max_results=50)
        issues = page.get("issues", [])
        if not issues:
            break

        for issue in issues:
            key   = issue["key"]
            fields= issue["fields"]
            proj  = fields["project"]["key"]
            itype = fields["issuetype"]["name"]
            status= fields["status"]["name"]

            if proj != "PREC" or itype not in ("Mission","Release") or status == "Validated (Complete)":
                continue

            desc = fields.get("description") or ""
            pairs = extract_pairs(desc if isinstance(desc, str) else get_issue_desc(key))
            if not pairs:
                print(f"{key}: no asset tree detected; skipping")
                continue

            existing = list_remote_links(key)
            for cat, nm in pairs:
                obj = aql_lookup(cat, nm)
                if not obj:
                    print(f"{key}: not found in Assets (schema 3) → {cat} / {nm}")
                    continue
                oid = obj.get("id")
                url_ = asset_url(oid)
                title = f"{cat} - {nm}"

                if (title, url_) in existing:
                    print(f"{key}: link already exists → {title}")
                    continue

                create_remote_link(key, title, url_)
                existing.add((title, url_))
                print(f"{key}: linked {title}")

            total_scanned += 1
            time.sleep(0.25)

        start = page.get("startAt", 0) + page.get("maxResults", 0)
        if start >= page.get("total", 0):
            break

    print(f"Done. Issues scanned: {total_scanned}")

if __name__ == "__main__":
    main()
