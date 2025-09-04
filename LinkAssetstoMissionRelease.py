#!/usr/bin/env python3
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
    'project = PREC AND issuetype = "Mission/Release" AND status != "Validated (Complete)"'
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

# ---------- Enhanced JQL search with cursor pagination ----------
def search_get(jql, next_token=None, fields=None, max_results=100):
    url = f"{JIRA_SITE}/rest/api/3/search/jql"
    params = {"jql": jql, "maxResults": max_results}
    if fields:
        params["fields"] = ",".join(fields)
    if next_token:
        params["nextPageToken"] = next_token
    return requests.get(url, headers=H, auth=AUTH, params=params)

def search_post(jql, next_token=None, fields=None, max_results=100):
    url = f"{JIRA_SITE}/rest/api/3/search/jql"
    body = {"jql": jql, "maxResults": max_results}
    if fields:
        body["fields"] = fields
    if next_token:
        body["nextPageToken"] = next_token
    return requests.post(url, headers=H, auth=AUTH, json=body)

def enhanced_search(jql, wanted_fields):
    next_token = None
    while True:
        r = search_get(jql, next_token=next_token, fields=wanted_fields)
        if r.status_code in (404, 405, 410):
            r = search_post(jql, next_token=next_token, fields=wanted_fields)
        if r.status_code != 200:
            die("Jira enhanced search failed", r)
        page = r.json()
        for issue in page.get("issues", []):
            yield issue
        next_token = page.get("nextPageToken")
        if not next_token:
            break

# ---------- Issue helpers ----------
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

# ---------- Assets lookup (schema 3) ----------
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

# ---------- Parse the bullet tree ----------
bullet_re = re.compile(r"^(?:-|\*)\s+(?P<cat>.+?)\s*$")
child_re  = re.compile(r"^\s{2,}(?:-|\*)\s+(?P<name>.+?)\s*$")

# ---------- Parse the bullet tree (supports Markdown *and* ADF) ----------
bullet_re = re.compile(r"^(?:-|\*)\s+(?P<cat>.+?)\s*$")
child_re  = re.compile(r"^\s{2,}(?:-|\*)\s+(?P<name>.+?)\s*$")

def _adf_text_from_paragraph(node):
    """Concatenate text content from an ADF paragraph node."""
    if not node or node.get("type") != "paragraph":
        return ""
    out = []
    for frag in node.get("content", []) or []:
        if frag.get("type") == "text" and "text" in frag:
            out.append(frag["text"])
    return "".join(out).strip()

def _adf_pairs_from_list(list_node):
    """
    Given an ADF bulletList node, return [(Category, Name), ...]
    We treat each top-level listItem's paragraph as a Category,
    and any nested bulletList items under it as Names.
    """
    pairs = []
    if not list_node or list_node.get("type") != "bulletList":
        return pairs

    for li in list_node.get("content", []) or []:  # listItem nodes
        if li.get("type") != "listItem":
            continue
        # ListItem usually has: [ paragraph, (optional) bulletList ]
        children = li.get("content", []) or []
        if not children:
            continue

        # First paragraph = category
        cat = None
        for ch in children:
            if ch.get("type") == "paragraph":
                cat = _adf_text_from_paragraph(ch)
                break
        if not cat:
            continue

        # Find nested bulletList(s) for names
        has_child = False
        for ch in children:
            if ch.get("type") == "bulletList":
                for sub_li in ch.get("content", []) or []:
                    if sub_li.get("type") != "listItem":
                        continue
                    # name = first paragraph text inside the sub listItem
                    name = ""
                    for sub_ch in sub_li.get("content", []) or []:
                        if sub_ch.get("type") == "paragraph":
                            name = _adf_text_from_paragraph(sub_ch)
                            if name:
                                break
                    if name:
                        pairs.append((cat, name))
                        has_child = True
        # If no nested list, treat the category itself as a single-name row? (not our use case)
    return pairs

def extract_pairs(desc):
    """
    Accepts either a plain string (Markdown-ish) or an ADF dict.
    Returns list of (Category, Name).
    """
    if isinstance(desc, dict):  # ADF
        pairs = []
        for node in (desc.get("content") or []):
            if node.get("type") == "bulletList":
                pairs.extend(_adf_pairs_from_list(node))
        return pairs

    # Fallback: plain text bullets with indentation
    text = desc if isinstance(desc, str) else ""
    pairs, current_cat = [], None
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

# ---------- Main ----------
def main():
    wanted_fields = ["summary","description","issuetype","project","status"]
    total_scanned = 0

    for issue in enhanced_search(JQL, wanted_fields):
        key    = issue["key"]
        fields = issue.get("fields") or {}
        proj   = fields.get("project", {}).get("key")
        itype  = fields.get("issuetype", {}).get("name")
        status = fields.get("status", {}).get("name")

        # Only PREC + exact type + not Validated (Complete)
        if proj != "PREC" or itype != "Mission/Release" or status == "Validated (Complete)":
            continue

        desc = fields.get("description") or get_issue_desc(key)  # may be ADF dict or string
        pairs = extract_pairs(desc)

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
        time.sleep(0.2)

    print(f"Done. Issues scanned: {total_scanned}")

if __name__ == "__main__":
    main()
