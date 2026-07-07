import json
import time
import requests

url = "https://www.meetup.com/gql2"

headers = {
    "accept": "*/*",
    "accept-language": "fr-FR",
    "apollographql-client-name": "nextjs-web",
    "content-type": "application/json",
    "dnt": "1",
    "origin": "https://www.meetup.com",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
    ),
    "cookie": (
        "__meetup_auth_access_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJFUzI1NiJ9"
        ".eyJzdWIiOiIzMzMzNDM1NDYiLCJuYmYiOjE3NzczMDg1MTMsInJvbGUiOiJmaXJzdF9wYXJ0eSIsImlzcyI6Ii5tZWV0dXAuY29tIiwicXVhbnR1bV9sZWFwZWQiOmZhbHNlLCJyZWZyZXNoX3Rva2Vuc19jaGFpbl9pZCI6IjBlMDBmOTA2LTNlZmQtNGFmMy04ZTgyLTBiNTExYjY5NzhjYyIsImV4cCI6MTc3NzMxMjExMywiaWF0IjoxNzc3MzA4NTEzLCJqdGkiOiJjNWYyMjIzYS03NWMwLTRhODUtOWM5Mi0yMDg0YmUyNzBkN2UifQ"
        ".NK4fT7Rdxut1qDzQ_VtrQuRd5EeXWcD2TYwnPH4ql8ACnhpp7py6JuZw5evmcAacUEjtIXl95yR2IolXBQYM1Q; "
        "MEETUP_SESSION=b75290e7-3641-4cbe-93a2-a8ca49893b32; "
        "memberId=333343546; "
        "MEETUP_CSRF=34b531db-07cb-4ca2-8e72-0a434adafab3"
    ),
}


def fetch_member_info(member_id):
    payload = {
        "operationName": "getSidebarInfo",
        "variables": {"memberId": member_id},
        "extensions": {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "c0cd94d07bd64ba5b356aa8636a307fdd4661968ed1d7eda4305b58360b64a43",
            }
        },
    }
    r = requests.post(url, json=payload, headers=headers)
    return r.json().get("data", {}).get("member")


with open("meetup_response.json") as f:
    data = json.load(f)

edges = data["data"]["event"]["rsvps"]["edges"]
members = [e["node"]["member"] for e in edges]

results = []
for i, m in enumerate(members, 1):
    member_id = m["id"]
    name = m["name"]
    print(f"[{i}/{len(members)}] Fetching {name}...")

    info = fetch_member_info(member_id)
    if info:
        results.append({
            "id": member_id,
            "name": info.get("name"),
            "email": info.get("email") or None,
            "city": info.get("city"),
            "country": info.get("country"),
            "bio": info.get("bio"),
            "social_networks": info.get("socialNetworks", []),
            "job_field": info.get("jobField"),
            "member_since": info.get("startDate"),
            "events_attended": info.get("rsvps", {}).get("totalCount"),
        })
    else:
        results.append({"id": member_id, "name": name, "error": "not accessible"})

    time.sleep(0.3)

with open("meetup_members_details.json", "w") as f:
    json.dump(results, f, indent=2, ensure_ascii=False)

print(f"\nDone — {len(results)} members saved to meetup_members_details.json")
