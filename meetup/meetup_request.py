import json
import requests

url = "https://www.meetup.com/gql2"

headers = {
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "fr-FR",
    "apollographql-client-name": "nextjs-web",
    "baggage": (
        "sentry-environment=production,"
        "sentry-release=30c955f8293b0d7276c3814de099afae4db10e3d,"
        "sentry-public_key=5d12cd2317664353456ab4c40d079af2,"
        "sentry-trace_id=ae1945f95c398637fa41f210a10b6a0e,"
        "sentry-org_id=6787,"
        "sentry-sampled=false,"
        "sentry-sample_rand=0.6756206712449033,"
        "sentry-sample_rate=0.1"
    ),
    "content-type": "application/json",
    "dnt": "1",
    "origin": "https://www.meetup.com",
    "priority": "u=1, i",
    "referer": (
        "https://www.meetup.com/fr-fr/open-sourcer/events/314040194/"
        "?eventOrigin=notifications&notificationId=%3Cinbox%3E%21333343546-1776857997173"
    ),
    "sec-ch-ua": '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "sentry-trace": "ae1945f95c398637fa41f210a10b6a0e-b906752875f834ba-0",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
    ),
}

payload = {
    "operationName": "getEventByIdForAttendees",
    "variables": {
        "eventId": "314040194",
        "first": 20,
        "filter": {
            "rsvpStatus": ["YES", "ATTENDED", "NO_SHOW", "EXCUSED_ABSENCE"]
        },
        "sort": {
            "sortField": "RELEVANCE",
            "sortOrder": "DESC",
            "hostsFirst": True
        }
    },
    "extensions": {
        "persistedQuery": {
            "version": 1,
            "sha256Hash": "0ce7a205b999d4da76a331d142286f1fb4250cead193d3d6888aad258342bd7a"
        }
    },
}

all_edges = []
cursor = None

while True:
    payload["variables"]["first"] = 20
    if cursor:
        payload["variables"]["after"] = cursor
    elif "after" in payload["variables"]:
        del payload["variables"]["after"]

    response = requests.post(url, json=payload, headers=headers)
    data = response.json()

    rsvps = data["data"]["event"]["rsvps"]
    all_edges.extend(rsvps["edges"])

    page_info = rsvps["pageInfo"]
    if not page_info["hasNextPage"]:
        break
    cursor = page_info["endCursor"]

data["data"]["event"]["rsvps"]["edges"] = all_edges

with open("meetup_response.json", "w") as f:
    json.dump(data, f, indent=2, ensure_ascii=False)

names = [e["node"]["member"]["name"] for e in all_edges]
print(f"Total participants fetched: {len(names)}")
for i, name in enumerate(names, 1):
    print(f"{i}. {name}")
