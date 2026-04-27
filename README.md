# Meetup Profiles Scraper

Fetches the attendee list for a Meetup event and enriches each profile with bio, location, and social network links via Meetup's internal GraphQL API.

## What it does

1. **Fetches all RSVPs** for a given Meetup event (paginated)
2. **Enriches each member** with sidebar profile data: city, country, bio, social networks (LinkedIn, Instagram, Twitter), job field, and member since date
3. **Saves results** to JSON files for further processing

## Architecture

```
meetup-request.txt          # Raw browser request captured from DevTools (headers + payload)
meetup-member-info.txt      # Raw browser request for the member sidebar query

meetup_request.py           # Step 1 — fetches all event attendees (paginated)
                            #   → output: meetup_response.json

meetup_member_info.py       # Step 2 — enriches each member with profile details
                            #   → reads:  meetup_response.json
                            #   → output: meetup_members_details.json

meetup_response.json        # Raw attendee list (44 entries, all RSVP statuses)
meetup_members_details.json # Enriched member profiles
```

## How it works

Both scripts call `https://www.meetup.com/gql2`, Meetup's internal GraphQL endpoint, using **Apollo Persisted Queries** — a SHA-256 hash identifies the query server-side, so no query string is needed.

**Step 1 — `meetup_request.py`**
- Operation: `getEventByIdForAttendees`
- Paginates using cursor-based navigation (`after` + `endCursor`) until `hasNextPage` is false
- Filters RSVPs by status: `YES`, `ATTENDED`, `NO_SHOW`, `EXCUSED_ABSENCE`

**Step 2 — `meetup_member_info.py`**
- Operation: `getSidebarInfo`
- Loops through all member IDs from `meetup_response.json`
- Requires an authenticated session (auth token cookie) to access profile data
- Adds a 300ms delay between requests to avoid rate limiting

## Output fields (`meetup_members_details.json`)

| Field | Description |
|---|---|
| `id` | Meetup member ID |
| `name` | Display name |
| `email` | Email address (returned empty by Meetup even when authenticated) |
| `city` | City |
| `country` | Country code |
| `bio` | Profile bio |
| `social_networks` | List of linked accounts (LinkedIn, Instagram, Twitter) with URLs |
| `job_field` | Job field if set |
| `member_since` | Account creation date |
| `events_attended` | Total RSVPs count |

## Authentication

`meetup_request.py` works without authentication (public event data).  
`meetup_member_info.py` requires the `__meetup_auth_access_token` cookie (and session cookies) from an active Meetup browser session. Update the `cookie` field in the headers with a fresh token when it expires.

## Dependencies

```
pip install requests
```
