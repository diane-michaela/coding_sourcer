"""
Builds meetup_viewer.html — a self-contained, searchable/sortable table of
enriched Meetup attendees. Reads meetup_members_details.json, embeds the data
directly in the page (no server-side dependency beyond a static file host).
Also merges in meetup_linkedin_matches.csv (from find_linkedin.py) if present.
"""

import csv
import json
import os

with open("meetup_members_details.json") as f:
    data = json.load(f)

linkedin_by_name = {}
if os.path.exists("meetup_linkedin_matches.csv"):
    with open("meetup_linkedin_matches.csv", newline="") as f:
        for row in csv.DictReader(f):
            linkedin_by_name[row["name"]] = {
                "url": row.get("linkedin_url") or "",
                "confidence": row.get("confidence") or "",
            }

rows = []
for d in data:
    member_since = (d.get("member_since") or "")[:10]
    name = d.get("name") or ""
    li = linkedin_by_name.get(name, {})
    rows.append({
        "name": name,
        "city": d.get("city") or "",
        "country": (d.get("country") or "").upper(),
        "bio": d.get("bio") or "",
        "job_field": d.get("job_field") or "",
        "social": d.get("social_networks") or [],
        "member_since": member_since,
        "events_attended": d.get("events_attended") if d.get("events_attended") is not None else 0,
        "id": d.get("id") or "",
        "linkedin_url": li.get("url", ""),
        "linkedin_confidence": li.get("confidence", ""),
    })

data_json = json.dumps(rows, ensure_ascii=False)

html = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Figma Paris meetup attendees</title>
<style>
  :root {
    --bg: #faf9f5; --fg: #141413; --muted: #6b6a66; --border: #e5e3da;
    --accent: #b04e72; --good: #3f7a75; --row-hover: #f0eee6;
  }
  * { box-sizing: border-box; }
  body { margin:0; padding:24px; font:14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; background:var(--bg); color:var(--fg); }
  h1 { font-size:20px; margin:0 0 4px; }
  .sub { color:var(--muted); margin:0 0 18px; font-size:13px; }
  .toolbar { display:flex; gap:12px; align-items:center; margin-bottom:14px; flex-wrap:wrap; }
  .search { flex:1; min-width:240px; position:relative; }
  .search input { width:100%; padding:9px 12px 9px 34px; border:1px solid var(--border); border-radius:8px; font-size:14px; background:#fff; }
  .search svg { position:absolute; left:10px; top:50%; transform:translateY(-50%); width:16px; height:16px; color:var(--muted); }
  .count { color:var(--muted); font-size:13px; white-space:nowrap; }
  .filters { display:flex; gap:8px; }
  .filters button { border:1px solid var(--border); background:#fff; border-radius:6px; padding:6px 10px; font-size:12px; cursor:pointer; }
  .filters button.active { background:var(--accent); color:#fff; border-color:var(--accent); }
  .tablewrap { overflow-x:auto; border:1px solid var(--border); border-radius:10px; background:#fff; }
  table { border-collapse:collapse; width:100%; min-width:1250px; }
  th { text-align:left; padding:10px 12px; font-size:12px; text-transform:uppercase; letter-spacing:.03em; color:var(--muted); border-bottom:1px solid var(--border); cursor:pointer; white-space:nowrap; user-select:none; position:sticky; top:0; background:#fff; }
  th.sorted { color:var(--fg); }
  td { padding:9px 12px; border-bottom:1px solid var(--border); vertical-align:top; font-size:13px; }
  tr:hover td { background:var(--row-hover); }
  td.bio { max-width:320px; color:var(--muted); }
  td.num { text-align:right; font-variant-numeric:tabular-nums; }
  a { color:var(--accent); text-decoration:none; }
  a:hover { text-decoration:underline; }
  .pill { display:inline-block; padding:1px 7px; border-radius:99px; font-size:11px; background:#eee; color:var(--muted); margin:1px 2px 1px 0; white-space:nowrap; }
  .pill.social { background:#e6f0ee; color:var(--good); }
  .pill.li-high { background:#e6f0ee; color:var(--good); }
  .pill.li-medium { background:#f3ece5; color:#8a5a2b; }
  .pill.li-low { background:#f6e6e6; color:#a4453f; }
  .none { color:var(--muted); font-style:italic; }
</style>
</head>
<body>
<h1>Figma Paris meetup attendees</h1>
<p class="sub">__COUNT__ attendees from the <a href="https://www.meetup.com/figma-paris/events/262061926/" target="_blank" rel="noopener">Figma Paris meetup event</a>, enriched with Meetup profile data.</p>

<div class="toolbar">
  <div class="search">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="7"/><path d="M21 21l-4.3-4.3"/></svg>
    <input id="q" type="text" placeholder="Filter by name, city, country, bio, job field...">
  </div>
  <div class="filters">
    <button id="fBio">Has bio only</button>
    <button id="fSocial">Has social links only</button>
    <button id="fLinkedin">Has LinkedIn only</button>
  </div>
  <div class="count" id="count"></div>
</div>

<div class="tablewrap">
  <table>
    <thead>
      <tr>
        <th data-key="events_attended" class="sorted">Events <span class="arrow">&darr;</span></th>
        <th data-key="name">Name</th>
        <th data-key="city">City</th>
        <th data-key="country">Country</th>
        <th data-key="job_field">Job field</th>
        <th>Bio</th>
        <th>Social</th>
        <th>LinkedIn</th>
        <th data-key="member_since">Member since</th>
      </tr>
    </thead>
    <tbody id="rows"></tbody>
  </table>
</div>

<script>
const DATA = __DATA__;

let sortKey = 'events_attended';
let sortDir = -1;
let onlyBio = false;
let onlySocial = false;
let onlyLinkedin = false;

function esc(s) {
  const div = document.createElement('div');
  div.textContent = s == null ? '' : s;
  return div.innerHTML;
}

function socialPills(list) {
  return (list || []).map(s => `<a class="pill social" href="${esc(s.url)}" target="_blank" rel="noopener">${esc(s.service)}</a>`).join('');
}

function linkedinCell(d) {
  if (!d.linkedin_url) {
    return d.linkedin_confidence === 'skipped (name too short/generic)'
      ? '<span class="none">skipped</span>'
      : '<span class="none">no match</span>';
  }
  const cls = 'li-' + (d.linkedin_confidence || 'low');
  return `<a class="pill ${cls}" href="${esc(d.linkedin_url)}" target="_blank" rel="noopener">${esc(d.linkedin_confidence)}</a>`;
}

function render() {
  const q = document.getElementById('q').value.trim().toLowerCase();
  let filtered = DATA.filter(d => {
    if (onlyBio && !d.bio) return false;
    if (onlySocial && (!d.social || !d.social.length)) return false;
    if (onlyLinkedin && !d.linkedin_url) return false;
    if (!q) return true;
    return (d.name + ' ' + d.city + ' ' + d.country + ' ' + d.bio + ' ' + d.job_field)
      .toLowerCase().includes(q);
  });
  filtered.sort((a, b) => {
    let av = a[sortKey], bv = b[sortKey];
    if (typeof av === 'string') { av = av.toLowerCase(); bv = bv.toLowerCase(); }
    if (av < bv) return -1 * sortDir;
    if (av > bv) return 1 * sortDir;
    return 0;
  });

  document.getElementById('count').textContent = filtered.length + ' / ' + DATA.length + ' attendees';

  const tbody = document.getElementById('rows');
  tbody.innerHTML = filtered.map(d => {
    return `<tr>
      <td class="num">${d.events_attended}</td>
      <td>${esc(d.name)}</td>
      <td>${esc(d.city) || '<span class="none">—</span>'}</td>
      <td>${esc(d.country) || '<span class="none">—</span>'}</td>
      <td>${esc(d.job_field) || '<span class="none">—</span>'}</td>
      <td class="bio">${esc(d.bio) || '<span class="none">—</span>'}</td>
      <td>${d.social && d.social.length ? socialPills(d.social) : '<span class="none">none</span>'}</td>
      <td>${linkedinCell(d)}</td>
      <td>${esc(d.member_since) || '<span class="none">—</span>'}</td>
    </tr>`;
  }).join('');
}

document.getElementById('q').addEventListener('input', render);
document.getElementById('fBio').addEventListener('click', () => {
  onlyBio = !onlyBio;
  document.getElementById('fBio').classList.toggle('active', onlyBio);
  render();
});
document.getElementById('fSocial').addEventListener('click', () => {
  onlySocial = !onlySocial;
  document.getElementById('fSocial').classList.toggle('active', onlySocial);
  render();
});
document.getElementById('fLinkedin').addEventListener('click', () => {
  onlyLinkedin = !onlyLinkedin;
  document.getElementById('fLinkedin').classList.toggle('active', onlyLinkedin);
  render();
});

document.querySelectorAll('thead th[data-key]').forEach(th => {
  th.addEventListener('click', () => {
    const key = th.dataset.key;
    if (sortKey === key) { sortDir *= -1; } else { sortKey = key; sortDir = -1; }
    document.querySelectorAll('thead th').forEach(t => {
      t.classList.remove('sorted');
      t.querySelector('.arrow')?.remove();
    });
    th.classList.add('sorted');
    const arrow = document.createElement('span');
    arrow.className = 'arrow';
    arrow.innerHTML = sortDir === 1 ? '&uarr;' : '&darr;';
    th.appendChild(arrow);
    render();
  });
});

render();
</script>
</body>
</html>"""

html = html.replace("__DATA__", data_json).replace("__COUNT__", str(len(rows)))

with open("meetup_viewer.html", "w") as f:
    f.write(html)

print(f"Written meetup_viewer.html ({len(html)} bytes, {len(rows)} attendees)")
