"""
Builds hf_candidates_viewer.html — a self-contained, searchable/sortable table of
the Search & LLM Ops HF candidates. Reads hf_search_llm_ops_candidates_with_location.csv
(output of filter_with_github_location.py) and embeds the data directly in the page
(no server-side dependency beyond a static file host).
"""

import csv
import json

SOURCE_CSV = "hf_search_llm_ops_candidates_with_location.csv"
OUTPUT_HTML = "hf_candidates_viewer.html"

with open(SOURCE_CSV, newline="") as f:
    raw_rows = list(csv.DictReader(f))

rows = []
for d in raw_rows:
    location = d.get("github_location") or d.get("location_guess") or ""
    rows.append({
        "name": d.get("name") or "",
        "hf_profile_link": d.get("hf_profile_link") or "",
        "github_link": d.get("github_link") or "",
        "linkedin_link": d.get("linkedin_link") or "",
        "location": location,
        "location_source": "github" if d.get("github_location") else ("bio-guess" if d.get("location_guess") else ""),
        "org_company": d.get("org_company") or "",
        "matched_keywords": d.get("matched_keywords") or "",
        "top_repo": d.get("top_repo") or "",
        "top_repo_summary": d.get("top_repo_summary") or "",
        "last_activity": d.get("last_activity") or "",
        "prolific_signal": d.get("prolific_signal") or "",
        "jd_match_score": d.get("jd_match_score") or "",
        "summary": d.get("summary") or "",
    })

data_json = json.dumps(rows, ensure_ascii=False)

html = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Search & LLM Ops HF candidates</title>
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
  table { border-collapse:collapse; width:100%; min-width:1400px; }
  th { text-align:left; padding:10px 12px; font-size:12px; text-transform:uppercase; letter-spacing:.03em; color:var(--muted); border-bottom:1px solid var(--border); cursor:pointer; white-space:nowrap; user-select:none; position:sticky; top:0; background:#fff; }
  th.sorted { color:var(--fg); }
  td { padding:9px 12px; border-bottom:1px solid var(--border); vertical-align:top; font-size:13px; }
  tr:hover td { background:var(--row-hover); }
  td.wrap { max-width:280px; color:var(--muted); }
  td.num { text-align:right; font-variant-numeric:tabular-nums; }
  a { color:var(--accent); text-decoration:none; }
  a:hover { text-decoration:underline; }
  .pill { display:inline-block; padding:1px 7px; border-radius:99px; font-size:11px; background:#eee; color:var(--muted); margin:1px 2px 1px 0; white-space:nowrap; }
  .pill.social { background:#e6f0ee; color:var(--good); }
  .pill.loc-github { background:#e6f0ee; color:var(--good); }
  .pill.loc-guess { background:#f3ece5; color:#8a5a2b; }
  .none { color:var(--muted); font-style:italic; }
</style>
</head>
<body>
<h1>Search &amp; LLM Ops HF candidates</h1>
<p class="sub">__COUNT__ candidates with a GitHub and/or LinkedIn link, sourced from Hugging Face Hub for the RavenPack Senior ML Engineer — Search &amp; LLM Ops role. Location pulled from GitHub's public profile API where available (green pill), otherwise a best-effort guess from HF bio text (orange pill).</p>

<div class="toolbar">
  <div class="search">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="7"/><path d="M21 21l-4.3-4.3"/></svg>
    <input id="q" type="text" placeholder="Filter by name, org, keywords, summary...">
  </div>
  <div class="filters">
    <button id="fLocation">Has location only</button>
    <button id="fGithub">Has GitHub only</button>
    <button id="fLinkedin">Has LinkedIn only</button>
  </div>
  <div class="count" id="count"></div>
</div>

<div class="tablewrap">
  <table>
    <thead>
      <tr>
        <th data-key="name" class="sorted">Name <span class="arrow">&darr;</span></th>
        <th data-key="location">Location</th>
        <th data-key="org_company">Org / company</th>
        <th data-key="matched_keywords">Matched keywords</th>
        <th>Top repo</th>
        <th data-key="last_activity">Last activity</th>
        <th>Links</th>
      </tr>
    </thead>
    <tbody id="rows"></tbody>
  </table>
</div>

<script>
const DATA = __DATA__;

let sortKey = 'name';
let sortDir = 1;
let onlyLocation = false;
let onlyGithub = false;
let onlyLinkedin = false;

function esc(s) {
  const div = document.createElement('div');
  div.textContent = s == null ? '' : s;
  return div.innerHTML;
}

function locationCell(d) {
  if (!d.location) return '<span class="none">unknown</span>';
  const cls = d.location_source === 'github' ? 'loc-github' : 'loc-guess';
  const label = d.location_source === 'github' ? 'GitHub' : 'bio-guess';
  return `<span class="pill ${cls}">${label}</span> ${esc(d.location)}`;
}

function linksCell(d) {
  const pills = [];
  if (d.hf_profile_link) pills.push(`<a class="pill social" href="${esc(d.hf_profile_link)}" target="_blank" rel="noopener">HF</a>`);
  if (d.github_link) pills.push(`<a class="pill social" href="${esc(d.github_link)}" target="_blank" rel="noopener">GitHub</a>`);
  if (d.linkedin_link) pills.push(`<a class="pill social" href="${esc(d.linkedin_link)}" target="_blank" rel="noopener">LinkedIn</a>`);
  return pills.join('') || '<span class="none">none</span>';
}

function render() {
  const q = document.getElementById('q').value.trim().toLowerCase();
  let filtered = DATA.filter(d => {
    if (onlyLocation && !d.location) return false;
    if (onlyGithub && !d.github_link) return false;
    if (onlyLinkedin && !d.linkedin_link) return false;
    if (!q) return true;
    return (d.name + ' ' + d.org_company + ' ' + d.matched_keywords + ' ' + d.summary + ' ' + d.top_repo_summary)
      .toLowerCase().includes(q);
  });
  filtered.sort((a, b) => {
    let av = a[sortKey], bv = b[sortKey];
    if (typeof av === 'string') { av = av.toLowerCase(); bv = bv.toLowerCase(); }
    if (av < bv) return -1 * sortDir;
    if (av > bv) return 1 * sortDir;
    return 0;
  });

  document.getElementById('count').textContent = filtered.length + ' / ' + DATA.length + ' candidates';

  const tbody = document.getElementById('rows');
  tbody.innerHTML = filtered.map(d => {
    return `<tr>
      <td>${d.hf_profile_link ? `<a href="${esc(d.hf_profile_link)}" target="_blank" rel="noopener">${esc(d.name)}</a>` : esc(d.name)}</td>
      <td>${locationCell(d)}</td>
      <td>${esc(d.org_company) || '<span class="none">—</span>'}</td>
      <td class="wrap">${esc(d.matched_keywords) || '<span class="none">—</span>'}</td>
      <td class="wrap" title="${esc(d.top_repo_summary)}">${esc(d.top_repo) || '<span class="none">—</span>'}</td>
      <td>${esc(d.last_activity) || '<span class="none">—</span>'}</td>
      <td>${linksCell(d)}</td>
    </tr>`;
  }).join('');
}

document.getElementById('q').addEventListener('input', render);
document.getElementById('fLocation').addEventListener('click', () => {
  onlyLocation = !onlyLocation;
  document.getElementById('fLocation').classList.toggle('active', onlyLocation);
  render();
});
document.getElementById('fGithub').addEventListener('click', () => {
  onlyGithub = !onlyGithub;
  document.getElementById('fGithub').classList.toggle('active', onlyGithub);
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
    if (sortKey === key) { sortDir *= -1; } else { sortKey = key; sortDir = 1; }
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

with open(OUTPUT_HTML, "w") as f:
    f.write(html)

print(f"Written {OUTPUT_HTML} ({len(html)} bytes, {len(rows)} candidates)")
