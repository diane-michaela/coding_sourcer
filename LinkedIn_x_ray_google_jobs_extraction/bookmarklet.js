/*
 * LinkedIn Job Search Extractor -- bookmarklet
 * ---------------------------------------------
 * Paginates through a Google search of LinkedIn job postings
 * (site:linkedin.com/jobs/view ...), collects every unique job
 * link it finds, and downloads them as a CSV file (Title, URL).
 *
 * See README.md in this folder for installation instructions
 * (macOS and Windows) and usage steps.
 */

(async function () {
  const linksMap = new Map();
  const url = new URL(location.href);
  const maxPages = 30;   // safety cap on number of result pages to fetch
  const delayMs = 1200;  // pause between page requests, in milliseconds

  for (let page = 0; page < maxPages; page++) {
    url.searchParams.set('start', page * 10);
    const res = await fetch(url.toString(), { credentials: 'include' });
    const html = await res.text();
    const doc = new DOMParser().parseFromString(html, 'text/html');
    const anchors = [...doc.querySelectorAll('a[href*="linkedin.com/jobs/view"]')];

    const before = linksMap.size;
    anchors.forEach((a) => {
      // Google's "Translate this page" links wrap the real result URL
      // inside a translate.goog host, and unrelated UI links (e.g. "AI
      // Mode") can carry it as a tracking param -- unwrap/reject those
      // so only genuine linkedin.com/jobs/view pages get through.
      let parsed;
      try {
        parsed = new URL(a.href);
      } catch (e) {
        return;
      }
      if (/\.translate\.goog$/i.test(parsed.hostname)) {
        const inner = parsed.searchParams.get('u');
        if (!inner) return;
        try {
          parsed = new URL(inner);
        } catch (e) {
          return;
        }
      }
      if (!parsed.hostname.endsWith('linkedin.com') || !parsed.pathname.startsWith('/jobs/view')) {
        return;
      }
      const clean = parsed.origin + parsed.pathname;
      if (!linksMap.has(clean)) {
        linksMap.set(clean, (a.textContent || '').trim().replace(/\s+/g, ' '));
      }
    });

    if (anchors.length === 0 || linksMap.size === before) break; // reached end of results
    await new Promise((r) => setTimeout(r, delayMs));
  }

  // Google's title text for a LinkedIn job listing depends on the
  // result's locale:
  //   English: "<Company> hiring <Role> in <Location> | LinkedIn"
  //         or "<Role> at <Company> | LinkedIn"
  //   French:  "<Role> chez <Company>" (optionally " — <Location>") " | LinkedIn"
  //         or "<Company> recrute [pour un poste de|pour des postes de|un|une] <Role> | LinkedIn"
  // Less often: "<Role> - <Company> | LinkedIn". Parse all of these;
  // anything else leaves Company blank rather than guessing.
  const parseTitle = (title) => {
    let m = title.match(/^(.*?)\s+hiring\s+(.*?)\s+in\s+(.*?)\s*\|\s*LinkedIn\s*$/i);
    if (m) return { company: m[1].trim(), role: m[2].trim(), location: m[3].trim() };

    m = title.match(/^(.*?)\schez\s(.*?)\s*\|\s*LinkedIn\s*$/i);
    if (m) {
      const role = m[1].trim();
      const parts = m[2].split(/\s[—–]\s/);
      const company = parts[0].trim();
      const location = parts.length > 1 ? parts.slice(1).join(' — ').trim() : '';
      return { company, role, location };
    }

    m = title.match(/^(.*?)\srecrute\s(?:pour\s(?:un poste|des postes)\sde\s|une?\s)?(.*?)\s*\|\s*LinkedIn\s*$/i);
    if (m) {
      const company = m[1].trim();
      const parts = m[2].split(/\s[—–]\s/);
      const role = parts[0].trim();
      const location = parts.length > 1 ? parts.slice(1).join(' — ').trim() : '';
      return { company, role, location };
    }

    m = title.match(/^(.*?)\sat\s(.*?)\s*\|\s*LinkedIn\s*$/i);
    if (m) return { company: m[2].trim(), role: m[1].trim(), location: '' };

    m = title.match(/^(.*?)\s-\s(.*?)\s*\|\s*LinkedIn\s*$/i);
    if (m) return { company: m[2].trim(), role: m[1].trim(), location: '' };

    return { company: '', role: title, location: '' };
  };

  const esc = (s) => '"' + String(s).replace(/"/g, '""') + '"';
  const rows = [['Title', 'Company', 'Role', 'Location', 'URL'].map(esc).join(',')];
  linksMap.forEach((title, link) => {
    const { company, role, location } = parseTitle(title);
    rows.push([title, company, role, location, link].map(esc).join(','));
  });
  const csv = rows.join('\r\n');

  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
  const blobUrl = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = blobUrl;
  a.download = 'linkedin-jobs-' + Date.now() + '.csv';
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  setTimeout(() => URL.revokeObjectURL(blobUrl), 5000);

  alert('Done: ' + linksMap.size + ' unique job listings exported to CSV.');
})();

/* ---------------------------------------------------------------------
   READY-TO-PASTE BOOKMARKLET
   Copy the single line below (starting with "javascript:") in full,
   and paste it into the URL field when creating a new bookmark.
   See README.md for step-by-step instructions.
--------------------------------------------------------------------- */

javascript:(async function(){const m=new Map();const u=new URL(location.href);const maxPages=30,delayMs=1200;for(let p=0;p<maxPages;p++){u.searchParams.set('start',p*10);const r=await fetch(u.toString(),{credentials:'include'});const h=await r.text();const d=new DOMParser().parseFromString(h,'text/html');const as=[...d.querySelectorAll('a[href*="linkedin.com/jobs/view"]')];const b=m.size;as.forEach(a=>{let pu;try{pu=new URL(a.href)}catch(e){return}if(/\.translate\.goog$/i.test(pu.hostname)){const inner=pu.searchParams.get('u');if(!inner)return;try{pu=new URL(inner)}catch(e){return}}if(!pu.hostname.endsWith('linkedin.com')||!pu.pathname.startsWith('/jobs/view'))return;const c=pu.origin+pu.pathname;if(!m.has(c))m.set(c,(a.textContent||'').trim().replace(/\s+/g,' '))});if(as.length===0||m.size===b)break;await new Promise(res=>setTimeout(res,delayMs))}const pt=t=>{let mm=t.match(/^(.*?)\s+hiring\s+(.*?)\s+in\s+(.*?)\s*\|\s*LinkedIn\s*$/i);if(mm)return{c:mm[1].trim(),r:mm[2].trim(),l:mm[3].trim()};mm=t.match(/^(.*?)\schez\s(.*?)\s*\|\s*LinkedIn\s*$/i);if(mm){const role=mm[1].trim();const parts=mm[2].split(/\s[—–]\s/);const company=parts[0].trim();const location=parts.length>1?parts.slice(1).join(' — ').trim():'';return{c:company,r:role,l:location}}mm=t.match(/^(.*?)\srecrute\s(?:pour\s(?:un poste|des postes)\sde\s|une?\s)?(.*?)\s*\|\s*LinkedIn\s*$/i);if(mm){const company=mm[1].trim();const parts=mm[2].split(/\s[—–]\s/);const role=parts[0].trim();const location=parts.length>1?parts.slice(1).join(' — ').trim():'';return{c:company,r:role,l:location}}mm=t.match(/^(.*?)\sat\s(.*?)\s*\|\s*LinkedIn\s*$/i);if(mm)return{c:mm[2].trim(),r:mm[1].trim(),l:''};mm=t.match(/^(.*?)\s-\s(.*?)\s*\|\s*LinkedIn\s*$/i);if(mm)return{c:mm[2].trim(),r:mm[1].trim(),l:''};return{c:'',r:t,l:''}};const esc=s=>'"'+String(s).replace(/"/g,'""')+'"';const rows=[['Title','Company','Role','Location','URL'].map(esc).join(',')];m.forEach((t,l)=>{const{c,r,l:loc}=pt(t);rows.push([t,c,r,loc,l].map(esc).join(','))});const csv=rows.join('\r\n');const blob=new Blob([csv],{type:'text/csv;charset=utf-8;'});const bu=URL.createObjectURL(blob);const a=document.createElement('a');a.href=bu;a.download='linkedin-jobs-'+Date.now()+'.csv';document.body.appendChild(a);a.click();document.body.removeChild(a);setTimeout(()=>URL.revokeObjectURL(bu),5000);alert('Done: '+m.size+' unique job listings exported to CSV.')})();
