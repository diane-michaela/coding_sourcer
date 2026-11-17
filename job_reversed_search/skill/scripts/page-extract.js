/*
 * page-extract.js -- visible, page-by-page collection in a connected browser.
 *
 * Same extraction logic as bookmarklet.js (helpers copied unchanged), but it
 * reads ONLY the Google results page currently open in the tab, so the user
 * watches each page load. Rows accumulate in sessionStorage (key jrs-rows),
 * which survives navigation between Google pages in the same tab.
 *
 * Usage (Claude, via the browser javascript tool):
 *   1. Navigate the tab to the Google URL (start=0). Run this file's code.
 *      On start=0 it resets the stored rows.
 *   2. Navigate to &start=10, 20, ... and run it again on each page.
 *   3. Stop when it returns captcha:true, omitted:true, or newRows:0.
 *   4. Run the FINALIZE snippet at the bottom to build the CSV
 *      (same columns as the bookmarklet), then read it back.
 */
(() => {
const FR_PLACES = ['paris', 'île-de-france', 'ile-de-france', 'lyon', 'marseille', 'toulouse', 'bordeaux', 'nantes', 'lille', 'rennes', 'montpellier', 'strasbourg', 'nice', 'grenoble', 'sophia antipolis', 'villeurbanne', 'boulogne-billancourt', 'levallois-perret', 'puteaux', 'courbevoie', 'nanterre', 'montrouge', 'issy-les-moulineaux', 'la défense', 'clermont-ferrand', 'rouen', 'tours', 'le mans', 'angers', 'dijon', 'brest', 'aix-en-provence', 'metz', 'nancy', 'orléans', 'reims', 'caen', 'cesson-sévigné', 'malakoff', 'france'];
const OUT_PLACES = ['united states', 'usa', 'canada', 'toronto', 'montréal', 'montreal', 'new york', 'san francisco', 'seattle', 'austin', 'boston', 'chicago', 'salt lake city', 'mountain view', 'london', 'united kingdom', 'berlin', 'munich', 'münchen', 'germany', 'deutschland', 'amsterdam', 'netherlands', 'madrid', 'barcelona', 'tarragona', 'spain', 'lisbon', 'portugal', 'warsaw', 'poland', 'budapest', 'bangalore', 'bengaluru', 'india', 'brazil', 'são paulo', 'mexico', 'costa rica', 'singapore', 'dubai', 'tel aviv', 'israel', 'athens', 'greece'];
const FR_HINTS = /(^|[^a-zà-ÿ])(h\/f|f\/h|h-f|f-h|cdi|cdd|télétravail|alternance|ingénieur|développeur|confirmé|expérimenté)([^a-zà-ÿ]|$)/i;
const hasPlace = (text, list) => list.find(p => new RegExp('(^|[^a-zà-ÿ])' + p.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '($|[^a-zà-ÿ])', 'i').test(text));
const QWORDS = new Set((new URL(location.href).searchParams.get('q') || '').toLowerCase().split(/[^a-zà-ÿ0-9]+/).filter(Boolean));
const isQueryBold = b => { const w = b.toLowerCase().split(/[^a-zà-ÿ0-9]+/).filter(Boolean); return w.length > 0 && w.every(x => QWORDS.has(x)); };
const isPlace = t => !!(hasPlace(t.toLowerCase(), FR_PLACES.concat(OUT_PLACES)) || /\d{5}|\d+\s*(av|rue|bd|boulevard|avenue)\b/i.test(t));
const locate = (title, snippet, src, bolds = [], url = '') => {
  let loc = '';
  const mm = title.match(/\s[—–]\s([^|—–]+?)\s*(\||$)/) || title.match(/\b(?:CDI|CDD|Stage|Freelance|Alternance)\s+à\s+([^|\-–—]+)/i) || title.match(/\shiring\s.+?\sin\s(.+?)\s*\|/i);
  if (mm) loc = mm[1].replace(/\s*-\s*Welcome to the Jungle.*$/i, '').trim();
  if (loc && src !== 'LinkedIn' && !isPlace(loc)) loc = '';
  if (!loc && src === 'WTTJ') { const w = url.match(/\/jobs\/[^/?#]*?_([a-z-]+?)(?:_[a-z0-9]{6,})?\/?$/i); if (w) loc = w[1].replace(/-/g, ' '); }
  let clean = snippet;
  bolds.filter(isQueryBold).forEach(b => { clean = clean.split(b).join(' '); });
  if (!loc) loc = (bolds.find(b => isPlace(b) && !isQueryBold(b)) || '').trim();
  const text = (title + ' ' + clean).toLowerCase();
  const fr = hasPlace(loc.toLowerCase(), FR_PLACES) || hasPlace(text, FR_PLACES);
  const out = hasPlace(loc.toLowerCase(), OUT_PLACES) || hasPlace(text, OUT_PLACES);
  if (!loc) loc = fr || out || '';
  loc = loc.replace(/^./, c => c.toUpperCase());
  const cc = (url.match(/^https?:\/\/([a-z]{2})\.linkedin\.com\//i) || [])[1];
  const pays = cc ? cc.toUpperCase() : '?';
  return { loc, pays };
};
const STACK = ['bedrock', 'agentcore', 'langchain', 'langgraph', 'llamaindex', 'crewai', 'autogen', 'dspy', 'semantic kernel', 'haystack', 'rag', 'llm', 'sagemaker', 'mlflow', 'mlops', 'llmops', 'aws', 'azure', 'gcp', 'python', 'typescript', 'node.js', 'react', 'next.js', 'terraform', 'pulumi', 'ansible', 'kubernetes', 'docker', 'postgresql', 'redis', 'kafka', 'databricks', 'figma', 'storybook', 'tailwind', 'cypress', 'jest'];
const stackOf = (title, snippet, bolds) => {
  const text = (title + ' ' + snippet + ' ' + bolds.join(' ')).toLowerCase();
  return STACK.filter(x => hasPlace(text, [x])).join(', ');
};
const blockOf = a => a.closest('div.g, div.MjjYud, div[data-hveid]') || a.parentElement || a;
const SUFFIX = /\s*(\|\s*LinkedIn.*|-\s*Welcome to the Jungle.*|-\s*Recruitee|-\s*Teamtailor|-\s*Application|\|\s*Jobs (at|bei) .*|\.\.\.)\s*$/i;
const splitParts = t => t.split(/\s+[-–—|]\s+|\s+-(?=\S)|(?<=\S)-\s+/).map(x => x.trim()).filter(Boolean);
const compact = x => (x || '').toLowerCase().replace(/[^a-z0-9]/g, '');
const stripCompany = (t, company) => {
  const c = compact(company);
  const words = t.split(/\s+/);
  let acc = '', i = 0;
  while (i < words.length) {
    const w = compact(words[i]);
    if (w && c.startsWith(acc + w)) { acc += w; i++; }
    else if (acc && /^\d+$/.test(w)) i++;
    else break;
  }
  return acc.length >= 2 && i < words.length ? words.slice(i).join(' ').replace(/^[,.\s]+/, '') : t;
};
const dropCompanyPart = (parts, company) => {
  const c = compact(company);
  const kept = c.length >= 3 ? parts.filter(x => { const k = compact(x); return !(k.includes(c) || c.startsWith(k)); }) : parts;
  return kept.length ? kept : parts;
};
const roleOf = (title, company, src) => {
  const t = title.replace(SUFFIX, '').trim();
  if (src === 'Greenhouse') { const g = t.match(/^Job Application for\s+(.+?)\s+at\s+.+$/i); if (g) return g[1].trim(); }
  if (src === 'Ashby' && t.includes('@')) return t.split(/\s+@\s*/)[0].trim();
  if (src === 'Personio') return t.split('|')[0].trim();
  const parts = dropCompanyPart(splitParts(t), company);
  if (parts.length === 1) return stripCompany(parts[0], company).replace(/^[-–—|,.\s]+/, '').trim();
  return (src === 'Lever' ? parts.join(' - ') : parts[0]).trim();
};
const s2w=s=>s.split('-').filter(Boolean).map(w=>w.charAt(0).toUpperCase()+w.slice(1)).join(' ');const pus=p=>{let slug=p.replace(/^\/jobs\/view\//,'').replace(/\/$/,'');try{slug=decodeURIComponent(slug)}catch(e){}const mm=slug.match(/^(.+)-at-(.+)-\d+$/i);if(!mm)return null;return{role:s2w(mm[1]),company:s2w(mm[2])}};const pt=t=>{let mm=t.match(/^(.*?)\s+hiring\s+(.*?)\s+in\s+(.*?)\s*\|\s*LinkedIn\s*$/i);if(mm)return{c:mm[1].trim(),r:mm[2].trim()};mm=t.match(/^(.*?)\schez\s(.*?)\s*\|\s*LinkedIn\s*$/i);if(mm)return{c:mm[2].split(/\s[—–]\s/)[0].trim(),r:mm[1].trim()};mm=t.match(/^(.*?)\srecrute\s(?:pour\s(?:un poste|des postes)\sde\s|une?\s)?(.*?)\s*\|\s*LinkedIn\s*$/i);if(mm)return{c:mm[1].trim(),r:mm[2].split(/\s[—–]\s/)[0].trim()};mm=t.match(/^(.*?)\sat\s(.*?)\s*\|\s*LinkedIn\s*$/i);if(mm)return{c:mm[2].trim(),r:mm[1].trim()};mm=t.match(/^(.*?)\s-\s(.*?)\s*\|\s*LinkedIn\s*$/i);if(mm)return{c:mm[2].trim(),r:mm[1].trim()};return{c:'',r:t}};
const KEY='jrs-rows';
const start=+(new URL(location.href).searchParams.get('start')||0);
if(start===0) sessionStorage.removeItem(KEY);
const store=JSON.parse(sessionStorage.getItem(KEY)||'[]');
const seen=new Set(store.map(r=>r[6]));
const body=document.body.innerText;
const captcha=/unusual traffic|recaptcha/i.test(body)||!!document.querySelector('#captcha-form');
const omitted=/omis certains r|omitted some entries|omitido algunas/i.test(body);
let added=0;
document.querySelectorAll('a[href*="linkedin.com/jobs/view"]').forEach(a=>{
  let pu;try{pu=new URL(a.href)}catch(e){return}
  if(/\.translate\.goog$/i.test(pu.hostname)){const inner=pu.searchParams.get('u');if(!inner)return;try{pu=new URL(inner)}catch(e){return}}
  if(!pu.hostname.endsWith('linkedin.com')||!pu.pathname.startsWith('/jobs/view'))return;
  const url=pu.origin+pu.pathname; if(seen.has(url))return; seen.add(url);
  const title=((a.querySelector('h3')||a).textContent||'').trim().replace(/\s+/g,' ');
  const snippet=(blockOf(a).textContent||'').replace(/\s+/g,' ');
  const bolds=[...blockOf(a).querySelectorAll('em, b')].map(e=>e.textContent||'');
  const si=pus(pu.pathname); const ft=pt(title);
  const g=locate(title,snippet,'LinkedIn',bolds,url);
  store.push([title,(si&&si.company)||ft.c,(si&&si.role)||ft.r,g.loc,g.pays,stackOf(title,snippet,bolds),url]);
  added++;
});
sessionStorage.setItem(KEY,JSON.stringify(store));
return JSON.stringify({page:start/10+1,newRows:added,total:store.length,captcha,omitted});
})()

/* ---------------------------------------------------------------------
   FINALIZE (run once, on the last page): builds the bookmarklet CSV in
   window.jrsCsv and returns its length. Read it back in slices of about
   950 characters (window.jrsCsv.slice(0,950), ...), batched.

(() => {const esc=s=>'"'+String(s).replace(/"/g,'""')+'"';const ts=Date.now();const rows=JSON.parse(sessionStorage.getItem('jrs-rows')||'[]');const out=[['ID','Title','Company','Role','Location','Country code','Stack','Source','URL'].map(esc).join(',')];rows.forEach((r,i)=>out.push([ts+'-'+String(i+1).padStart(3,'0'),r[0],r[1],r[2],r[3],r[4],r[5],'LinkedIn',r[6]].map(esc).join(',')));window.jrsCsv=out.join('\r\n');return window.jrsCsv.length;})()
   --------------------------------------------------------------------- */
