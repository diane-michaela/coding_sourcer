/*
 * fast-extract.js -- DEFAULT collection in a connected browser.
 *
 * The bookmarklet's code, unchanged, except: it does not download a file,
 * it stores the CSV in window.jrsCsv, and it reports how it stopped.
 * Run it once, with the browser javascript tool, on the FIRST Google
 * results page. It fetches the next pages in the background (1.2 s apart),
 * so the tab stays on page 1. It returns {rows, pages, stop}, where stop is
 * "omitted" (search complete), "no new job", or "captcha" (hand over to the
 * user, then run it again).
 * It also saves the CSV in sessionStorage under jrs-<country code> (jrs-uk,
 * jrs-pl, ...), read from the site: term of the query, so a multi-country
 * run survives navigation and a CAPTCHA pause. The Country code column holds that
 * country code in capitals (UK, PL, FR...), or ? when the job URL has none.
 * Then bring the rows into Claude with readback.js.
 */
var jrsPg=0,jrsStop='';await (async function(){
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
const m=new Map();const u=new URL(location.href);const maxPages=30,delayMs=1200;const s2w=s=>s.split('-').filter(Boolean).map(w=>w.charAt(0).toUpperCase()+w.slice(1)).join(' ');const pus=p=>{let slug=p.replace(/^\/jobs\/view\//,'').replace(/\/$/,'');try{slug=decodeURIComponent(slug)}catch(e){}const mm=slug.match(/^(.+)-at-(.+)-\d+$/i);if(!mm)return null;return{role:s2w(mm[1]),company:s2w(mm[2])}};for(let p=0;p<maxPages;p++){u.searchParams.set('start',p*10);const r=await fetch(u.toString(),{credentials:'include'});const h=await r.text();jrsPg=p+1;if(/unusual traffic|recaptcha|captcha-form/i.test(h)){jrsStop='captcha';break;}if(/omis certains r|omitted some entries|omitido algunas/i.test(h)){jrsStop='omitted';}const d=new DOMParser().parseFromString(h,'text/html');const as=[...d.querySelectorAll('a[href*="linkedin.com/jobs/view"]')];const b=m.size;as.forEach(a=>{let pu;try{pu=new URL(a.href)}catch(e){return}if(/\.translate\.goog$/i.test(pu.hostname)){const inner=pu.searchParams.get('u');if(!inner)return;try{pu=new URL(inner)}catch(e){return}}if(!pu.hostname.endsWith('linkedin.com')||!pu.pathname.startsWith('/jobs/view'))return;const c=pu.origin+pu.pathname;if(!m.has(c)){const si=pus(pu.pathname);m.set(c,{title:((a.querySelector('h3')||a).textContent||'').trim().replace(/\s+/g,' '),snippet:(blockOf(a).textContent||'').replace(/\s+/g,' '),bolds:[...blockOf(a).querySelectorAll('em, b')].map(e=>e.textContent||''),sr:si?si.role:'',sc:si?si.company:''})}});if(jrsStop==='omitted'){break;}if(as.length===0||m.size===b){jrsStop=jrsStop||'no new job';break;}await new Promise(res=>setTimeout(res,delayMs))}const pt=t=>{let mm=t.match(/^(.*?)\s+hiring\s+(.*?)\s+in\s+(.*?)\s*\|\s*LinkedIn\s*$/i);if(mm)return{c:mm[1].trim(),r:mm[2].trim()};mm=t.match(/^(.*?)\schez\s(.*?)\s*\|\s*LinkedIn\s*$/i);if(mm)return{c:mm[2].split(/\s[—–]\s/)[0].trim(),r:mm[1].trim()};mm=t.match(/^(.*?)\srecrute\s(?:pour\s(?:un poste|des postes)\sde\s|une?\s)?(.*?)\s*\|\s*LinkedIn\s*$/i);if(mm)return{c:mm[1].trim(),r:mm[2].split(/\s[—–]\s/)[0].trim()};mm=t.match(/^(.*?)\sat\s(.*?)\s*\|\s*LinkedIn\s*$/i);if(mm)return{c:mm[2].trim(),r:mm[1].trim()};mm=t.match(/^(.*?)\s-\s(.*?)\s*\|\s*LinkedIn\s*$/i);if(mm)return{c:mm[2].trim(),r:mm[1].trim()};return{c:'',r:t}};const esc=s=>'"'+String(s).replace(/"/g,'""')+'"';const ts=Date.now();let n=0;const rows=[['ID','Title','Company','Role','Location','Country code','Stack','Source','URL'].map(esc).join(',')];m.forEach((info,l)=>{const ft=pt(info.title);const c=info.sc||ft.c;const r=info.sr||ft.r;const g=locate(info.title,info.snippet,'LinkedIn',info.bolds,l);rows.push([ts+'-'+String(++n).padStart(3,'0'),info.title,c,r,g.loc,g.pays,stackOf(info.title,info.snippet,info.bolds),'LinkedIn',l].map(esc).join(','))});const csv=rows.join('\r\n');window.jrsCsv=csv;const qcc=((u.searchParams.get('q')||'').match(/site:([a-z]{2}|www)\.linkedin\.com/i)||[])[1]||'xx';try{sessionStorage.setItem('jrs-'+qcc.toLowerCase(),csv)}catch(e){}window.jrsInfo={country:qcc.toUpperCase(),rows:m.size,pages:jrsPg,stop:jrsStop,savedAs:'jrs-'+qcc.toLowerCase()};})();
JSON.stringify(window.jrsInfo)
