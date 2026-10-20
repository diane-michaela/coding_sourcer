/*
 * Google Jobs Any-Site Extractor -- bookmarklet
 * ---------------------------------------------
 * Same idea as bookmarklet.js, but keeps every organic result, not just
 * LinkedIn: Welcome to the Jungle and ATS job pages (Lever, Greenhouse,
 * Ashby, Teamtailor, Workable, Recruitee, Personio, SmartRecruiters...).
 * Company comes from the ATS URL. CSV: Title, Company, Role, Location,
 * Pays, Stack, Source, URL.
 *
 * See README.md in this folder for installation instructions
 * (macOS and Windows) and usage steps.
 */

(async function(){
const maxPages = 30, delayMs = 1200;
const u = new URL(location.href);
const m = new Map();
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
  let pays = '?';
  if (hasPlace(loc.toLowerCase(), FR_PLACES) || /\b\d{5}\b/.test(loc)) pays = 'France';
  else if (hasPlace(loc.toLowerCase(), OUT_PLACES)) pays = 'Hors France';
  else if (fr && !out) pays = 'France';
  else if (out && !fr) pays = 'Hors France';
  else if (!fr && !out && (src === 'LinkedIn' || FR_HINTS.test(title))) pays = 'France';
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
const s2w = s => { try { s = decodeURIComponent(s || ''); } catch (e) {} return String(s || '').replace(/(?<=[a-z])\d+$/i, '').split(/[-_\s]/).filter(Boolean).map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' '); };
const ats = url => {
  const h = url.hostname.replace(/^www\./, '');
  const p = url.pathname.split('/').filter(Boolean);
  if (h === 'jobs.lever.co') return { src: 'Lever', company: p[0] };
  if (/greenhouse\.io$/.test(h)) return { src: 'Greenhouse', company: p[0] === 'embed' ? url.searchParams.get('for') : p[0] };
  if (h === 'jobs.ashbyhq.com') return { src: 'Ashby', company: p[0] };
  if (h.endsWith('.teamtailor.com')) return { src: 'Teamtailor', company: h.split('.')[0] };
  if (h === 'apply.workable.com') return { src: 'Workable', company: p[0] === 'j' ? '' : p[0] };
  if (h.endsWith('.recruitee.com')) return { src: 'Recruitee', company: h.split('.')[0] };
  const sub = [['jobs.personio.de', 'Personio'], ['jobs.personio.com', 'Personio'], ['digitalrecruiters.com', 'DigitalRecruiters'], ['breezy.hr', 'Breezy'], ['bamboohr.com', 'BambooHR'], ['myworkdayjobs.com', 'Workday'], ['werecruit.io', 'Werecruit'], ['flatchr.io', 'Flatchr']].find(([d]) => h.endsWith('.' + d));
  if (sub) return { src: sub[1], company: h.split('.')[0] };
  const named = { 'jobs.smartrecruiters.com': 'SmartRecruiters', 'join.com': 'JOIN' };
  if (named[h] || h.endsWith('taleez.com') || h.endsWith('jobaffinity.fr')) return { src: named[h] || (h.endsWith('taleez.com') ? 'Taleez' : 'Jobaffinity'), company: p[0] === 'companies' ? p[1] : p[0] };
  if (h.endsWith('welcometothejungle.com')) { const i = p.indexOf('companies'); return { src: 'WTTJ', company: i >= 0 ? p[i + 1] : '' }; }
  if (h.endsWith('linkedin.com') && url.pathname.startsWith('/jobs/view')) {
    let slug = p[2] || ''; try { slug = decodeURIComponent(slug); } catch (e) {}
    const mm = slug.match(/^(.+)-at-(.+)-\d+$/i);
    return { src: 'LinkedIn', company: mm ? mm[2] : '' };
  }
  return { src: 'Other', company: h };
};
for (let pg = 0; pg < maxPages; pg++) {
  u.searchParams.set('start', pg * 10);
  const r = await fetch(u.toString(), { credentials: 'include' });
  const d = new DOMParser().parseFromString(await r.text(), 'text/html');
  const before = m.size;
  let found = 0;
  d.querySelectorAll('a h3').forEach(h3 => {
    const a = h3.closest('a');
    if (!a) return;
    let href = a.getAttribute('href') || '';
    if (href.startsWith('/url?')) href = new URLSearchParams(href.slice(5)).get('q') || '';
    let pu;
    try { pu = new URL(href, location.origin); } catch (e) { return; }
    if (/\.translate\.goog$/i.test(pu.hostname)) {
      const inner = pu.searchParams.get('u');
      if (!inner) return;
      try { pu = new URL(inner); } catch (e) { return; }
    }
    if (/(^|\.)google\./.test(pu.hostname)) return;
    found++;
    const key = pu.origin + pu.pathname.replace(/\/l\/[a-z]{2}(?=\/o\/)/, '').replace(/\/(c\/new|apply)\/?$/, '');
    if (!m.has(key)) {
      const info = ats(pu);
      const block = blockOf(a);
      const title = (h3.textContent || '').trim().replace(/\s+/g, ' ');
      const snippet = (block.textContent || '').replace(h3.textContent || '', ' ').replace(/\s+/g, ' ');
      const bolds = [...block.querySelectorAll('em, b')].map(e => e.textContent || '');
      m.set(key, { title, snippet, bolds, src: info.src, company: s2w(info.company) });
    }
  });
  if (found === 0 || m.size === before) break;
  await new Promise(res => setTimeout(res, delayMs));
}
const esc = s => '"' + String(s).replace(/"/g, '""') + '"';
const rows = [['Title', 'Company', 'Role', 'Location', 'Pays', 'Stack', 'Source', 'URL'].map(esc).join(',')];
m.forEach((info, link) => { const g = locate(info.title, info.snippet, info.src, info.bolds, link); rows.push([info.title, info.company, roleOf(info.title, info.company, info.src), g.loc, g.pays, stackOf(info.title, info.snippet, info.bolds), info.src, link].map(esc).join(',')); });
const blob = new Blob([rows.join('\r\n')], { type: 'text/csv;charset=utf-8;' });
const bu = URL.createObjectURL(blob);
const dl = document.createElement('a');
dl.href = bu;
dl.download = 'google-jobs-' + Date.now() + '.csv';
document.body.appendChild(dl);
dl.click();
document.body.removeChild(dl);
setTimeout(() => URL.revokeObjectURL(bu), 5000);
alert('Done: ' + m.size + ' unique results exported to CSV.');
})();

/* ---------------------------------------------------------------------
   READY-TO-PASTE BOOKMARKLET
   Copy the single line below (starting with "javascript:") in full,
   and paste it into the URL field when creating a new bookmark.
   It is URL-encoded, which is normal: the browser decodes it on click.
   See README.md for step-by-step instructions.
--------------------------------------------------------------------- */

javascript:%28async%20function%28%29%7B%20const%20maxPages%20%3D%2030%2C%20delayMs%20%3D%201200%3B%20const%20u%20%3D%20new%20URL%28location.href%29%3B%20const%20m%20%3D%20new%20Map%28%29%3B%20const%20FR_PLACES%20%3D%20%5B%27paris%27%2C%20%27%C3%AEle-de-france%27%2C%20%27ile-de-france%27%2C%20%27lyon%27%2C%20%27marseille%27%2C%20%27toulouse%27%2C%20%27bordeaux%27%2C%20%27nantes%27%2C%20%27lille%27%2C%20%27rennes%27%2C%20%27montpellier%27%2C%20%27strasbourg%27%2C%20%27nice%27%2C%20%27grenoble%27%2C%20%27sophia%20antipolis%27%2C%20%27villeurbanne%27%2C%20%27boulogne-billancourt%27%2C%20%27levallois-perret%27%2C%20%27puteaux%27%2C%20%27courbevoie%27%2C%20%27nanterre%27%2C%20%27montrouge%27%2C%20%27issy-les-moulineaux%27%2C%20%27la%20d%C3%A9fense%27%2C%20%27clermont-ferrand%27%2C%20%27rouen%27%2C%20%27tours%27%2C%20%27le%20mans%27%2C%20%27angers%27%2C%20%27dijon%27%2C%20%27brest%27%2C%20%27aix-en-provence%27%2C%20%27metz%27%2C%20%27nancy%27%2C%20%27orl%C3%A9ans%27%2C%20%27reims%27%2C%20%27caen%27%2C%20%27cesson-s%C3%A9vign%C3%A9%27%2C%20%27malakoff%27%2C%20%27france%27%5D%3B%20const%20OUT_PLACES%20%3D%20%5B%27united%20states%27%2C%20%27usa%27%2C%20%27canada%27%2C%20%27toronto%27%2C%20%27montr%C3%A9al%27%2C%20%27montreal%27%2C%20%27new%20york%27%2C%20%27san%20francisco%27%2C%20%27seattle%27%2C%20%27austin%27%2C%20%27boston%27%2C%20%27chicago%27%2C%20%27salt%20lake%20city%27%2C%20%27mountain%20view%27%2C%20%27london%27%2C%20%27united%20kingdom%27%2C%20%27berlin%27%2C%20%27munich%27%2C%20%27m%C3%BCnchen%27%2C%20%27germany%27%2C%20%27deutschland%27%2C%20%27amsterdam%27%2C%20%27netherlands%27%2C%20%27madrid%27%2C%20%27barcelona%27%2C%20%27tarragona%27%2C%20%27spain%27%2C%20%27lisbon%27%2C%20%27portugal%27%2C%20%27warsaw%27%2C%20%27poland%27%2C%20%27budapest%27%2C%20%27bangalore%27%2C%20%27bengaluru%27%2C%20%27india%27%2C%20%27brazil%27%2C%20%27s%C3%A3o%20paulo%27%2C%20%27mexico%27%2C%20%27costa%20rica%27%2C%20%27singapore%27%2C%20%27dubai%27%2C%20%27tel%20aviv%27%2C%20%27israel%27%2C%20%27athens%27%2C%20%27greece%27%5D%3B%20const%20FR_HINTS%20%3D%20%2F%28%5E%7C%5B%5Ea-z%C3%A0-%C3%BF%5D%29%28h%5C%2Ff%7Cf%5C%2Fh%7Ch-f%7Cf-h%7Ccdi%7Ccdd%7Ct%C3%A9l%C3%A9travail%7Calternance%7Cing%C3%A9nieur%7Cd%C3%A9veloppeur%7Cconfirm%C3%A9%7Cexp%C3%A9riment%C3%A9%29%28%5B%5Ea-z%C3%A0-%C3%BF%5D%7C%24%29%2Fi%3B%20const%20hasPlace%20%3D%20%28text%2C%20list%29%20%3D%3E%20list.find%28p%20%3D%3E%20new%20RegExp%28%27%28%5E%7C%5B%5Ea-z%C3%A0-%C3%BF%5D%29%27%20%2B%20p.replace%28%2F%5B.%2A%2B%3F%5E%24%7B%7D%28%29%7C%5B%5C%5D%5C%5C%5D%2Fg%2C%20%27%5C%5C%24%26%27%29%20%2B%20%27%28%24%7C%5B%5Ea-z%C3%A0-%C3%BF%5D%29%27%2C%20%27i%27%29.test%28text%29%29%3B%20const%20QWORDS%20%3D%20new%20Set%28%28new%20URL%28location.href%29.searchParams.get%28%27q%27%29%20%7C%7C%20%27%27%29.toLowerCase%28%29.split%28%2F%5B%5Ea-z%C3%A0-%C3%BF0-9%5D%2B%2F%29.filter%28Boolean%29%29%3B%20const%20isQueryBold%20%3D%20b%20%3D%3E%20%7B%20const%20w%20%3D%20b.toLowerCase%28%29.split%28%2F%5B%5Ea-z%C3%A0-%C3%BF0-9%5D%2B%2F%29.filter%28Boolean%29%3B%20return%20w.length%20%3E%200%20%26%26%20w.every%28x%20%3D%3E%20QWORDS.has%28x%29%29%3B%20%7D%3B%20const%20isPlace%20%3D%20t%20%3D%3E%20%21%21%28hasPlace%28t.toLowerCase%28%29%2C%20FR_PLACES.concat%28OUT_PLACES%29%29%20%7C%7C%20%2F%5Cd%7B5%7D%7C%5Cd%2B%5Cs%2A%28av%7Crue%7Cbd%7Cboulevard%7Cavenue%29%5Cb%2Fi.test%28t%29%29%3B%20const%20locate%20%3D%20%28title%2C%20snippet%2C%20src%2C%20bolds%20%3D%20%5B%5D%2C%20url%20%3D%20%27%27%29%20%3D%3E%20%7B%20let%20loc%20%3D%20%27%27%3B%20const%20mm%20%3D%20title.match%28%2F%5Cs%5B%E2%80%94%E2%80%93%5D%5Cs%28%5B%5E%7C%E2%80%94%E2%80%93%5D%2B%3F%29%5Cs%2A%28%5C%7C%7C%24%29%2F%29%20%7C%7C%20title.match%28%2F%5Cb%28%3F%3ACDI%7CCDD%7CStage%7CFreelance%7CAlternance%29%5Cs%2B%C3%A0%5Cs%2B%28%5B%5E%7C%5C-%E2%80%93%E2%80%94%5D%2B%29%2Fi%29%20%7C%7C%20title.match%28%2F%5Cshiring%5Cs.%2B%3F%5Csin%5Cs%28.%2B%3F%29%5Cs%2A%5C%7C%2Fi%29%3B%20if%20%28mm%29%20loc%20%3D%20mm%5B1%5D.replace%28%2F%5Cs%2A-%5Cs%2AWelcome%20to%20the%20Jungle.%2A%24%2Fi%2C%20%27%27%29.trim%28%29%3B%20if%20%28loc%20%26%26%20src%20%21%3D%3D%20%27LinkedIn%27%20%26%26%20%21isPlace%28loc%29%29%20loc%20%3D%20%27%27%3B%20if%20%28%21loc%20%26%26%20src%20%3D%3D%3D%20%27WTTJ%27%29%20%7B%20const%20w%20%3D%20url.match%28%2F%5C%2Fjobs%5C%2F%5B%5E%2F%3F%23%5D%2A%3F_%28%5Ba-z-%5D%2B%3F%29%28%3F%3A_%5Ba-z0-9%5D%7B6%2C%7D%29%3F%5C%2F%3F%24%2Fi%29%3B%20if%20%28w%29%20loc%20%3D%20w%5B1%5D.replace%28%2F-%2Fg%2C%20%27%20%27%29%3B%20%7D%20let%20clean%20%3D%20snippet%3B%20bolds.filter%28isQueryBold%29.forEach%28b%20%3D%3E%20%7B%20clean%20%3D%20clean.split%28b%29.join%28%27%20%27%29%3B%20%7D%29%3B%20if%20%28%21loc%29%20loc%20%3D%20%28bolds.find%28b%20%3D%3E%20isPlace%28b%29%20%26%26%20%21isQueryBold%28b%29%29%20%7C%7C%20%27%27%29.trim%28%29%3B%20const%20text%20%3D%20%28title%20%2B%20%27%20%27%20%2B%20clean%29.toLowerCase%28%29%3B%20const%20fr%20%3D%20hasPlace%28loc.toLowerCase%28%29%2C%20FR_PLACES%29%20%7C%7C%20hasPlace%28text%2C%20FR_PLACES%29%3B%20const%20out%20%3D%20hasPlace%28loc.toLowerCase%28%29%2C%20OUT_PLACES%29%20%7C%7C%20hasPlace%28text%2C%20OUT_PLACES%29%3B%20if%20%28%21loc%29%20loc%20%3D%20fr%20%7C%7C%20out%20%7C%7C%20%27%27%3B%20loc%20%3D%20loc.replace%28%2F%5E.%2F%2C%20c%20%3D%3E%20c.toUpperCase%28%29%29%3B%20let%20pays%20%3D%20%27%3F%27%3B%20if%20%28hasPlace%28loc.toLowerCase%28%29%2C%20FR_PLACES%29%20%7C%7C%20%2F%5Cb%5Cd%7B5%7D%5Cb%2F.test%28loc%29%29%20pays%20%3D%20%27France%27%3B%20else%20if%20%28hasPlace%28loc.toLowerCase%28%29%2C%20OUT_PLACES%29%29%20pays%20%3D%20%27Hors%20France%27%3B%20else%20if%20%28fr%20%26%26%20%21out%29%20pays%20%3D%20%27France%27%3B%20else%20if%20%28out%20%26%26%20%21fr%29%20pays%20%3D%20%27Hors%20France%27%3B%20else%20if%20%28%21fr%20%26%26%20%21out%20%26%26%20%28src%20%3D%3D%3D%20%27LinkedIn%27%20%7C%7C%20FR_HINTS.test%28title%29%29%29%20pays%20%3D%20%27France%27%3B%20return%20%7B%20loc%2C%20pays%20%7D%3B%20%7D%3B%20const%20STACK%20%3D%20%5B%27bedrock%27%2C%20%27agentcore%27%2C%20%27langchain%27%2C%20%27langgraph%27%2C%20%27llamaindex%27%2C%20%27crewai%27%2C%20%27autogen%27%2C%20%27dspy%27%2C%20%27semantic%20kernel%27%2C%20%27haystack%27%2C%20%27rag%27%2C%20%27llm%27%2C%20%27sagemaker%27%2C%20%27mlflow%27%2C%20%27mlops%27%2C%20%27llmops%27%2C%20%27aws%27%2C%20%27azure%27%2C%20%27gcp%27%2C%20%27python%27%2C%20%27typescript%27%2C%20%27node.js%27%2C%20%27react%27%2C%20%27next.js%27%2C%20%27terraform%27%2C%20%27pulumi%27%2C%20%27ansible%27%2C%20%27kubernetes%27%2C%20%27docker%27%2C%20%27postgresql%27%2C%20%27redis%27%2C%20%27kafka%27%2C%20%27databricks%27%2C%20%27figma%27%2C%20%27storybook%27%2C%20%27tailwind%27%2C%20%27cypress%27%2C%20%27jest%27%5D%3B%20const%20stackOf%20%3D%20%28title%2C%20snippet%2C%20bolds%29%20%3D%3E%20%7B%20const%20text%20%3D%20%28title%20%2B%20%27%20%27%20%2B%20snippet%20%2B%20%27%20%27%20%2B%20bolds.join%28%27%20%27%29%29.toLowerCase%28%29%3B%20return%20STACK.filter%28x%20%3D%3E%20hasPlace%28text%2C%20%5Bx%5D%29%29.join%28%27%2C%20%27%29%3B%20%7D%3B%20const%20blockOf%20%3D%20a%20%3D%3E%20a.closest%28%27div.g%2C%20div.MjjYud%2C%20div%5Bdata-hveid%5D%27%29%20%7C%7C%20a.parentElement%20%7C%7C%20a%3B%20const%20SUFFIX%20%3D%20%2F%5Cs%2A%28%5C%7C%5Cs%2ALinkedIn.%2A%7C-%5Cs%2AWelcome%20to%20the%20Jungle.%2A%7C-%5Cs%2ARecruitee%7C-%5Cs%2ATeamtailor%7C-%5Cs%2AApplication%7C%5C%7C%5Cs%2AJobs%20%28at%7Cbei%29%20.%2A%7C%5C.%5C.%5C.%29%5Cs%2A%24%2Fi%3B%20const%20splitParts%20%3D%20t%20%3D%3E%20t.split%28%2F%5Cs%2B%5B-%E2%80%93%E2%80%94%7C%5D%5Cs%2B%7C%5Cs%2B-%28%3F%3D%5CS%29%7C%28%3F%3C%3D%5CS%29-%5Cs%2B%2F%29.map%28x%20%3D%3E%20x.trim%28%29%29.filter%28Boolean%29%3B%20const%20compact%20%3D%20x%20%3D%3E%20%28x%20%7C%7C%20%27%27%29.toLowerCase%28%29.replace%28%2F%5B%5Ea-z0-9%5D%2Fg%2C%20%27%27%29%3B%20const%20stripCompany%20%3D%20%28t%2C%20company%29%20%3D%3E%20%7B%20const%20c%20%3D%20compact%28company%29%3B%20const%20words%20%3D%20t.split%28%2F%5Cs%2B%2F%29%3B%20let%20acc%20%3D%20%27%27%2C%20i%20%3D%200%3B%20while%20%28i%20%3C%20words.length%29%20%7B%20const%20w%20%3D%20compact%28words%5Bi%5D%29%3B%20if%20%28w%20%26%26%20c.startsWith%28acc%20%2B%20w%29%29%20%7B%20acc%20%2B%3D%20w%3B%20i%2B%2B%3B%20%7D%20else%20if%20%28acc%20%26%26%20%2F%5E%5Cd%2B%24%2F.test%28w%29%29%20i%2B%2B%3B%20else%20break%3B%20%7D%20return%20acc.length%20%3E%3D%202%20%26%26%20i%20%3C%20words.length%20%3F%20words.slice%28i%29.join%28%27%20%27%29.replace%28%2F%5E%5B%2C.%5Cs%5D%2B%2F%2C%20%27%27%29%20%3A%20t%3B%20%7D%3B%20const%20dropCompanyPart%20%3D%20%28parts%2C%20company%29%20%3D%3E%20%7B%20const%20c%20%3D%20compact%28company%29%3B%20const%20kept%20%3D%20c.length%20%3E%3D%203%20%3F%20parts.filter%28x%20%3D%3E%20%7B%20const%20k%20%3D%20compact%28x%29%3B%20return%20%21%28k.includes%28c%29%20%7C%7C%20c.startsWith%28k%29%29%3B%20%7D%29%20%3A%20parts%3B%20return%20kept.length%20%3F%20kept%20%3A%20parts%3B%20%7D%3B%20const%20roleOf%20%3D%20%28title%2C%20company%2C%20src%29%20%3D%3E%20%7B%20const%20t%20%3D%20title.replace%28SUFFIX%2C%20%27%27%29.trim%28%29%3B%20if%20%28src%20%3D%3D%3D%20%27Greenhouse%27%29%20%7B%20const%20g%20%3D%20t.match%28%2F%5EJob%20Application%20for%5Cs%2B%28.%2B%3F%29%5Cs%2Bat%5Cs%2B.%2B%24%2Fi%29%3B%20if%20%28g%29%20return%20g%5B1%5D.trim%28%29%3B%20%7D%20if%20%28src%20%3D%3D%3D%20%27Ashby%27%20%26%26%20t.includes%28%27%40%27%29%29%20return%20t.split%28%2F%5Cs%2B%40%5Cs%2A%2F%29%5B0%5D.trim%28%29%3B%20if%20%28src%20%3D%3D%3D%20%27Personio%27%29%20return%20t.split%28%27%7C%27%29%5B0%5D.trim%28%29%3B%20const%20parts%20%3D%20dropCompanyPart%28splitParts%28t%29%2C%20company%29%3B%20if%20%28parts.length%20%3D%3D%3D%201%29%20return%20stripCompany%28parts%5B0%5D%2C%20company%29.replace%28%2F%5E%5B-%E2%80%93%E2%80%94%7C%2C.%5Cs%5D%2B%2F%2C%20%27%27%29.trim%28%29%3B%20return%20%28src%20%3D%3D%3D%20%27Lever%27%20%3F%20parts.join%28%27%20-%20%27%29%20%3A%20parts%5B0%5D%29.trim%28%29%3B%20%7D%3B%20const%20s2w%20%3D%20s%20%3D%3E%20%7B%20try%20%7B%20s%20%3D%20decodeURIComponent%28s%20%7C%7C%20%27%27%29%3B%20%7D%20catch%20%28e%29%20%7B%7D%20return%20String%28s%20%7C%7C%20%27%27%29.replace%28%2F%28%3F%3C%3D%5Ba-z%5D%29%5Cd%2B%24%2Fi%2C%20%27%27%29.split%28%2F%5B-_%5Cs%5D%2F%29.filter%28Boolean%29.map%28w%20%3D%3E%20w.charAt%280%29.toUpperCase%28%29%20%2B%20w.slice%281%29%29.join%28%27%20%27%29%3B%20%7D%3B%20const%20ats%20%3D%20url%20%3D%3E%20%7B%20const%20h%20%3D%20url.hostname.replace%28%2F%5Ewww%5C.%2F%2C%20%27%27%29%3B%20const%20p%20%3D%20url.pathname.split%28%27%2F%27%29.filter%28Boolean%29%3B%20if%20%28h%20%3D%3D%3D%20%27jobs.lever.co%27%29%20return%20%7B%20src%3A%20%27Lever%27%2C%20company%3A%20p%5B0%5D%20%7D%3B%20if%20%28%2Fgreenhouse%5C.io%24%2F.test%28h%29%29%20return%20%7B%20src%3A%20%27Greenhouse%27%2C%20company%3A%20p%5B0%5D%20%3D%3D%3D%20%27embed%27%20%3F%20url.searchParams.get%28%27for%27%29%20%3A%20p%5B0%5D%20%7D%3B%20if%20%28h%20%3D%3D%3D%20%27jobs.ashbyhq.com%27%29%20return%20%7B%20src%3A%20%27Ashby%27%2C%20company%3A%20p%5B0%5D%20%7D%3B%20if%20%28h.endsWith%28%27.teamtailor.com%27%29%29%20return%20%7B%20src%3A%20%27Teamtailor%27%2C%20company%3A%20h.split%28%27.%27%29%5B0%5D%20%7D%3B%20if%20%28h%20%3D%3D%3D%20%27apply.workable.com%27%29%20return%20%7B%20src%3A%20%27Workable%27%2C%20company%3A%20p%5B0%5D%20%3D%3D%3D%20%27j%27%20%3F%20%27%27%20%3A%20p%5B0%5D%20%7D%3B%20if%20%28h.endsWith%28%27.recruitee.com%27%29%29%20return%20%7B%20src%3A%20%27Recruitee%27%2C%20company%3A%20h.split%28%27.%27%29%5B0%5D%20%7D%3B%20const%20sub%20%3D%20%5B%5B%27jobs.personio.de%27%2C%20%27Personio%27%5D%2C%20%5B%27jobs.personio.com%27%2C%20%27Personio%27%5D%2C%20%5B%27digitalrecruiters.com%27%2C%20%27DigitalRecruiters%27%5D%2C%20%5B%27breezy.hr%27%2C%20%27Breezy%27%5D%2C%20%5B%27bamboohr.com%27%2C%20%27BambooHR%27%5D%2C%20%5B%27myworkdayjobs.com%27%2C%20%27Workday%27%5D%2C%20%5B%27werecruit.io%27%2C%20%27Werecruit%27%5D%2C%20%5B%27flatchr.io%27%2C%20%27Flatchr%27%5D%5D.find%28%28%5Bd%5D%29%20%3D%3E%20h.endsWith%28%27.%27%20%2B%20d%29%29%3B%20if%20%28sub%29%20return%20%7B%20src%3A%20sub%5B1%5D%2C%20company%3A%20h.split%28%27.%27%29%5B0%5D%20%7D%3B%20const%20named%20%3D%20%7B%20%27jobs.smartrecruiters.com%27%3A%20%27SmartRecruiters%27%2C%20%27join.com%27%3A%20%27JOIN%27%20%7D%3B%20if%20%28named%5Bh%5D%20%7C%7C%20h.endsWith%28%27taleez.com%27%29%20%7C%7C%20h.endsWith%28%27jobaffinity.fr%27%29%29%20return%20%7B%20src%3A%20named%5Bh%5D%20%7C%7C%20%28h.endsWith%28%27taleez.com%27%29%20%3F%20%27Taleez%27%20%3A%20%27Jobaffinity%27%29%2C%20company%3A%20p%5B0%5D%20%3D%3D%3D%20%27companies%27%20%3F%20p%5B1%5D%20%3A%20p%5B0%5D%20%7D%3B%20if%20%28h.endsWith%28%27welcometothejungle.com%27%29%29%20%7B%20const%20i%20%3D%20p.indexOf%28%27companies%27%29%3B%20return%20%7B%20src%3A%20%27WTTJ%27%2C%20company%3A%20i%20%3E%3D%200%20%3F%20p%5Bi%20%2B%201%5D%20%3A%20%27%27%20%7D%3B%20%7D%20if%20%28h.endsWith%28%27linkedin.com%27%29%20%26%26%20url.pathname.startsWith%28%27%2Fjobs%2Fview%27%29%29%20%7B%20let%20slug%20%3D%20p%5B2%5D%20%7C%7C%20%27%27%3B%20try%20%7B%20slug%20%3D%20decodeURIComponent%28slug%29%3B%20%7D%20catch%20%28e%29%20%7B%7D%20const%20mm%20%3D%20slug.match%28%2F%5E%28.%2B%29-at-%28.%2B%29-%5Cd%2B%24%2Fi%29%3B%20return%20%7B%20src%3A%20%27LinkedIn%27%2C%20company%3A%20mm%20%3F%20mm%5B2%5D%20%3A%20%27%27%20%7D%3B%20%7D%20return%20%7B%20src%3A%20%27Other%27%2C%20company%3A%20h%20%7D%3B%20%7D%3B%20for%20%28let%20pg%20%3D%200%3B%20pg%20%3C%20maxPages%3B%20pg%2B%2B%29%20%7B%20u.searchParams.set%28%27start%27%2C%20pg%20%2A%2010%29%3B%20const%20r%20%3D%20await%20fetch%28u.toString%28%29%2C%20%7B%20credentials%3A%20%27include%27%20%7D%29%3B%20const%20d%20%3D%20new%20DOMParser%28%29.parseFromString%28await%20r.text%28%29%2C%20%27text%2Fhtml%27%29%3B%20const%20before%20%3D%20m.size%3B%20let%20found%20%3D%200%3B%20d.querySelectorAll%28%27a%20h3%27%29.forEach%28h3%20%3D%3E%20%7B%20const%20a%20%3D%20h3.closest%28%27a%27%29%3B%20if%20%28%21a%29%20return%3B%20let%20href%20%3D%20a.getAttribute%28%27href%27%29%20%7C%7C%20%27%27%3B%20if%20%28href.startsWith%28%27%2Furl%3F%27%29%29%20href%20%3D%20new%20URLSearchParams%28href.slice%285%29%29.get%28%27q%27%29%20%7C%7C%20%27%27%3B%20let%20pu%3B%20try%20%7B%20pu%20%3D%20new%20URL%28href%2C%20location.origin%29%3B%20%7D%20catch%20%28e%29%20%7B%20return%3B%20%7D%20if%20%28%2F%5C.translate%5C.goog%24%2Fi.test%28pu.hostname%29%29%20%7B%20const%20inner%20%3D%20pu.searchParams.get%28%27u%27%29%3B%20if%20%28%21inner%29%20return%3B%20try%20%7B%20pu%20%3D%20new%20URL%28inner%29%3B%20%7D%20catch%20%28e%29%20%7B%20return%3B%20%7D%20%7D%20if%20%28%2F%28%5E%7C%5C.%29google%5C.%2F.test%28pu.hostname%29%29%20return%3B%20found%2B%2B%3B%20const%20key%20%3D%20pu.origin%20%2B%20pu.pathname.replace%28%2F%5C%2Fl%5C%2F%5Ba-z%5D%7B2%7D%28%3F%3D%5C%2Fo%5C%2F%29%2F%2C%20%27%27%29.replace%28%2F%5C%2F%28c%5C%2Fnew%7Capply%29%5C%2F%3F%24%2F%2C%20%27%27%29%3B%20if%20%28%21m.has%28key%29%29%20%7B%20const%20info%20%3D%20ats%28pu%29%3B%20const%20block%20%3D%20blockOf%28a%29%3B%20const%20title%20%3D%20%28h3.textContent%20%7C%7C%20%27%27%29.trim%28%29.replace%28%2F%5Cs%2B%2Fg%2C%20%27%20%27%29%3B%20const%20snippet%20%3D%20%28block.textContent%20%7C%7C%20%27%27%29.replace%28h3.textContent%20%7C%7C%20%27%27%2C%20%27%20%27%29.replace%28%2F%5Cs%2B%2Fg%2C%20%27%20%27%29%3B%20const%20bolds%20%3D%20%5B...block.querySelectorAll%28%27em%2C%20b%27%29%5D.map%28e%20%3D%3E%20e.textContent%20%7C%7C%20%27%27%29%3B%20m.set%28key%2C%20%7B%20title%2C%20snippet%2C%20bolds%2C%20src%3A%20info.src%2C%20company%3A%20s2w%28info.company%29%20%7D%29%3B%20%7D%20%7D%29%3B%20if%20%28found%20%3D%3D%3D%200%20%7C%7C%20m.size%20%3D%3D%3D%20before%29%20break%3B%20await%20new%20Promise%28res%20%3D%3E%20setTimeout%28res%2C%20delayMs%29%29%3B%20%7D%20const%20esc%20%3D%20s%20%3D%3E%20%27%22%27%20%2B%20String%28s%29.replace%28%2F%22%2Fg%2C%20%27%22%22%27%29%20%2B%20%27%22%27%3B%20const%20rows%20%3D%20%5B%5B%27Title%27%2C%20%27Company%27%2C%20%27Role%27%2C%20%27Location%27%2C%20%27Pays%27%2C%20%27Stack%27%2C%20%27Source%27%2C%20%27URL%27%5D.map%28esc%29.join%28%27%2C%27%29%5D%3B%20m.forEach%28%28info%2C%20link%29%20%3D%3E%20%7B%20const%20g%20%3D%20locate%28info.title%2C%20info.snippet%2C%20info.src%2C%20info.bolds%2C%20link%29%3B%20rows.push%28%5Binfo.title%2C%20info.company%2C%20roleOf%28info.title%2C%20info.company%2C%20info.src%29%2C%20g.loc%2C%20g.pays%2C%20stackOf%28info.title%2C%20info.snippet%2C%20info.bolds%29%2C%20info.src%2C%20link%5D.map%28esc%29.join%28%27%2C%27%29%29%3B%20%7D%29%3B%20const%20blob%20%3D%20new%20Blob%28%5Brows.join%28%27%5Cr%5Cn%27%29%5D%2C%20%7B%20type%3A%20%27text%2Fcsv%3Bcharset%3Dutf-8%3B%27%20%7D%29%3B%20const%20bu%20%3D%20URL.createObjectURL%28blob%29%3B%20const%20dl%20%3D%20document.createElement%28%27a%27%29%3B%20dl.href%20%3D%20bu%3B%20dl.download%20%3D%20%27google-jobs-%27%20%2B%20Date.now%28%29%20%2B%20%27.csv%27%3B%20document.body.appendChild%28dl%29%3B%20dl.click%28%29%3B%20document.body.removeChild%28dl%29%3B%20setTimeout%28%28%29%20%3D%3E%20URL.revokeObjectURL%28bu%29%2C%205000%29%3B%20alert%28%27Done%3A%20%27%20%2B%20m.size%20%2B%20%27%20unique%20results%20exported%20to%20CSV.%27%29%3B%20%7D%29%28%29%3B
