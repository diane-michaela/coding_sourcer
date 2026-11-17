/*
 * readback.js -- bring the collected rows from the browser into Claude.
 *
 * Run ONCE after fast-extract.js (or page-extract.js FINALIZE), in the same
 * tab. It turns the CSV into a compact text: one line per posting, fields
 * separated by the broken bar character (U+00A6), with the parts that are
 * identical on every row removed (the ID timestamp, Source, and the
 * https://xx.linkedin.com/jobs/view/ prefix). That cuts the size by about a
 * quarter. It returns the country, timestamp, URL prefix, row count and
 * length.
 *
 * Source of the rows: set KEY on the first line to a saved country key
 * (jrs-uk, jrs-pl, ...) to read it from sessionStorage, which is what a
 * multi-country run needs. Leave KEY empty to read window.jrsCsv.
 *
 * The compact text is kept in window.jrsCompact AND in sessionStorage under
 * jrs-compact, so it survives a page reload. Read it in slices of 950
 * characters, about 10 slices per browser call, for example
 *   sessionStorage.getItem('jrs-compact').slice(0, 950)
 * and append each call's output to <country code>.txt (one file per
 * country, e.g. uk.txt) in the container as you go. Then run
 * scripts/merge-countries.py (several countries) and scripts/build-postings.py.
 */
(() => {
  const KEY = '';
  const csv = KEY ? sessionStorage.getItem(KEY) : window.jrsCsv;
  if (!csv) return JSON.stringify({ error: 'no rows found for ' + (KEY || 'window.jrsCsv') });
  const parse = l => { const out = []; let i = 0; while (i < l.length) { i++; let s = ''; while (i < l.length) { if (l[i] === '"') { if (l[i+1] === '"') { s += '"'; i += 2; } else { i++; break; } } else s += l[i++]; } out.push(s); i++; } return out; };
  const rows = csv.split('\r\n').slice(1).map(parse);
  const ts = rows.length ? rows[0][0].split('-')[0] : '';
  const pm = rows.length ? rows[0][8].match(/^https:\/\/([a-z]+)\.linkedin\.com\/jobs\/view\//) : null;
  const prefix = pm ? pm[0] : '';
  const compact = rows.map(f => [f[1], f[2], f[3], f[4], f[5], f[6], prefix && f[8].startsWith(prefix) ? f[8].slice(prefix.length) : '!' + f[8]].join('\u00a6')).join('\n');
  window.jrsCompact = compact;
  try { sessionStorage.setItem('jrs-compact', compact); } catch (e) {}
  return JSON.stringify({ country: pm ? pm[1].toUpperCase() : '?', ts, prefix, rows: rows.length, len: compact.length });
})()
