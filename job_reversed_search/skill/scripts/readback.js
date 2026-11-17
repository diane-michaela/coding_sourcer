/*
 * readback.js -- bring the collected rows from the browser into Claude.
 *
 * Run ONCE after fast-extract.js (or page-extract.js FINALIZE), in the same
 * tab. It turns window.jrsCsv into a compact text: one line per posting,
 * fields separated by the broken bar character (U+00A6), with the parts that
 * are identical on every row removed (the ID timestamp, Source, and the
 * https://xx.linkedin.com/jobs/view/ prefix). That cuts the size by about a
 * quarter. It returns the timestamp, URL prefix, row count and length.
 *
 * Then read window.jrsCompact in slices of 950 characters, about 10 slices
 * per browser call, and append each call's output to compact.txt in the
 * container as you go (do not wait for the end). Finally run
 * scripts/build-postings.py to rebuild the exact bookmarklet CSV plus the
 * Company type column.
 */
(() => {
  const parse = l => { const out = []; let i = 0; while (i < l.length) { i++; let s = ''; while (i < l.length) { if (l[i] === '"') { if (l[i+1] === '"') { s += '"'; i += 2; } else { i++; break; } } else s += l[i++]; } out.push(s); i++; } return out; };
  const rows = window.jrsCsv.split('\r\n').slice(1).map(parse);
  const ts = rows.length ? rows[0][0].split('-')[0] : '';
  const prefix = rows.length ? rows[0][8].match(/^https:\/\/[a-z]+\.linkedin\.com\/jobs\/view\//)[0] : '';
  window.jrsCompact = rows.map(f => [f[1], f[2], f[3], f[4], f[5], f[6], f[8].startsWith(prefix) ? f[8].slice(prefix.length) : '!' + f[8]].join('\u00a6')).join('\n');
  return JSON.stringify({ ts, prefix, rows: rows.length, len: window.jrsCompact.length });
})()
