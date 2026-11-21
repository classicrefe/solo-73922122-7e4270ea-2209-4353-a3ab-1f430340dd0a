#!/usr/bin/env node
/*
 * DOM-level test of the waveform page's incremental refresh.
 *
 * Loads the real index.html + refresh.js with jsdom, stubs fetch to replay a
 * scripted sequence of full/delta/stale responses, and asserts on the rendered
 * table: unchanged rows are not re-rendered, in-place status changes update the
 * status badge and the download button, and rows leaving the window disappear.
 *
 * jsdom is an optional dev dependency (not needed to run the app):
 *   npm install --no-save jsdom
 *   NODE_PATH=$PWD/node_modules node tests/test_refresh_dom.js
 * The test skips (exit 0) if jsdom cannot be resolved.
 */
'use strict';

const assert = require('assert');
const fs = require('fs');
const path = require('path');

let JSDOM;
try {
  ({ JSDOM } = require('jsdom'));
} catch (e) {
  console.log('SKIP: jsdom not installed (npm install --no-save jsdom)');
  process.exit(0);
}

const STATIC = path.join(__dirname, '..', 'web', 'app', 'static');
// Inline refresh.js so the page runs without an HTTP server or relative URLs.
const model = fs.readFileSync(path.join(STATIC, 'refresh.js'), 'utf8');
const html = fs.readFileSync(path.join(STATIC, 'index.html'), 'utf8')
  .replace('<script src="/static/refresh.js"></script>',
           '<script>' + model + '</script>');

let failures = 0;
function check(cond, label) {
  if (cond) console.log('  PASS  ' + label);
  else { failures += 1; console.log('  FAIL  ' + label); }
}

function job(id, status, extra) {
  return Object.assign({
    id, channel: 1, status, error: null, created_at: 1000 + id,
    started_at: null, finished_at: null, sample_interval: null,
    volt_scale: null, volt_offset: null, points: null, has_samples: false,
  }, extra || {});
}

const responses = [];
const seenUrls = [];
function fakeFetch(url) {
  seenUrls.push(String(url));
  const next = responses.shift();
  if (!next) return Promise.resolve({ok: true, json: async () => ({})});
  return Promise.resolve({ok: true, json: async () => next});
}

async function settle(win) {
  for (let i = 0; i < 5; i++) {
    await new Promise(resolve => win.setTimeout(resolve, 5));
  }
}

(async () => {
  // The page reads location.search; give jsdom a document URL.
  const dom = new JSDOM(html, {
    runScripts: 'dangerously',
    resources: 'usable',
    url: 'http://localhost:8000/',
    beforeParse(win) {
      win.fetch = fakeFetch;
      win.setInterval = () => 0;  // drive polls manually
    },
  });
  const win = dom.window;
  await new Promise(resolve => win.addEventListener('load', resolve));
  const doc = win.document;
  const rows = () => Array.from(doc.querySelectorAll('#rows tr[id^="row-"]'));
  const row = id => doc.getElementById('row-' + id);
  const text = id => row(id) ? row(id).textContent : '';

  // Health check consumes one fetch; scripted responses start after load.
  seenUrls.length = 0;

  // ---- first load: full page ----------------------------------------------
  responses.push({mode: 'full', epoch: 'e', started: 100, cursor: 10,
                  records: [job(2, 'done', {has_samples: true, points: 4096}),
                            job(1, 'queued')]});
  win.poll();
  await settle(win);
  check(rows().map(r => Number(r.id.slice(4))).join(',') === '2,1',
        'first load renders newest first');
  check(/下载CSV/.test(text(2)), 'completed row shows download button');
  check(/取消/.test(text(1)) && !/下载CSV/.test(text(1)),
        'queued row shows cancel only');
  check(seenUrls[0].includes('/api/acquisitions/changes') &&
        !seenUrls[0].includes('since='), 'first request has no cursor');
  const row2Node = row(2);

  // ---- delta with an in-place completion ----------------------------------
  responses.push({mode: 'delta', epoch: 'e', started: 100, cursor: 12,
                  records: [job(1, 'done', {has_samples: true, points: 4096})]});
  win.poll();
  await settle(win);
  check(row(2) === row2Node, 'unchanged row was not re-created');
  check(/完成/.test(text(1)), 'existing row updated to done');
  check(/下载CSV/.test(text(1)), 'download button appears after completion');
  check(!/取消/.test(text(1)), 'cancel button removed after completion');
  check(rows().length === 2, 'no duplicate row for the updated job');
  check(seenUrls[seenUrls.length - 1].includes('since=10') &&
        seenUrls[seenUrls.length - 1].includes('epoch=e'),
        'poll sent cursor + epoch');
  const htmlBefore = row(2).innerHTML;

  // ---- unchanged poll: no DOM work ----------------------------------------
  responses.push({mode: 'delta', epoch: 'e', started: 100, cursor: 12});
  win.poll();
  await settle(win);
  check(row(2).innerHTML === htmlBefore && row(1) === row(1),
        'unchanged poll leaves the rows untouched');

  // ---- new row + removal of a row that left the window --------------------
  responses.push({mode: 'delta', epoch: 'e', started: 100, cursor: 15,
                  records: [job(3, 'queued')], removed: [2]});
  win.poll();
  await settle(win);
  check(!row(2), 'removed row dropped from the DOM');
  check(rows().map(r => Number(r.id.slice(4))).join(',') === '3,1',
        'new row appears and order is maintained');

  // ---- stale response is ignored ------------------------------------------
  const snapshot = doc.getElementById('rows').innerHTML;
  responses.push({mode: 'delta', epoch: 'e', started: 100, cursor: 13,
                  records: [job(1, 'running'), job(2, 'done')]});
  win.poll();
  await settle(win);
  check(doc.getElementById('rows').innerHTML === snapshot,
        'late response with an older cursor is ignored');
  check(!row(2), 'stale response does not resurrect a removed row');

  // ---- server restart full page with a higher generation ------------------
  responses.push({mode: 'full', epoch: 'e2', started: 200, cursor: 1,
                  records: [job(3, 'done', {has_samples: true, points: 4096})]});
  win.poll();
  await settle(win);
  check(rows().map(r => Number(r.id.slice(4))).join(',') === '3',
        'restart full page replaces the window');
  check(/下载CSV/.test(text(3)), 'restart page enables download for stored row');

  // ---- late full page from the old generation is rejected -----------------
  responses.push({mode: 'full', epoch: 'e', started: 100, cursor: 99,
                  records: [job(1, 'queued'), job(2, 'queued')]});
  win.poll();
  await settle(win);
  check(rows().map(r => Number(r.id.slice(4))).join(',') === '3',
        'late full page from the old process does not roll back');

  win.close();
  if (failures) {
    console.log('\n' + failures + ' FAILURE(S)');
    process.exit(1);
  }
  console.log('\nALL DOM CHECKS PASSED');
})().catch(err => {
  console.error(err);
  process.exit(1);
});
