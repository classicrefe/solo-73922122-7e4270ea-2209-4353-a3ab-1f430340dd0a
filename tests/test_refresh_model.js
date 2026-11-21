#!/usr/bin/env node
/*
 * Unit tests for the client-side incremental refresh model (refresh.js).
 * Run: node tests/test_refresh_model.js
 */
'use strict';

const assert = require('assert');
const path = require('path');
const refresh = require(path.join(__dirname, '..', 'web', 'app', 'static', 'refresh.js'));

let failures = 0;
function check(cond, label) {
  if (cond) {
    console.log('  PASS  ' + label);
  } else {
    failures += 1;
    console.log('  FAIL  ' + label);
  }
}

function job(id, status, extra) {
  return Object.assign({
    id, channel: 1, status, error: null, created_at: 1000 + id,
    started_at: null, finished_at: null, sample_interval: null,
    volt_scale: null, volt_offset: null, points: null, has_samples: false,
  }, extra || {});
}

// 1. First load: full page, newest first, cursor/epoch captured.
{
  const st = refresh.createState(3);
  const r = refresh.apply(st, {
    mode: 'full', epoch: 'e1', started: 100, cursor: 10,
    records: [job(3, 'queued'), job(2, 'done', {has_samples: true}), job(1, 'done')],
  });
  check(r.applied && r.full, 'full page applied');
  check(JSON.stringify(st.order) === JSON.stringify([3, 2, 1]), 'order is newest first');
  check(st.cursor === 10 && st.epoch === 'e1', 'cursor + epoch stored');
}

// 2. Unchanged delta: nothing changed, nothing removed.
{
  const st = refresh.createState(3);
  refresh.apply(st, {mode: 'full', epoch: 'e', started: 100, cursor: 5,
                     records: [job(1, 'done', {has_samples: true})]});
  const r = refresh.apply(st, {mode: 'delta', epoch: 'e', started: 100, cursor: 5});
  check(r.applied && !r.full, 'unchanged delta accepted');
  check(r.changed.length === 0 && r.removed.length === 0, 'unchanged delta has no changes');
}

// 3. In-place status update of an existing row (queued -> done) plus new row.
{
  const st = refresh.createState(3);
  refresh.apply(st, {mode: 'full', epoch: 'e', started: 100, cursor: 5,
                     records: [job(1, 'queued'), job(2, 'done')]});
  const r = refresh.apply(st, {
    mode: 'delta', epoch: 'e', started: 100, cursor: 7,
    records: [job(1, 'done', {has_samples: true, points: 4096}), job(3, 'queued')],
  });
  check(r.applied && r.full === false, 'delta applied');
  check(r.changed.sort().join(',') === '1,3', 'changed ids are 1 and 3');
  check(st.jobs.get(1).status === 'done' && st.jobs.get(1).has_samples === true,
        'existing row status and download flag updated in place');
  check(JSON.stringify(st.order) === JSON.stringify([3, 2, 1]), 'new row enters at the top, order kept');
}

// 4. Removal of a row that scrolled out of the window.
{
  const st = refresh.createState(2);
  refresh.apply(st, {mode: 'full', epoch: 'e', started: 100, cursor: 5,
                     records: [job(2, 'done'), job(1, 'done')]});
  const r = refresh.apply(st, {mode: 'delta', epoch: 'e', started: 100, cursor: 6,
                               records: [job(3, 'queued')], removed: [1]});
  check(r.removed.indexOf(1) >= 0, 'removed id reported');
  check(!st.jobs.has(1), 'removed row dropped from client state');
  check(JSON.stringify(st.order) === JSON.stringify([3, 2]), 'window is the newest 2');
  check(st.jobs.size === 2, 'client map stays bounded to the page size');
}

// 5. Late/stale response must not roll back a newer state.
{
  const st = refresh.createState(3);
  refresh.apply(st, {mode: 'full', epoch: 'e', started: 100, cursor: 5,
                     records: [job(1, 'running')]});
  // Apply a newer delta (job done, cursor 9).
  refresh.apply(st, {mode: 'delta', epoch: 'e', started: 100, cursor: 9,
                     records: [job(1, 'done', {has_samples: true})]});
  // A late response generated from cursor 5 arrives now.
  const r = refresh.apply(st, {mode: 'delta', epoch: 'e', started: 100, cursor: 6,
                               records: [job(1, 'running')]});
  check(r.applied === false, 'late delta rejected');
  check(st.jobs.get(1).status === 'done', 'stale response did not overwrite newer state');
  check(st.cursor === 9, 'cursor did not move backwards');

  const r2 = refresh.apply(st, {mode: 'full', epoch: 'e', started: 100, cursor: 7,
                                records: [job(1, 'queued')]});
  check(r2.applied === false && st.jobs.get(1).status === 'done',
        'late full page also rejected');
}

// 6. Server restart: new epoch full page replaces the local window.
{
  const st = refresh.createState(2);
  refresh.apply(st, {mode: 'full', epoch: 'old', started: 100, cursor: 50,
                     records: [job(5, 'done'), job(4, 'done')]});
  const r = refresh.apply(st, {mode: 'full', epoch: 'new', started: 200, cursor: 1,
                               records: [job(5, 'done')]});
  check(r.applied && r.full, 'new-epoch full page accepted');
  check(st.epoch === 'new' && st.cursor === 1, 'epoch/cursor replaced');
  check(st.jobs.size === 1 && st.order.join(',') === '5', 'window replaced by the page');
}

// 7. A delta from a different epoch is not merged.
{
  const st = refresh.createState(2);
  refresh.apply(st, {mode: 'full', epoch: 'old', started: 100, cursor: 50, records: [job(1, 'done')]});
  const r = refresh.apply(st, {mode: 'delta', epoch: 'new', started: 200, cursor: 51,
                               records: [job(2, 'queued')]});
  check(r.applied === false, 'cross-epoch delta rejected');
}

// 8. Duplicate ids in records are reported once.
{
  const st = refresh.createState(3);
  refresh.apply(st, {mode: 'full', epoch: 'e', started: 100, cursor: 1, records: [job(1, 'queued')]});
  const r = refresh.apply(st, {mode: 'delta', epoch: 'e', started: 100, cursor: 2,
                               records: [job(1, 'running'), job(1, 'running')]});
  check(r.changed.length === 1, 'changed ids de-duplicated');
}

// 9. Window recovers positions when a newer id is removed but the old row is
//    still within range (client keeps its own copy from the bounded map).
{
  const st = refresh.createState(3);
  refresh.apply(st, {mode: 'full', epoch: 'e', started: 100, cursor: 3,
                     records: [job(3, 'done'), job(2, 'done'), job(1, 'done')]});
  const r = refresh.apply(st, {mode: 'delta', epoch: 'e', started: 100, cursor: 4, removed: [3]});
  check(r.removed.join(',') === '3', 'top row removed');
  check(st.order.join(',') === '2,1', 'remaining rows kept in order');
}

// 10. A late full page from an older server process is rejected.
{
  const st = refresh.createState(2);
  refresh.apply(st, {mode: 'full', epoch: 'old', started: 100, cursor: 99,
                     records: [job(1, 'done')]});
  const r = refresh.apply(st, {mode: 'full', epoch: 'new', started: 200, cursor: 1,
                               records: [job(2, 'queued')]});
  check(r.applied && st.epoch === 'new', 'new-process full page applied');
  // This response was in flight from the old process when the restart happened.
  const late = refresh.apply(st, {mode: 'full', epoch: 'old', started: 100,
                                  cursor: 99, records: [job(1, 'done')]});
  check(late.applied === false, 'late full page from the older process rejected');
  check(st.epoch === 'new' && st.order.join(',') === '2',
        'page stays on the newer process snapshot');
}

if (failures) {
  console.log('\n' + failures + ' FAILURE(S)');
  process.exit(1);
}
console.log('\nALL MODEL CHECKS PASSED');
