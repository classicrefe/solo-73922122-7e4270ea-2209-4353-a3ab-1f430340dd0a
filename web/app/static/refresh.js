/*
 * Client-side model for the incremental acquisition list.
 *
 * The server returns either a full page (first load, server restart, cursor
 * older than its bounded history) or a delta:
 *   {mode:"delta", epoch, started, cursor, records?: [...], removed?: [ids]}
 * Records carry only the metadata columns, never the sample blob.
 *
 * This module merges responses into a bounded newest-first window and rejects
 * responses that belong to an older page version, so a late response can never
 * roll the UI back to an older state.  A page version is identified by
 * (epoch, started, cursor): epoch changes on every server start, `started` is
 * the server boot time so a response still in flight from the previous process
 * is rejected even when it is a full page, and cursor increases across
 * mutations within one process.
 *
 * It is a UMD-ish module: usable from the browser as `AcqRefresh`, and from
 * Node for tests via `require`.
 */
(function (root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.AcqRefresh = api;
})(typeof globalThis !== 'undefined' ? globalThis : this, function () {
  'use strict';

  const DEFAULT_PAGE_SIZE = 50;

  function createState(pageSize) {
    return {
      pageSize: pageSize || DEFAULT_PAGE_SIZE,
      jobs: new Map(),
      order: [],
      cursor: null,
      epoch: null,
      started: null,
    };
  }

  function windowIds(jobs, pageSize) {
    return Array.from(jobs.keys()).sort((a, b) => b - a).slice(0, pageSize);
  }

  // Same-process responses are ordered by cursor. Responses from another
  // server process are judged by that process's boot time: a page produced by
  // a process that started before the one we already know about is stale, even
  // if it is a full page (e.g. it was in flight during a restart).
  function isStale(state, data) {
    if (state.epoch !== null && data.epoch === state.epoch) {
      return state.cursor !== null && typeof data.cursor === 'number' &&
        data.cursor < state.cursor;
    }
    return state.started !== null && typeof data.started === 'number' &&
      data.started < state.started;
  }

  function rejected() {
    return { applied: false, full: false, changed: [], removed: [] };
  }

  function applyFull(state, data) {
    if (isStale(state, data)) return rejected();
    state.jobs = new Map();
    for (const job of data.records || []) state.jobs.set(job.id, job);
    state.order = windowIds(state.jobs, state.pageSize);
    state.cursor = data.cursor;
    state.epoch = data.epoch;
    state.started = typeof data.started === 'number' ? data.started : state.started;
    return { applied: true, full: true, changed: state.order.slice(), removed: [] };
  }

  function applyDelta(state, data) {
    if (isStale(state, data)) return rejected();
    // A delta from another server process is untrustworthy: wait for the full
    // page the next poll will request (its cursor/epoch are still the old one).
    if (state.epoch !== null && data.epoch !== state.epoch) return rejected();

    const changed = [];
    for (const job of data.records || []) {
      state.jobs.set(job.id, job);
      changed.push(job.id);
    }

    const removed = [];
    for (const id of data.removed || []) {
      if (state.jobs.delete(id)) removed.push(id);
    }

    // Recompute the newest pageSize window; prune anything that fell out so
    // the client map stays bounded like the server history.
    state.order = windowIds(state.jobs, state.pageSize);
    const keep = new Set(state.order);
    for (const id of Array.from(state.jobs.keys())) {
      if (!keep.has(id)) {
        state.jobs.delete(id);
        if (!removed.includes(id)) removed.push(id);
      }
    }
    state.cursor = data.cursor;
    state.epoch = data.epoch;
    state.started = typeof data.started === 'number' ? data.started : state.started;

    const changedInWindow = changed.filter((id, i) =>
      keep.has(id) && changed.indexOf(id) === i);
    return {
      applied: true,
      full: false,
      changed: changedInWindow,
      removed: removed,
    };
  }

  function apply(state, data) {
    if (!data || typeof data !== 'object') return rejected();
    if (data.mode === 'full') return applyFull(state, data);
    if (data.mode === 'delta' && typeof data.cursor === 'number') {
      return applyDelta(state, data);
    }
    return rejected();
  }

  return {
    DEFAULT_PAGE_SIZE,
    createState,
    apply,
    applyFull,
    applyDelta,
    isStale,
    windowIds,
  };
});
