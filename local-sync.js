'use strict';

const fetch = require('node-fetch');
const Database = require('better-sqlite3');
const express = require('express');
const path = require('path');
const fs = require('fs');

const app = express();
app.use(express.json());

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const RENDER_URL = process.env.RENDER_URL || 'http://localhost:3000';
const LOCAL_PORT = 3001;
const SYNC_INTERVAL_MS = 5 * 60 * 1000;
const DB_PATH = path.join(__dirname, 'coinex-alpha.db');

// ─── INIT LOCAL DB ─────────────────────────────────────────────────────────
const db = new Database(DB_PATH);

db.exec(`
  CREATE TABLE IF NOT EXISTS coinex_scan_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    supabase_id INTEGER,
    timestamp TEXT,
    market TEXT,
    name TEXT,
    first_seen TEXT,
    price_at_first_seen REAL,
    price_change_5m REAL,
    price_change_1h REAL,
    price_change_24h REAL,
    volume_24h REAL,
    volume_1h REAL,
    buy_ratio INTEGER,
    last_price REAL,
    high_24h REAL,
    low_24h REAL,
    mc REAL,
    score INTEGER,
    session TEXT,
    is_suspicious INTEGER DEFAULT 0
  );
  CREATE INDEX IF NOT EXISTS idx_local_timestamp ON coinex_scan_logs(timestamp);
  CREATE INDEX IF NOT EXISTS idx_local_name ON coinex_scan_logs(name);
  CREATE INDEX IF NOT EXISTS idx_local_score ON coinex_scan_logs(score);
  CREATE INDEX IF NOT EXISTS idx_local_session ON coinex_scan_logs(session);
`);

// ─── SYNC STATE ────────────────────────────────────────────────────────────
const syncState = {
  lastSyncTime: null,
  recordsSynced: 0,
  totalLocalRecords: 0,
  syncing: false,
  lastError: null
};

// ─── FETCH WITH ABORT ──────────────────────────────────────────────────────
async function fetchWithTimeout(url, options = {}, timeoutMs = 15000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, { ...options, signal: controller.signal });
    clearTimeout(timer);
    return res;
  } catch (err) {
    clearTimeout(timer);
    throw err;
  }
}

// ─── GET LAST SYNCED TIMESTAMP ─────────────────────────────────────────────
function getLastSyncedTimestamp() {
  const row = db.prepare('SELECT MAX(timestamp) as last FROM coinex_scan_logs').get();
  return row?.last || new Date(0).toISOString();
}

// ─── SYNC FROM SUPABASE ────────────────────────────────────────────────────
async function syncFromSupabase() {
  if (syncState.syncing) return;
  syncState.syncing = true;

  try {
    const lastTs = getLastSyncedTimestamp();
    console.log(`Syncing from ${lastTs}...`);

    const url = `${SUPABASE_URL}/rest/v1/coinex_scan_logs?timestamp=gt.${encodeURIComponent(lastTs)}&order=timestamp.asc&limit=5000`;
    const res = await fetchWithTimeout(url, {
      headers: {
        'apikey': SUPABASE_KEY,
        'Authorization': `Bearer ${SUPABASE_KEY}`
      }
    }, 20000);

    if (!res.ok) throw new Error(`Supabase fetch error: ${res.status}`);
    const rows = await res.json();

    if (rows.length === 0) {
      console.log('No new rows to sync');
      syncState.lastSyncTime = new Date().toISOString();
      syncState.syncing = false;
      return;
    }

    const insert = db.prepare(`
      INSERT OR IGNORE INTO coinex_scan_logs 
      (supabase_id, timestamp, market, name, first_seen, price_at_first_seen,
       price_change_5m, price_change_1h, price_change_24h, volume_24h, volume_1h,
       buy_ratio, last_price, high_24h, low_24h, mc, score, session, is_suspicious)
      VALUES 
      (@id, @timestamp, @market, @name, @first_seen, @price_at_first_seen,
       @price_change_5m, @price_change_1h, @price_change_24h, @volume_24h, @volume_1h,
       @buy_ratio, @last_price, @high_24h, @low_24h, @mc, @score, @session, @is_suspicious)
    `);

    const insertMany = db.transaction(rows => {
      for (const row of rows) insert.run({
        ...row,
        is_suspicious: row.is_suspicious ? 1 : 0
      });
    });

    insertMany(rows);
    syncState.recordsSynced += rows.length;
    syncState.lastSyncTime = new Date().toISOString();
    syncState.lastError = null;

    const countRow = db.prepare('SELECT COUNT(*) as c FROM coinex_scan_logs').get();
    syncState.totalLocalRecords = countRow.c;

    console.log(`Synced ${rows.length} rows. Total local: ${syncState.totalLocalRecords}`);

  } catch (err) {
    syncState.lastError = err.message;
    console.error('Sync error:', err.message);
  } finally {
    syncState.syncing = false;
  }
}

// ─── LOCAL API ROUTES ──────────────────────────────────────────────────────
app.get('/status', (req, res) => {
  const countRow = db.prepare('SELECT COUNT(*) as c FROM coinex_scan_logs').get();
  res.json({
    status: 'running',
    lastSyncTime: syncState.lastSyncTime,
    recordsSynced: syncState.recordsSynced,
    totalLocalRecords: countRow.c,
    syncing: syncState.syncing,
    lastError: syncState.lastError,
    dbPath: DB_PATH,
    dbSizeMB: (fs.statSync(DB_PATH).size / 1024 / 1024).toFixed(2)
  });
});

app.get('/history/deep', (req, res) => {
  try {
    const { start, end, limit = 2000 } = req.query;
    let query = 'SELECT * FROM coinex_scan_logs WHERE 1=1';
    const params = [];
    if (start) { query += ' AND timestamp >= ?'; params.push(start); }
    if (end) { query += ' AND timestamp <= ?'; params.push(end); }
    query += ' ORDER BY timestamp DESC LIMIT ?';
    params.push(parseInt(limit));
    const rows = db.prepare(query).all(...params);
    res.json({ data: rows, count: rows.length });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/history/token', (req, res) => {
  try {
    const { name } = req.query;
    if (!name) return res.status(400).json({ error: 'name required' });
    const rows = db.prepare(
      'SELECT * FROM coinex_scan_logs WHERE name LIKE ? ORDER BY timestamp ASC LIMIT 1000'
    ).all(`%${name}%`);
    res.json({ data: rows });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/history/lowmc', (req, res) => {
  try {
    const rows = db.prepare(
      'SELECT * FROM coinex_scan_logs WHERE mc < 5000000 AND score >= 55 AND price_change_24h > 20 ORDER BY price_change_24h DESC LIMIT 500'
    ).all();
    const sessionBreakdown = { Asia: 0, Europe: 0, US: 0 };
    rows.forEach(r => { if (r.session) sessionBreakdown[r.session] = (sessionBreakdown[r.session] || 0) + 1; });
    res.json({ data: rows, count: rows.length, sessionBreakdown });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/history/sessions', (req, res) => {
  try {
    const sessions = ['Asia', 'Europe', 'US'];
    const breakdown = {};
    sessions.forEach(s => {
      const rows = db.prepare(
        'SELECT score, price_change_24h FROM coinex_scan_logs WHERE session = ? AND score >= 50'
      ).all(s);
      breakdown[s] = {
        count: rows.length,
        avgScore: rows.length ? rows.reduce((a, b) => a + b.score, 0) / rows.length : 0,
        avgGain: rows.length ? rows.reduce((a, b) => a + (b.price_change_24h || 0), 0) / rows.length : 0,
        pumps: rows.filter(r => r.price_change_24h > 20).length
      };
    });
    res.json({ breakdown });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/sync/now', async (req, res) => {
  await syncFromSupabase();
  res.json({ status: 'synced', lastSyncTime: syncState.lastSyncTime });
});

app.get('/export', (req, res) => {
  try {
    const { format = 'csv', start, end, minScore = 0, session, category } = req.query;
    let query = 'SELECT * FROM coinex_scan_logs WHERE score >= ?';
    const params = [parseInt(minScore)];
    if (start) { query += ' AND timestamp >= ?'; params.push(start); }
    if (end) { query += ' AND timestamp <= ?'; params.push(end); }
    if (session) { query += ' AND session = ?'; params.push(session); }
    if (category === 'winners') { query += ' AND price_change_24h > 20'; }
    if (category === 'losers') { query += ' AND price_change_24h < -20'; }
    if (category === 'suspicious') { query += ' AND is_suspicious = 1'; }
    query += ' ORDER BY timestamp DESC LIMIT 10000';

    const rows = db.prepare(query).all(...params);

    if (format === 'json') {
      res.setHeader('Content-Disposition', 'attachment; filename=coinex-export.json');
      res.json(rows);
    } else {
      const headers = Object.keys(rows[0] || {}).join(',');
      const csv = [headers, ...rows.map(r => Object.values(r).join(','))].join('\n');
      res.setHeader('Content-Disposition', 'attachment; filename=coinex-export.csv');
      res.setHeader('Content-Type', 'text/csv');
      res.send(csv);
    }
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/cleanup', (req, res) => {
  try {
    const { before, minScore = 0, keepCategory, session } = req.query;
    let query = 'DELETE FROM coinex_scan_logs WHERE 1=1';
    const params = [];
    if (before) { query += ' AND timestamp < ?'; params.push(before); }
    if (minScore) { query += ' AND score < ?'; params.push(parseInt(minScore)); }
    if (session) { query += ' AND session = ?'; params.push(session); }
    if (keepCategory === 'winners') query += ' AND price_change_24h <= 20';

    const preview = db.prepare(query.replace('DELETE', 'SELECT COUNT(*) as c')).get(...params);
    if (req.query.preview === 'true') {
      return res.json({ wouldDelete: preview.c });
    }

    const result = db.prepare(query).run(...params);
    res.json({ deleted: result.changes });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── START ─────────────────────────────────────────────────────────────────
syncFromSupabase();
setInterval(syncFromSupabase, SYNC_INTERVAL_MS);

app.listen(LOCAL_PORT, () => {
  console.log(`CoinEx Local Sync running on http://localhost:${LOCAL_PORT}`);
  console.log(`DB: ${DB_PATH}`);
  console.log(`Syncing every 5 minutes from Supabase`);
});
