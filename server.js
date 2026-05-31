'use strict';

const express = require('express');
const fetch = require('node-fetch');
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');
const path = require('path');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ─── ENV ───────────────────────────────────────────────────────────────────
const GROQ_API_KEY = process.env.GROQ_API_KEY;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const PORT = process.env.PORT || 3000;

// ─── SUPABASE ──────────────────────────────────────────────────────────────
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY, {
  realtime: { transport: ws }
});

// ─── CONSTANTS ─────────────────────────────────────────────────────────────
const COINEX_BASE = 'https://api.coinex.com/v2';
const SCAN_INTERVAL_MS = 30000;
const FULL_SNAPSHOT_INTERVAL = 20;
const STORAGE_WARN_PCT = 70;
const STORAGE_CLEANUP_PCT = 80;
const SUPABASE_FREE_BYTES = 500 * 1024 * 1024;
const BYTES_PER_ROW = 512;

// ─── SESSION HELPER ────────────────────────────────────────────────────────
function getCurrentSession() {
  const hour = new Date().getUTCHours();
  if (hour >= 0 && hour < 8) return 'Asia';
  if (hour >= 8 && hour < 16) return 'Europe';
  return 'US';
}

// ─── STATE ─────────────────────────────────────────────────────────────────
const state = {
  scanCount: 0,
  lastScanTime: null,
  lastScanTimestamp: null,
  winners: [],
  pumpTimeLog: [],
  fingerprint: {
    avgMC: 0, avgLiq: 0, avgBuyRatio: 0,
    avgVolAccel: 0, avgPumpAge: 0, avgScore: 0, count: 0
  },
  storageRows: 0,
  storageWarn: false,
  seenMarkets: new Set(),
  newListings: [],
  lastPrices: {},
  lastVolumes: {},
  cycleCount: 0
};

// ─── FETCH WITH ABORT ──────────────────────────────────────────────────────
async function fetchWithTimeout(url, options = {}, timeoutMs = 10000) {
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

// ─── COINEX FETCH HELPER ───────────────────────────────────────────────────
async function coinexGet(endpoint, params = {}) {
  const url = new URL(COINEX_BASE + endpoint);
  Object.entries(params).forEach(([k, v]) => url.searchParams.set(k, v));
  const res = await fetchWithTimeout(url.toString(), {
    headers: { 'Accept': 'application/json' }
  }, 12000);
  if (!res.ok) throw new Error(`CoinEx ${endpoint} ${res.status}`);
  return res.json();
}

// ─── NARRATIVE SCORE ───────────────────────────────────────────────────────
const NARRATIVES = [
  'AI', 'GPT', 'AGENT', 'BOT',
  'PEPE', 'DOGE', 'SHIB', 'FLOKI', 'BONK', 'WIF', 'MEME',
  'TRUMP', 'BIDEN', 'ELON', 'MUSK',
  'MOON', 'PUMP', 'GEM', 'X100', 'X1000',
  'BTC', 'SOL', 'ETH', 'BASE',
  'CAT', 'DOG', 'FROG', 'BIRD',
  'DEFI', 'DAO', 'NFT', 'WEB3'
];

function narrativeScore(name) {
  if (!name) return 0;
  const upper = name.toUpperCase();
  const hit = NARRATIVES.some(n => upper.includes(n));
  return hit ? 10 : 0;
}

// ─── TIMING INTELLIGENCE ───────────────────────────────────────────────────
function getTimingMultiplier() {
  if (state.pumpTimeLog.length < 5) return 1.0;
  const counts = {};
  state.pumpTimeLog.forEach(h => { counts[h] = (counts[h] || 0) + 1; });
  const sorted = Object.entries(counts).sort((a, b) => b[1] - a[1]);
  const total = sorted.length;
  const top = sorted.slice(0, Math.ceil(total * 0.25)).map(e => parseInt(e[0]));
  const bottom = sorted.slice(Math.floor(total * 0.75)).map(e => parseInt(e[0]));
  const hour = new Date().getUTCHours();
  if (top.includes(hour)) return 1.12;
  if (bottom.includes(hour)) return 0.88;
  return 1.0;
}

function getPeakQuietStatus() {
  if (state.pumpTimeLog.length < 5) return 'normal';
  const multiplier = getTimingMultiplier();
  if (multiplier === 1.12) return 'peak';
  if (multiplier === 0.88) return 'quiet';
  return 'normal';
}

// ─── PATTERN MATCH ─────────────────────────────────────────────────────────
function patternMatch(coin) {
  const fp = state.fingerprint;
  if (fp.count < 3) return 0;
  let score = 0;
  if (fp.avgMC > 0) {
    const ratio = coin.mc / fp.avgMC;
    if (ratio > 0.5 && ratio < 2) score += 25;
    else if (ratio > 0.25 && ratio < 4) score += 10;
  }
  if (fp.avgBuyRatio > 0) {
    const diff = Math.abs(coin.buyRatio - fp.avgBuyRatio);
    if (diff < 10) score += 25;
    else if (diff < 20) score += 10;
  }
  if (fp.avgVolAccel > 0) {
    const ratio = coin.volAccel / fp.avgVolAccel;
    if (ratio > 0.5 && ratio < 2) score += 25;
  }
  return Math.min(100, score);
}

// ─── SCORING ENGINE ────────────────────────────────────────────────────────
function scoreCoin(coin) {
  let score = 0;

  // Price momentum 24h (0-20)
  const ch24 = coin.priceChange24h || 0;
  if (ch24 > 50) score += 20;
  else if (ch24 > 30) score += 15;
  else if (ch24 > 15) score += 10;
  else if (ch24 > 5) score += 5;

  // Price momentum 1h (0-15)
  const ch1h = coin.priceChange1h || 0;
  if (ch1h > 20) score += 15;
  else if (ch1h > 10) score += 10;
  else if (ch1h > 5) score += 7;
  else if (ch1h > 2) score += 3;

  // Volume surge (0-20)
  const volAccel = coin.volAccel || 0;
  if (volAccel > 4) score += 20;
  else if (volAccel > 3) score += 15;
  else if (volAccel > 2) score += 10;
  else if (volAccel > 1.5) score += 5;

  // Order book imbalance (0-15)
  const br = coin.buyRatio || 50;
  if (br > 70) score += 15;
  else if (br > 60) score += 10;
  else if (br > 55) score += 5;

  // Distance from 24h low (0-10)
  if (coin.low24h > 0 && coin.lastPrice > 0 && coin.high24h > coin.low24h) {
    const range = coin.high24h - coin.low24h;
    const distFromLow = (coin.lastPrice - coin.low24h) / range;
    if (distFromLow < 0.15) score += 10;
    else if (distFromLow < 0.3) score += 6;
    else if (distFromLow < 0.5) score += 3;
  }

  // Distance from 24h high (0-10)
  if (coin.high24h > 0 && coin.lastPrice > 0 && coin.high24h > coin.low24h) {
    const range = coin.high24h - coin.low24h;
    const distFromHigh = (coin.high24h - coin.lastPrice) / range;
    if (distFromHigh < 0.05) score += 10;
    else if (distFromHigh < 0.1) score += 6;
    else if (distFromHigh < 0.2) score += 3;
  }

  // Narrative (0-10)
  score += narrativeScore(coin.name);

  // Pattern match (0-10)
  score += patternMatch(coin) * 0.1;

  // Penalties
  if (coin.volAccel < 0.3) score -= 10;
  if (ch24 < -30) score -= 15;
  if (coin.isSuspicious) score -= 20;
  if (br < 30 && coin.volume24h > 0) score -= 10;

  // Timing multiplier
  score = score * getTimingMultiplier();

  return Math.max(0, Math.min(100, Math.round(score)));
}

// ─── SIGNAL TAGS ───────────────────────────────────────────────────────────
function getSignalTags(coin) {
  const tags = [];
  if (coin.volAccel > 3) tags.push('VOL SURGE');
  if (coin.buyRatio > 65) tags.push('BUY PRESSURE');
  if (coin.buyRatio < 35) tags.push('SELL PRESSURE');
  if (coin.priceChange24h > 30) tags.push('MOMENTUM');
  if (coin.priceChange24h < -30) tags.push('ALREADY PUMPED');

  if (coin.high24h > 0 && coin.lastPrice > 0 && coin.high24h > coin.low24h) {
    const range = coin.high24h - coin.low24h;
    const distFromHigh = (coin.high24h - coin.lastPrice) / range;
    const distFromLow = (coin.lastPrice - coin.low24h) / range;
    if (distFromHigh < 0.05) tags.push('NEAR HIGH');
    if (distFromLow < 0.15) tags.push('NEAR LOW');
  }

  const pm = patternMatch(coin);
  if (pm > 60) tags.push('PATTERN MATCH');
  if (coin.isNewListing) tags.push('NEW LISTING');

  const mult = getTimingMultiplier();
  if (mult === 1.12) tags.push('PEAK HOUR');
  if (mult === 0.88) tags.push('QUIET HOUR');

  return tags;
}

// ─── SUSPICIOUS DETECTION ──────────────────────────────────────────────────
function isSuspicious(coin) {
  let s = 0;
  if (coin.priceChange24h < -60) s += 3;
  if (coin.volume24h < 100) s += 2;
  if (coin.buyRatio < 10 && coin.volume24h > 0) s += 3;
  if (coin.lastPrice <= 0) s += 5;
  return s >= 5;
}

// ─── FINGERPRINT UPDATE ────────────────────────────────────────────────────
function updateFingerprint(coin) {
  const fp = state.fingerprint;
  const n = fp.count;
  fp.avgMC = (fp.avgMC * n + (coin.mc || 0)) / (n + 1);
  fp.avgBuyRatio = (fp.avgBuyRatio * n + (coin.buyRatio || 0)) / (n + 1);
  fp.avgVolAccel = (fp.avgVolAccel * n + (coin.volAccel || 0)) / (n + 1);
  fp.avgScore = (fp.avgScore * n + (coin.score || 0)) / (n + 1);
  fp.count = n + 1;
  state.pumpTimeLog.push(new Date().getUTCHours());
  if (state.pumpTimeLog.length > 500) state.pumpTimeLog.shift();
}

// ─── STORAGE CHECK ─────────────────────────────────────────────────────────
async function checkStorage() {
  try {
    const { count } = await supabase
      .from('coinex_scan_logs')
      .select('*', { count: 'exact', head: true });
    state.storageRows = count || 0;
    const estimatedBytes = state.storageRows * BYTES_PER_ROW;
    const pct = (estimatedBytes / SUPABASE_FREE_BYTES) * 100;
    state.storageWarn = pct >= STORAGE_WARN_PCT;
    if (pct >= STORAGE_CLEANUP_PCT) await runAutoCleanup();
    return pct;
  } catch (e) {
    console.error('Storage check error:', e.message);
    return 0;
  }
}

// ─── AUTO CLEANUP ──────────────────────────────────────────────────────────
async function runAutoCleanup() {
  console.log('Running auto cleanup...');
  try {
    const cutoff = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
    await supabase
      .from('coinex_scan_logs')
      .delete()
      .lt('timestamp', cutoff)
      .lt('score', 40)
      .eq('is_suspicious', false);
    console.log('Auto cleanup complete');
  } catch (e) {
    console.error('Cleanup error:', e.message);
  }
}

// ─── MAIN BACKGROUND SCANNER ───────────────────────────────────────────────
async function backgroundScan() {
  try {
    state.cycleCount++;
    const isFullSnapshot = state.cycleCount % FULL_SNAPSHOT_INTERVAL === 0;
    const session = getCurrentSession();

    const tickerData = await coinexGet('/spot/ticker');
    if (!tickerData?.data) return;

    const tickers = tickerData.data;
    const usdtPairs = tickers.filter(t =>
      t.market && t.market.endsWith('USDT') && !t.market.startsWith('USDT')
    );

    const processed = [];

    for (const t of usdtPairs) {
      try {
        const market = t.market;
        const name = market.replace('USDT', '');
        const lastPrice = parseFloat(t.last) || 0;
        const high24h = parseFloat(t.high) || 0;
        const low24h = parseFloat(t.low) || 0;
        const volume24h = parseFloat(t.vol) || 0;
        const priceChange24h = parseFloat(t.change_rate) * 100 || 0;

        const prevVol = state.lastVolumes[market] || volume24h;
        const volume1h = Math.max(0, volume24h - prevVol);
        state.lastVolumes[market] = volume24h;

        const avgHourlyVol = volume24h / 24;
        const volAccel = avgHourlyVol > 0 ? volume1h / avgHourlyVol : 0;

        const prevPrice = state.lastPrices[market] || lastPrice;
        const priceChange5m = prevPrice > 0 ? ((lastPrice - prevPrice) / prevPrice) * 100 : 0;
        state.lastPrices[market] = lastPrice;
        const buyRatio = Math.min(100, Math.max(0, 50 + priceChange5m * 3));
        const priceChange1h = priceChange24h * 0.08;

        const mc = lastPrice * (volume24h / Math.max(lastPrice, 0.000001)) * 10;

        const suspicious = isSuspicious({
          priceChange24h, volume24h, buyRatio, lastPrice
        });

        const isNewListing = !state.seenMarkets.has(market);
        if (isNewListing) {
          state.seenMarkets.add(market);
          state.newListings.push({ market, name, seenAt: new Date().toISOString() });
          if (state.newListings.length > 50) state.newListings.shift();
        }

        const coin = {
          market, name, lastPrice, high24h, low24h,
          volume24h, volume1h, buyRatio,
          priceChange5m, priceChange1h, priceChange24h,
          volAccel, mc, isSuspicious: suspicious, isNewListing,
          session
        };

        coin.score = scoreCoin(coin);
        coin.tags = getSignalTags(coin);
        processed.push(coin);

        if (coin.score >= 70) updateFingerprint(coin);

      } catch (coinErr) {
        // skip bad coin
      }
    }

    processed.sort((a, b) => b.score - a.score);

    const toWrite = [];
    const top50 = processed.slice(0, 50);

    for (const coin of processed) {
      const isTop50 = top50.includes(coin);
      const prevPrice = state.lastPrices[coin.market + '_prev'] || coin.lastPrice;
      const prevVol = state.lastVolumes[coin.market + '_prev'] || coin.volume24h;
      const priceChanged = Math.abs(coin.lastPrice - prevPrice) / Math.max(prevPrice, 0.000001) > 0.005;
      const volChanged = Math.abs(coin.volume24h - prevVol) / Math.max(prevVol, 1) > 0.1;
      state.lastPrices[coin.market + '_prev'] = coin.lastPrice;
      state.lastVolumes[coin.market + '_prev'] = coin.volume24h;

      if (isTop50 || isFullSnapshot || priceChanged || volChanged || coin.isNewListing) {
        toWrite.push(coin);
      }
    }

    if (toWrite.length > 0) {
      const rows = toWrite.map(c => ({
        market: c.market,
        name: c.name,
        first_seen: c.isNewListing ? new Date().toISOString() : undefined,
        price_at_first_seen: c.isNewListing ? c.lastPrice : undefined,
        price_change_5m: c.priceChange5m,
        price_change_1h: c.priceChange1h,
        price_change_24h: c.priceChange24h,
        volume_24h: c.volume24h,
        volume_1h: c.volume1h,
        buy_ratio: Math.round(c.buyRatio),
        last_price: c.lastPrice,
        high_24h: c.high24h,
        low_24h: c.low24h,
        mc: c.mc,
        score: c.score,
        session: c.session,
        is_suspicious: c.isSuspicious
      }));

      const { error } = await supabase.from('coinex_scan_logs').insert(rows);
      if (error) console.error('Supabase write error:', error.message);
    }

    state.scanCount++;
    state.lastScanTime = new Date().toISOString();
    state.lastScanTimestamp = Date.now();
    state.winners = processed.slice(0, 100);

    if (state.cycleCount % 10 === 0) await checkStorage();

    console.log(`Scan #${state.scanCount} | ${processed.length} pairs | ${toWrite.length} written | Session: ${session}`);

  } catch (err) {
    console.error('Background scan error:', err.message);
  }
}

// ─── START SCANNER ─────────────────────────────────────────────────────────
backgroundScan();
setInterval(backgroundScan, SCAN_INTERVAL_MS);

// ═══════════════════════════════════════════════════════════════════════════
// API ROUTES
// ═══════════════════════════════════════════════════════════════════════════

app.get('/api/health', async (req, res) => {
  const storagePct = await checkStorage();
  res.json({
    status: 'ok',
    scanCount: state.scanCount,
    lastScanTime: state.lastScanTime,
    session: getCurrentSession(),
    storageRows: state.storageRows,
    storagePct: storagePct.toFixed(1),
    storageWarn: state.storageWarn,
    supabaseConnected: !!SUPABASE_URL,
    groqConnected: !!GROQ_API_KEY,
    uptime: process.uptime()
  });
});

app.get('/api/bgstate', (req, res) => {
  res.json({
    scanCount: state.scanCount,
    lastScanTime: state.lastScanTime,
    winnersCount: state.winners.length,
    fingerprint: state.fingerprint,
    pumpTimeLog: state.pumpTimeLog,
    peakQuiet: getPeakQuietStatus(),
    session: getCurrentSession(),
    newListings: state.newListings.slice(-10),
    timingMultiplier: getTimingMultiplier()
  });
});

app.get('/api/coinex/ticker', async (req, res) => {
  try {
    const data = await coinexGet('/spot/ticker');
    res.json(data);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/coinex/market', async (req, res) => {
  try {
    const data = await coinexGet('/spot/market');
    res.json(data);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/coinex/kline', async (req, res) => {
  try {
    const { market, period = '1min', limit = 100 } = req.query;
    if (!market) return res.status(400).json({ error: 'market required' });
    const data = await coinexGet('/spot/kline', { market, price_type: 'last', period, limit });
    res.json(data);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/coinex/depth', async (req, res) => {
  try {
    const { market, limit = 20 } = req.query;
    if (!market) return res.status(400).json({ error: 'market required' });
    const data = await coinexGet('/spot/depth', { market, limit });
    res.json(data);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/scanner/results', (req, res) => {
  res.json({ winners: state.winners, session: getCurrentSession() });
});

app.get('/api/history/window', async (req, res) => {
  try {
    const { start, end } = req.query;
    let q = supabase.from('coinex_scan_logs').select('*').order('timestamp', { ascending: false }).limit(1000);
    if (start) q = q.gte('timestamp', start);
    if (end) q = q.lte('timestamp', end);
    const { data, error } = await q;
    if (error) throw new Error(error.message);
    res.json({ data });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/history/summary', async (req, res) => {
  try {
    const { start, end } = req.query;
    let q = supabase.from('coinex_scan_logs').select('*');
    if (start) q = q.gte('timestamp', start);
    if (end) q = q.lte('timestamp', end);
    const { data, error } = await q;
    if (error) throw new Error(error.message);

    const coins = {};
    data.forEach(row => {
      if (!coins[row.name]) coins[row.name] = { rows: [], name: row.name, market: row.market };
      coins[row.name].rows.push(row);
    });

    const summaries = Object.values(coins).map(c => {
      const scores = c.rows.map(r => r.score || 0);
      const gains = c.rows.map(r => r.price_change_24h || 0);
      const vols = c.rows.map(r => r.volume_24h || 0);
      return {
        name: c.name,
        market: c.market,
        avgScore: scores.reduce((a, b) => a + b, 0) / scores.length,
        maxGain: Math.max(...gains),
        minGain: Math.min(...gains),
        maxVolume: Math.max(...vols),
        avgVolume: vols.reduce((a, b) => a + b, 0) / vols.length,
        scanCount: c.rows.length,
        sessions: [...new Set(c.rows.map(r => r.session))],
        suspicious: c.rows.some(r => r.is_suspicious)
      };
    });

    summaries.sort((a, b) => b.maxGain - a.maxGain);

    const narrativeSummary = {};
    NARRATIVES.forEach(n => {
      const matches = summaries.filter(s => s.name.toUpperCase().includes(n));
      if (matches.length > 0) {
        narrativeSummary[n] = {
          count: matches.length,
          avgGain: matches.reduce((a, b) => a + b.maxGain, 0) / matches.length
        };
      }
    });

    const sessionBreakdown = { Asia: 0, Europe: 0, US: 0 };
    data.forEach(r => { if (r.session) sessionBreakdown[r.session] = (sessionBreakdown[r.session] || 0) + 1; });

    const pumps = summaries.filter(s => s.maxGain > 20);
    const pumpProfile = pumps.length > 0 ? {
      avgMaxGain: pumps.reduce((a, b) => a + b.maxGain, 0) / pumps.length,
      avgVolume: pumps.reduce((a, b) => a + b.avgVolume, 0) / pumps.length,
      avgScore: pumps.reduce((a, b) => a + b.avgScore, 0) / pumps.length,
      count: pumps.length
    } : null;

    const topGainers = [...summaries].sort((a, b) => b.maxGain - a.maxGain).slice(0, 10);
    const topLosers = [...summaries].sort((a, b) => a.minGain - b.minGain).slice(0, 10);
    const highestVolume = [...summaries].sort((a, b) => b.maxVolume - a.maxVolume).slice(0, 10);

    res.json({
      totalRecords: data.length,
      uniqueCoins: summaries.length,
      pumps: pumps.length,
      suspicious: summaries.filter(s => s.suspicious).length,
      topGainers,
      topLosers,
      highestVolume,
      narratives: narrativeSummary,
      sessionBreakdown,
      pumpProfile
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/history/heatmap', async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('coinex_scan_logs')
      .select('timestamp, score')
      .gte('score', 60);
    if (error) throw new Error(error.message);
    const heatmap = Array(24).fill(0);
    data.forEach(r => {
      const h = new Date(r.timestamp).getUTCHours();
      heatmap[h]++;
    });
    res.json({ heatmap });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/history/bestwindow', async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('coinex_scan_logs')
      .select('timestamp, score, price_change_24h')
      .gte('score', 60);
    if (error) throw new Error(error.message);
    const hours = Array.from({ length: 24 }, (_, i) => ({ hour: i, pumps: 0, avgGain: 0, total: 0 }));
    data.forEach(r => {
      const h = new Date(r.timestamp).getUTCHours();
      hours[h].pumps++;
      hours[h].avgGain += r.price_change_24h || 0;
      hours[h].total++;
    });
    hours.forEach(h => { if (h.total > 0) h.avgGain = h.avgGain / h.total; });
    res.json({ hours });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/history/token', async (req, res) => {
  try {
    const { name } = req.query;
    if (!name) return res.status(400).json({ error: 'name required' });
    const { data, error } = await supabase
      .from('coinex_scan_logs')
      .select('*')
      .ilike('name', name)
      .order('timestamp', { ascending: true })
      .limit(500);
    if (error) throw new Error(error.message);
    res.json({ data });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/history/lowmc', async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('coinex_scan_logs')
      .select('*')
      .lt('mc', 5000000)
      .gte('score', 55)
      .order('timestamp', { ascending: false })
      .limit(500);
    if (error) throw new Error(error.message);

    const pumps = data.filter(r => r.price_change_24h > 20);
    const profile = pumps.length > 0 ? {
      count: pumps.length,
      avgMC: pumps.reduce((a, b) => a + (b.mc || 0), 0) / pumps.length,
      avgVolume: pumps.reduce((a, b) => a + (b.volume_24h || 0), 0) / pumps.length,
      avgScore: pumps.reduce((a, b) => a + (b.score || 0), 0) / pumps.length,
      avgBuyRatio: pumps.reduce((a, b) => a + (b.buy_ratio || 0), 0) / pumps.length,
      sessionBreakdown: {
        Asia: pumps.filter(r => r.session === 'Asia').length,
        Europe: pumps.filter(r => r.session === 'Europe').length,
        US: pumps.filter(r => r.session === 'US').length
      },
      topCoins: pumps.slice(0, 10).map(r => ({ name: r.name, gain: r.price_change_24h, session: r.session }))
    } : null;

    res.json({ profile, totalLowMC: data.length });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/history/sessions', async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('coinex_scan_logs')
      .select('session, score, price_change_24h, name')
      .gte('score', 50);
    if (error) throw new Error(error.message);
    const sessions = { Asia: [], Europe: [], US: [] };
    data.forEach(r => { if (r.session && sessions[r.session]) sessions[r.session].push(r); });
    const breakdown = {};
    Object.entries(sessions).forEach(([s, rows]) => {
      breakdown[s] = {
        count: rows.length,
        avgScore: rows.length ? rows.reduce((a, b) => a + (b.score || 0), 0) / rows.length : 0,
        avgGain: rows.length ? rows.reduce((a, b) => a + (b.price_change_24h || 0), 0) / rows.length : 0,
        pumps: rows.filter(r => r.price_change_24h > 20).length
      };
    });
    res.json({ breakdown });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/history/storage', async (req, res) => {
  try {
    const pct = await checkStorage();
    const estimatedBytes = state.storageRows * BYTES_PER_ROW;
    res.json({
      rows: state.storageRows,
      estimatedMB: (estimatedBytes / 1024 / 1024).toFixed(2),
      pct: pct.toFixed(1),
      warn: state.storageWarn,
      freeTierMB: 500
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/ai', async (req, res) => {
  try {
    const { mode, coin, windowData, messages, marketContext } = req.body;

    let systemPrompt = '';
    let userContent = '';
    let temperature = 0.3;
    let max_tokens = 150;

    if (mode === 'coin') {
      systemPrompt = `You are an expert CoinEx spot trader specializing in low MC momentum plays and volume breakouts.
Analyze the coin data provided and respond with EXACTLY 3 lines:
Line 1: Strongest signal
Line 2: Biggest risk
Line 3: Verdict — one of: BUY SMALL / WATCH / AVOID / TRAP
No other text.`;
      userContent = `Coin: ${coin?.name} (${coin?.market})
Price change 24h: ${coin?.priceChange24h?.toFixed(2)}%
Price change 1h: ${coin?.priceChange1h?.toFixed(2)}%
Volume 24h: $${coin?.volume24h?.toLocaleString()}
Volume surge: ${coin?.volAccel?.toFixed(2)}x
Buy ratio: ${coin?.buyRatio}%
Score: ${coin?.score}/100
Near 24h high: ${coin?.nearHigh ? 'Yes' : 'No'}
Near 24h low: ${coin?.nearLow ? 'Yes' : 'No'}
Session: ${getCurrentSession()}
Signal tags: ${(coin?.tags || []).join(', ')}`;
      temperature = 0.3;
      max_tokens = 150;

    } else if (mode === 'window') {
      systemPrompt = `You are a CoinEx market analyst. Analyze the time window data and provide a plain English summary. Cover: what narratives dominated, what conditions preceded the best moves, what session was most active, one specific actionable recommendation for next time this window approaches.`;
      userContent = JSON.stringify(windowData || {});
      temperature = 0.4;
      max_tokens = 300;

    } else if (mode === 'chat') {
      systemPrompt = `You are a conversational CoinEx trading expert. You have access to live market data. Be concise, specific, and actionable. Current context: ${JSON.stringify(marketContext || {})}`;
      temperature = 0.5;
      max_tokens = 400;
    }

    const groqMessages = mode === 'chat'
      ? (messages || [])
      : [{ role: 'user', content: userContent }];

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 20000);

    const groqRes = await fetch('https://api.groq.com/openai/v1/chat/completions', {
      method: 'POST',
      signal: controller.signal,
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${GROQ_API_KEY}`
      },
      body: JSON.stringify({
        model: 'llama-3.3-70b-versatile',
        messages: [{ role: 'system', content: systemPrompt }, ...groqMessages],
        temperature,
        max_tokens
      })
    });

    clearTimeout(timer);
    const groqData = await groqRes.json();
    const text = groqData.choices?.[0]?.message?.content || 'No response';
    res.json({ text });

  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

app.listen(PORT, () => {
  console.log(`CoinEx Alpha Engine running on port ${PORT}`);
  console.log(`Session: ${getCurrentSession()}`);
});
