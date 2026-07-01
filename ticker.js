const WebSocket = require('ws');
const http = require('http');
const https = require('https');
const axios = require('axios');
const urlParser = require('url');

/**
 * TICKER STATION (June 2026 - Ultra Optimized V3)
 * Standalone server for fast Binance Ticker updates + REST Proxy.
 * Frankfurt-based to bypass US geo-restrictions.
 */

const port = process.env.PORT || 10001;
const normalize = (s) => s.toUpperCase();

// --- HTTPS AGENT ---
const httpsAgent = new https.Agent({ rejectUnauthorized: false, keepAlive: true });

const APP_COINS = new Set([
    "BTCUSDT", "ETHUSDT", "DOTUSDT", "HBARUSDT", "XRPUSDT", "LINKUSDT", "ARBUSDT",
    "BNBUSDT", "SOLUSDT", "ADAUSDT", "DOGEUSDT", "TRXUSDT", "AVAXUSDT", "MATICUSDT",
    "SHIBUSDT", "LTCUSDT", "UNIUSDT", "BCHUSDT", "ICPUSDT", "ETCUSDT", "NEARUSDT",
    "ATOMUSDT", "OPUSDT", "XLMUSDT", "FILUSDT", "INJUSDT", "IMXUSDT", "APTUSDT",
    "CROUSDT", "LDOUSDT", "VETUSDT", "MKRUSDT", "GRTUSDT", "RNDRUSDT", "SUIUSDT",
    "AAVEUSDT", "ALGOUSDT", "EGLDUSDT", "AXSUSDT", "SANDUSDT", "MANAUSDT", "FTMUSDT",
    "THETAUSDT", "XTZUSDT", "SNXUSDT", "NEOUSDT", "FLOWUSDT", "KAVAUSDT", "MINAUSDT",
    "GALAUSDT", "APEUSDT", "DYDXUSDT", "LUNA2USDT", "EOSUSDT", "TWTUSDT", "ZILUSDT",
    "CRVUSDT", "GMTUSDT", "1INCHUSDT", "COMPUSDT", "STXUSDT", "XMRUSDT", "RUNEUSDT",
    "KLAYUSDT", "ARUSDT", "FETUSDT", "PAXGUSDT", "WLDUSDT", "WAVESUSDT", "ZECUSDT",
    "CAKEUSDT", "SEIUSDT", "GMXUSDT", "FXSUSDT", "DASHUSDT", "ENSUSDT", "PEPEUSDT",
    "CFXUSDT", "MASKUSDT", "ROSEUSDT", "LRCUSDT", "CVXUSDT", "WOOUSDT", "CELOUSDT",
    "IOTXUSDT", "FLOKIUSDT", "AGIXUSDT", "KSMUSDT", "CHZUSDT", "OCEANUSDT", "SUSHIUSDT",
    "BATUSDT", "BANDUSDT", "QTUMUSDT", "ANKRUSDT", "IOTAUSDT", "ENJUSDT", "YFIUSDT",
    "ONEUSDT", "STORJUSDT"
]);

// --- PROXY CACHE ---
const proxyCache = new Map();
const CACHE_TTL = 15000;

// --- TICKER ENGINE ---
let tickerCache = {};
let engineActive = false;

// --- METRICS STATE (Option 2 & 3: Persistence for Trend Calculation) ---
const indicators = {};
const previousMetrics = {}; // Stores last known OI/FR for trend detection

const calculateRSI = (closes, period = 14) => {
    if (closes.length <= period) return 50;
    let gains = 0, losses = 0;
    for (let i = 1; i <= period; i++) {
        const diff = closes[closes.length - i] - closes[closes.length - i - 1];
        if (diff > 0) gains += diff; else losses -= diff;
    }
    const rs = (gains / period) / (losses / period || 1);
    return 100 - (100 / (1 + rs));
};

const calculateEMA = (closes, period = 20) => {
    if (closes.length < period) return closes[closes.length - 1];
    const k = 2 / (period + 1);
    let ema = closes.slice(0, period).reduce((a, b) => a + b, 0) / period;
    for (let i = period; i < closes.length; i++) {
        ema = closes[i] * k + ema * (1 - k);
    }
    return ema;
};

const updateIndicators = async () => {
    console.log('>>> [METRICS] Refreshing Market Analytics...');
    for (const sym of APP_COINS) {
        try {
            const [kRes, oiRes, frRes] = await Promise.all([
                axios.get(`https://fapi.binance.com/fapi/v1/klines?symbol=${sym}&interval=1h&limit=60`, { httpsAgent, timeout: 5000 }),
                axios.get(`https://fapi.binance.com/fapi/v1/openInterest?symbol=${sym}`, { httpsAgent, timeout: 5000 }),
                axios.get(`https://fapi.binance.com/fapi/v1/premiumIndex?symbol=${sym}`, { httpsAgent, timeout: 5000 })
            ]);

            const closes = kRes.data.map(k => parseFloat(k[4]));
            const volumes = kRes.data.map(k => parseFloat(k[5]));
            const rsi = calculateRSI(closes);
            const ema = calculateEMA(closes);
            const price = closes[closes.length - 1];

            const rawOI = parseFloat(oiRes.data.openInterest);
            const rawFR = parseFloat(frRes.data.lastFundingRate);

            // --- OPTION 2: OI TREND CALCULATION ---
            const prev = previousMetrics[sym] || { oi: rawOI, fr: rawFR };
            // 1 = Rising, -1 = Falling, 0 = Flat
            let oiTrend = 0;
            if (rawOI > prev.oi * 1.001) oiTrend = 1; // 0.1% threshold for noise
            else if (rawOI < prev.oi * 0.999) oiTrend = -1;

            // --- OPTION 3: FUNDING SURGE DETECTION ---
            let frSurge = 0;
            const frDiffPct = prev.fr !== 0 ? Math.abs((rawFR - prev.fr) / prev.fr) : 0;
            // Rule: >50% change AND current > 0.005%
            if (frDiffPct > 0.50 && Math.abs(rawFR) > 0.00005) {
                frSurge = 1;
            }

            // Update previous for next cycle
            previousMetrics[sym] = { oi: rawOI, fr: rawFR };

            // --- REFINED CONVICTION LOGIC ---
            let conv = 0;
            if (Math.abs(price - ema) / ema > 0.005) conv++;
            if (rsi > 72 || rsi < 28) conv++;
            const latestVol = volumes[volumes.length - 1];
            const avgVol = volumes.slice(-21).reduce((a, b) => a + b, 0) / 21;
            if (latestVol > avgVol * 1.5) conv++;
            if (oiTrend === 1) conv++;
            if (Math.abs(rawFR) >= 0.0001) conv++;

            indicators[sym] = {
                r: Math.round(rsi),
                e: price > ema ? 1 : 0,
                cv: conv,
                oi: rawOI,
                oit: oiTrend, // OI Trend flag
                fr: rawFR,
                frs: frSurge, // FR Surge flag
                v21: avgVol   // 21-period average volume
            };
            await new Promise(r => setTimeout(r, 200));
        } catch (e) {}
    }
};

// --- BINANCE STREAM ---
const startTickerEngine = () => {
    if (engineActive) return;
    engineActive = true;
    const ws = new WebSocket('wss://fstream.binance.com/market/ws/!miniTicker@arr');
    ws.on('open', () => {
        console.log('>>> [TICKER STATION] Market Stream Connected.');
        ws.pingTimer = setInterval(() => ws.readyState === WebSocket.OPEN && ws.ping(), 30000);
    });
    ws.on('message', (data) => {
        try {
            const arr = JSON.parse(data);
            arr.forEach(item => {
                const sym = normalize(item.s);
                if (APP_COINS.has(sym)) {
                    const rawPrice = parseFloat(item.c);
                    const openPrice = parseFloat(item.o);
                    const change = openPrice !== 0 ? ((rawPrice - openPrice) / openPrice * 100).toFixed(2) : "0.00";
                    const p = rawPrice >= 1000 ? rawPrice.toFixed(2) : rawPrice.toPrecision(6);

                    const ind = indicators[sym] || { r: 50, e: 0, cv: 0, oi: 0, oit: 0, fr: 0, frs: 0, v21: 0 };

                    // --- OPTION 1: VOLUME RATIO CALCULATION ---
                    const volRatio = ind.v21 > 0 ? (parseFloat(item.q) / ind.v21).toFixed(2) : "1.00";

                    tickerCache[sym] = {
                        p, v: item.q, vr: volRatio, c: change,
                        r: ind.r, e: ind.e, cv: ind.cv,
                        oi: ind.oi, oit: ind.oit,
                        fr: ind.fr, frs: ind.frs
                    };
                }
            });
        } catch (e) {}
    });
    ws.on('close', () => { engineActive = false; setTimeout(startTickerEngine, 5000); });
};

// --- HTTP SERVER ---
const server = http.createServer((req, res) => {
    const url = new URL(req.url, `http://${req.headers.host}`);
    if (url.pathname === '/ping') return res.end('PONG');
    if (url.pathname.startsWith('/fapi/v1/')) {
        const cacheKey = req.url;
        if (proxyCache.has(cacheKey)) {
            const entry = proxyCache.get(cacheKey);
            if (Date.now() - entry.timestamp < CACHE_TTL) {
                res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
                return res.end(entry.data);
            }
        }
        res.end("Use WebSocket Tunnel.");
    }
    // Dashboard
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(`<h1>📡 TICKER STATION LIVE</h1><p>Frankfurt Optimization Active</p>`);
});

const wss = new WebSocket.Server({ server });
wss.on('connection', (ws) => {
    ws.subscribedTickers = new Set();
    ws.lastSent = new Map();
    ws.on('message', (msg) => {
        try {
            const j = JSON.parse(msg);
            if (j.op === 'subscribe_tickers') j.args.forEach(s => ws.subscribedTickers.add(s.toUpperCase()));
            else if (j.op === 'set_background') ws.isBackground = !!j.value;
        } catch (e) {}
    });

    const sendTimer = setInterval(() => {
        if (ws.readyState !== WebSocket.OPEN) return;
        const deltaData = {};
        ws.subscribedTickers.forEach(sym => {
            const data = tickerCache[sym];
            if (!data) return;
            const last = ws.lastSent.get(sym);
            if (!last || last.p !== data.p || last.r !== data.r || last.vr !== data.vr) {
                deltaData[sym] = data;
                ws.lastSent.set(sym, data);
            }
        });
        if (Object.keys(deltaData).length > 0) ws.send(JSON.stringify({ type: 'tickers', data: deltaData }));
    }, 10000);
    ws.on('close', () => clearInterval(sendTimer));
});

server.listen(port, '0.0.0.0', () => {
    console.log(`==> [READY] Ticker Station LIVE`);
    startTickerEngine();
    setTimeout(updateIndicators, 1000);
    setInterval(updateIndicators, 300000);
});
