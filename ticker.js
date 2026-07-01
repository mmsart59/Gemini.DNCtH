const WebSocket = require('ws');
const http = require('http');
const https = require('https');
const axios = require('axios');
const urlParser = require('url');

/**
 * TICKER STATION (June 2026 - Ultra Optimized)
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
const inflightRequests = new Map();
const CACHE_TTL = 15000;

// --- TICKER ENGINE ---
let tickerCache = {};
let lastSentCache = new Map(); // For Delta-Based Updates (Option 2)
let engineActive = false;

// --- INDICATOR ENGINE (Option 3: Server-Side RSI/EMA) ---
const indicators = {};
const klineHistory = {}; // Keep 60 klines per app coin

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
    console.log('>>> [INDICATORS] Refreshing Server-Side Analytics...');
    for (const sym of APP_COINS) {
        try {
            const url = `https://fapi.binance.com/fapi/v1/klines?symbol=${sym}&interval=1h&limit=60`;
            const res = await axios.get(url, { httpsAgent, timeout: 5000 });
            const closes = res.data.map(k => parseFloat(k[4]));
            const rsi = calculateRSI(closes);
            const ema = calculateEMA(closes);
            const price = closes[closes.length - 1];

            // Conviction Logic (Pre-calculated for coloring)
            let conv = 0;
            if (rsi > 70 || rsi < 30) conv++;
            if ((price > ema && rsi > 50) || (price < ema && rsi < 50)) conv++;

            indicators[sym] = {
                r: Math.round(rsi),
                e: price > ema ? 1 : 0, // 1 for above, 0 for below
                cv: conv
            };
            await new Promise(r => setTimeout(r, 100)); // Stagger to avoid 429
        } catch (e) {}
    }
};
setInterval(updateIndicators, 300000); // Update indicators every 5 mins

// --- BINANCE STREAM ---
const startTickerEngine = () => {
    if (engineActive) return;
    engineActive = true;
    const ws = new WebSocket('wss://fstream.binance.com/market/ws/!miniTicker@arr');
    ws.on('open', () => {
        console.log('>>> [TICKER STATION] Stream Connected.');
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

                    // Option 2: Precision Truncation (Data Saver)
                    const p = rawPrice >= 1000 ? rawPrice.toFixed(2) : rawPrice.toPrecision(6);
                    const ind = indicators[sym] || { r: 50, e: 0, cv: 0 };
                    tickerCache[sym] = { p, v: item.q, c: change, r: ind.r, e: ind.e, cv: ind.cv };
                }
            });
        } catch (e) {}
    });
    ws.on('close', () => { engineActive = false; setTimeout(startTickerEngine, 5000); });
};

// --- HTTP SERVER ---
const server = http.createServer((req, res) => {
    const parsedUrl = urlParser.parse(req.url, true);
    if (parsedUrl.pathname === '/ping') return res.end('PONG');

    if (parsedUrl.pathname.startsWith('/fapi/v1/')) {
        const cacheKey = req.url;
        if (proxyCache.has(cacheKey)) {
            const entry = proxyCache.get(cacheKey);
            if (Date.now() - entry.timestamp < CACHE_TTL) {
                res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
                return res.end(entry.data);
            }
        }
        // ... standard mirror rotation logic ...
        res.end("Use WebSocket for tickers.");
    }
});

// --- CLIENT MGMT ---
const wss = new WebSocket.Server({ server });
wss.on('connection', (ws) => {
    ws.subscribedTickers = new Set();
    ws.isBackground = false; // Option 1
    ws.lastSent = new Map();

    ws.on('message', (msg) => {
        try {
            const j = JSON.parse(msg);
            if (j.op === 'subscribe_tickers') {
                j.args.forEach(s => ws.subscribedTickers.add(s.toUpperCase()));
            } else if (j.op === 'set_background') {
                ws.isBackground = !!j.value;
            }
        } catch (e) {}
    });

    const sendTimer = setInterval(() => {
        if (ws.readyState !== WebSocket.OPEN) return;

        // Option 1: Stop in background, except for alarms (controlled by app's selective subscription)
        if (ws.isBackground && ws.subscribedTickers.size > 10) return;

        const deltaData = {};
        ws.subscribedTickers.forEach(sym => {
            const data = tickerCache[sym];
            if (!data) return;

            // Option 2: Delta-Based Updates (Only send if price or indicator changed)
            const last = ws.lastSent.get(sym);
            if (!last || last.p !== data.p || last.r !== data.r) {
                deltaData[sym] = data;
                ws.lastSent.set(sym, data);
            }
        });

        if (Object.keys(deltaData).length > 0) {
            ws.send(JSON.stringify({ type: 'tickers', data: deltaData }));
        }
    }, 10000);

    ws.on('close', () => clearInterval(sendTimer));
});

server.listen(port, '0.0.0.0', () => {
    console.log(`Ticker Station LIVE on ${port} (0.0.0.0)`);
    startTickerEngine();
    // Start indicators AFTER the server is already listening to satisfy Render's port scan
    setTimeout(updateIndicators, 5000);
});
