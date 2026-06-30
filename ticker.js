const WebSocket = require('ws');
const http = require('http');
const axios = require('axios');
const urlParser = require('url');

/**
 * TICKER STATION (June 2026 - Optimized)
 * Standalone server for fast Binance Ticker updates + REST Proxy.
 * Frankfurt-based to bypass US geo-restrictions.
 */

const port = process.env.PORT || 10001;
const normalize = (s) => s.toUpperCase();

// The specific 100 coins available in the app to reduce server load
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

// --- PROXY CACHE & CONCURRENCY CONTROL ---
const proxyCache = new Map();
const inflightRequests = new Map();
const CACHE_TTL = 15000; // 15 seconds cache

// Cleanup cache periodically
setInterval(() => {
    const now = Date.now();
    for (const [key, entry] of proxyCache.entries()) {
        if (now - entry.timestamp > CACHE_TTL * 2) {
            proxyCache.delete(key);
        }
    }
}, 60000);

// --- HTTP SERVER ---
const server = http.createServer((req, res) => {
    const parsedUrl = urlParser.parse(req.url, true);

    if (parsedUrl.pathname === '/ping') {
        res.writeHead(200, { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' });
        res.end('PONG');
        return;
    }

    // --- FULL BINANCE PROXY (Bypass 403 on App with Caching) ---
    if (parsedUrl.pathname.startsWith('/fapi/v1/')) {
        const cacheKey = req.url;
        const now = Date.now();

        // 1. Check Cache
        if (proxyCache.has(cacheKey)) {
            const entry = proxyCache.get(cacheKey);
            if (now - entry.timestamp < CACHE_TTL) {
                res.writeHead(200, {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'X-Proxy-Cache': 'HIT'
                });
                res.end(entry.data);
                return;
            }
        }

        // 2. Check In-flight Requests (Coalescing)
        if (inflightRequests.has(cacheKey)) {
            inflightRequests.get(cacheKey).then(data => {
                res.writeHead(200, {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'X-Proxy-Cache': 'COALESCED'
                });
                res.end(data);
            }).catch(err => {
                res.writeHead(err.status || 500);
                res.end(`Error: ${err.message}`);
            });
            return;
        }

        // 3. Perform Fresh Request with Mirror Rotation
        const mirrors = [
            'https://fapi.binance.com',
            'https://fapi.binance.me',
            'https://fapi.binance.info',
            'https://fapi.binancezh.me'
        ];

        const executeProxy = async () => {
            let lastError = null;
            for (const mirror of mirrors) {
                try {
                    const target = mirror + req.url;
                    const headers = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                        'Accept': 'application/json',
                        'Cache-Control': 'no-cache'
                    };

                    const response = await axios.get(target, { headers, timeout: 8000 });
                    const data = JSON.stringify(response.data);

                    proxyCache.set(cacheKey, { data, timestamp: Date.now() });
                    return data;
                } catch (err) {
                    lastError = err;
                    const status = err.response ? err.response.status : 500;
                    console.error(`[PROXY ERROR] ${mirror}${parsedUrl.pathname} failed (${status}):`, err.message);

                    // Only rotate on rate limits or server errors
                    if (status !== 429 && status !== 418 && status < 500) {
                        throw { status, message: err.message };
                    }
                    console.log(`>>> [PROXY] Rotating mirror for ${parsedUrl.pathname}...`);
                }
            }
            throw { status: lastError.response ? lastError.response.status : 503, message: lastError.message };
        };

        const requestPromise = executeProxy();
        inflightRequests.set(cacheKey, requestPromise);

        requestPromise
            .then(data => {
                res.writeHead(200, {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'X-Proxy-Cache': 'MISS'
                });
                res.end(data);
            })
            .catch(err => {
                res.writeHead(err.status || 500);
                res.end(`Error: ${err.message}`);
            })
            .finally(() => {
                inflightRequests.delete(cacheKey);
            });

        return;
    }

    // Web Dashboard (Simplified)
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(`<h1>📡 TICKER STATION LIVE</h1><p>Frankfurt Proxy Active with Caching & Mirror Rotation</p>`);
});

// --- WEBSOCKET SERVER (For App/Web) ---
const wss = new WebSocket.Server({ server });
let tickerCache = {};
let engineActive = false;

// --- BINANCE TICKER ENGINE ---
const startTickerEngine = () => {
    if (engineActive) return;
    engineActive = true;

    console.log('>>> [TICKER STATION] Connecting to Binance Market Stream...');
    const url = 'wss://fstream.binance.com/market/ws/!miniTicker@arr';
    const ws = new WebSocket(url);

    ws.on('open', () => {
        console.log('>>> [TICKER STATION] Binance Stream Connected.');
        ws.pingTimer = setInterval(() => { if(ws.readyState === WebSocket.OPEN) ws.ping(); }, 30000);
    });

    ws.on('message', (data) => {
        try {
            const arr = JSON.parse(data);
            if (!Array.isArray(arr)) return;
            arr.forEach(item => {
                const sym = normalize(item.s);
                if (APP_COINS.has(sym)) {
                    tickerCache[sym] = { p: item.c, v: item.q, c: "0" };
                }
            });
        } catch (e) {}
    });

    ws.on('close', () => {
        console.log('--- [TICKER STATION] Binance Lost. Reconnecting... ---');
        clearInterval(ws.pingTimer);
        engineActive = false;
        setTimeout(startTickerEngine, 5000);
    });
};

wss.on('connection', (ws) => {
    ws.subscribedTickers = new Set();
    ws.on('message', (msg) => {
        try {
            const j = JSON.parse(msg);
            if (j.op === 'subscribe_tickers') {
                j.args.forEach(s => ws.subscribedTickers.add(s.toUpperCase()));
            }
        } catch (e) {}
    });

    const sendTimer = setInterval(() => {
        if (ws.readyState !== WebSocket.OPEN) return;
        const filteredData = {};
        ws.subscribedTickers.forEach(sym => { if (tickerCache[sym]) filteredData[sym] = tickerCache[sym]; });
        if (Object.keys(filteredData).length > 0) ws.send(JSON.stringify({ type: 'tickers', data: filteredData }));
    }, 10000);

    ws.on('close', () => clearInterval(sendTimer));
});

server.listen(port, () => {
    console.log(`Ticker Station LIVE on ${port}`);
    startTickerEngine();
});
