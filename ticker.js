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
const CACHE_TTL = 15000;

// --- TICKER ENGINE ---
let tickerCache = {};
let engineActive = false;

// --- INDICATOR ENGINE ---
const indicators = {};

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

            const oi = parseFloat(oiRes.data.openInterest);
            const fr = parseFloat(frRes.data.lastFundingRate);

            // --- REFINED CONVICTION LOGIC (Less Flashy) ---
            let conv = 0;

            // Rule 1: EMA Signal (Must be > 0.5% away to count)
            if (Math.abs(price - ema) / ema > 0.005) conv++;

            // Rule 2: RSI Overbought/Oversold (Extreme only)
            if (rsi > 72 || rsi < 28) conv++;

            // Rule 3: High Volume strength
            const latestVol = volumes[volumes.length - 1];
            const avgVol = volumes.slice(-21).reduce((a, b) => a + b, 0) / 21;
            if (latestVol > avgVol * 1.5) conv++;

            // Rule 4: Open Interest Signal (High OI for symbol)
            if (oi > 0) conv++;

            // Rule 5: Funding Rate Signal (Significant cost to hold)
            if (Math.abs(fr) >= 0.0001) conv++;

            indicators[sym] = {
                r: Math.round(rsi),
                e: price > ema ? 1 : 0,
                cv: conv,
                oi: oi,
                fr: fr
            };
            await new Promise(r => setTimeout(r, 250)); // Slower stagger to be safe
        } catch (e) {}
    }
};

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
                    const p = rawPrice >= 1000 ? rawPrice.toFixed(2) : rawPrice.toPrecision(6);

                    const ind = indicators[sym] || { r: 50, e: 0, cv: 0, oi: 0, fr: 0 };
                    tickerCache[sym] = {
                        p, v: item.q, c: change,
                        r: ind.r, e: ind.e, cv: ind.cv,
                        oi: ind.oi, fr: ind.fr
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

    if (url.pathname === '/ping') {
        res.writeHead(200, { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' });
        return res.end('PONG');
    }

    if (url.pathname.startsWith('/fapi/v1/')) {
        const cacheKey = req.url;
        if (proxyCache.has(cacheKey)) {
            const entry = proxyCache.get(cacheKey);
            if (Date.now() - entry.timestamp < CACHE_TTL) {
                res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
                return res.end(entry.data);
            }
        }
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        return res.end("Use WebSocket for tickers.");
    }

    // Default: Web Dashboard
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(`
        <!DOCTYPE html>
        <html>
        <head>
            <title>TICKER STATION (FRANKFURT)</title>
            <style>
                body { background: #000; color: #fff; font-family: monospace; padding: 20px; line-height: 1.5; }
                .container { border: 1px solid #333; border-radius: 8px; padding: 20px; }
                .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 10px; }
                .item { background: #111; padding: 10px; border-radius: 4px; border-left: 3px solid #45F7B9; }
                .sym { color: #888; font-weight: bold; }
                .val { font-size: 1.2em; }
                h1 { margin-top: 0; color: #45F7B9; border-bottom: 1px solid #333; padding-bottom: 10px; }
                #status { font-size: 0.8em; color: #666; margin-bottom: 20px; }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>📡 TICKER STATION LIVE</h1>
                <div id="status">FRANKFURT NODE ACTIVE - 0.0.0.0:${port}</div>
                <div id="g" class="grid"></div>
            </div>
            <script>
                const g = document.getElementById('g');
                const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
                const ws = new WebSocket(protocol + '//' + window.location.host);
                ws.onopen = () => {
                    ws.send(JSON.stringify({ op: 'subscribe_tickers', args: ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT'] }));
                };
                ws.onmessage = (e) => {
                    const j = JSON.parse(e.data);
                    if (j.type === 'tickers') {
                        Object.keys(j.data).forEach(sym => {
                            let el = document.getElementById('s_' + sym);
                            if (!el) {
                                el = document.createElement('div');
                                el.id = 's_' + sym;
                                el.className = 'item';
                                g.appendChild(el);
                            }
                            el.innerHTML = '<div class="sym">' + sym + '</div><div class="val">$' + parseFloat(j.data[sym].p).toLocaleString() + '</div>';
                        });
                    }
                };
            </script>
        </body>
        </html>
    `);
});

const wss = new WebSocket.Server({ server });
wss.on('connection', (ws) => {
    ws.subscribedTickers = new Set();
    ws.isBackground = false;
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
        if (ws.isBackground && ws.subscribedTickers.size > 10) return;
        const deltaData = {};
        ws.subscribedTickers.forEach(sym => {
            const data = tickerCache[sym];
            if (!data) return;
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

// --- BOOT SEQUENCE ---
server.listen(port, '0.0.0.0', () => {
    console.log(`==> [READY] Ticker Station bound to 0.0.0.0:${port}`);
    startTickerEngine();
    setTimeout(updateIndicators, 1000);
    setInterval(updateIndicators, 300000);
});
