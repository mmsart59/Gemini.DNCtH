const WebSocket = require('ws');
const http = require('http');
const axios = require('axios');
const urlParser = require('url');

/**
 * TICKER STATION (June 2026 - Optimized)
 * Standalone server for fast Binance Ticker updates + REST Proxy.
 * Simplified for 2026: Single reliable endpoint, no mirrors.
 */

const port = process.env.PORT || 10001;
const TARGET_BINANCE = 'https://fapi.binance.com';
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

// --- HTTP SERVER (Keep Render Alive & REST Proxy) ---
const server = http.createServer((req, res) => {
    const parsedUrl = urlParser.parse(req.url, true);
    console.log(`[REQUEST] ${req.method} ${req.url}`);

    if (parsedUrl.pathname === '/ping') {
        res.writeHead(200, { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' });
        res.end('PONG');
        return;
    }

    // --- SIMPLE PROXY FOR BINANCE ---
    if (parsedUrl.pathname.includes('/fapi/') || parsedUrl.pathname.includes('/klines')) {
        const path = req.url.includes('/fapi/') ? req.url.substring(req.url.indexOf('/fapi/')) : req.url;
        const target = TARGET_BINANCE + path;

        axios.get(target, {
            headers: { 'User-Agent': 'Mozilla/5.0' },
            timeout: 5000
        })
        .then(response => {
            res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
            res.end(JSON.stringify(response.data));
        })
        .catch(err => {
            const status = err.response ? err.response.status : 500;
            res.writeHead(status, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
            res.end(JSON.stringify({ error: err.message, status }));
        });
        return;
    }

    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('TICKER STATION ACTIVE');
});

// --- WEBSOCKET SERVER (For App/Web) ---
const wss = new WebSocket.Server({ server });
let tickerCache = {};
let engineActive = false;

// --- BINANCE TICKER ENGINE ---
const startTickerEngine = () => {
    if (engineActive) return;
    engineActive = true;

    const url = 'wss://fstream.binance.com/ws/!ticker@arr';
    const ws = new WebSocket(url);

    ws.on('open', () => {
        console.log('>>> [TICKER STATION] Binance Ticker Connected.');
        ws.pingTimer = setInterval(() => {
            if(ws.readyState === WebSocket.OPEN) ws.ping();
        }, 30000);
        startIndicatorEngine();
    });

    ws.on('message', (data) => {
        try {
            const arr = JSON.parse(data);
            if (!Array.isArray(arr)) return;
            arr.forEach(item => {
                const sym = normalize(item.s);
                if (APP_COINS.has(sym)) {
                    if (!tickerCache[sym]) {
                        tickerCache[sym] = { p: "0", v: "0", c: "0", r: 0, o: 0 };
                    }
                    tickerCache[sym].p = item.c; // Last Price
                    tickerCache[sym].v = item.q; // Quote Volume
                    tickerCache[sym].c = item.P; // 24h Price Change Percent
                }
            });
        } catch (e) {}
    });

    ws.on('close', () => {
        console.log('--- [TICKER STATION] Ticker Lost. Reconnecting... ---');
        clearInterval(ws.pingTimer);
        engineActive = false;
        setTimeout(startTickerEngine, 5000);
    });
};

const startIndicatorEngine = () => {
    const symbols = Array.from(APP_COINS);
    const CHUNK_SIZE = 20;
    for (let i = 0; i < symbols.length; i += CHUNK_SIZE) {
        const chunk = symbols.slice(i, i + CHUNK_SIZE);
        const chunkStreams = [];
        chunk.forEach(s => {
            chunkStreams.push(`${s.toLowerCase()}@markPrice`);
            chunkStreams.push(`${s.toLowerCase()}@openInterest`);
        });

        const url = `wss://fstream.binance.com/stream?streams=${chunkStreams.join('/')}`;
        const ws = new WebSocket(url);

        ws.on('message', (data) => {
            try {
                const msg = JSON.parse(data);
                if (!msg.data) return;
                const sym = normalize(msg.data.s);
                if (!tickerCache[sym]) tickerCache[sym] = { p: "0", v: "0", c: "0", r: 0, o: 0 };

                if (msg.data.e === 'markPriceUpdate') {
                    tickerCache[sym].r = parseFloat(msg.data.r); // Funding Rate
                } else if (msg.data.e === 'openInterestUpdate') {
                    tickerCache[sym].o = parseFloat(msg.data.o); // Open Interest
                }
            } catch (e) {}
        });

        ws.on('close', () => {
            // Reconnect handled by startTickerEngine re-triggering this
        });
    }
};

// --- CLIENT MANAGEMENT ---
wss.on('connection', (ws) => {
    ws.subscribedTickers = new Set();
    console.log(`[TICKER STATION] New Client Connected.`);

    ws.on('message', (msg) => {
        try {
            const j = JSON.parse(msg);
            if (j.op === 'subscribe_tickers') {
                j.args.forEach(s => ws.subscribedTickers.add(s.toUpperCase()));
            }
        } catch (e) {}
    });

    // Increased frequency to 2 seconds for fresh data
    const sendTimer = setInterval(() => {
        if (ws.readyState !== WebSocket.OPEN) return;

        const filteredData = {};
        ws.subscribedTickers.forEach(sym => {
            if (tickerCache[sym]) filteredData[sym] = tickerCache[sym];
        });

        if (Object.keys(filteredData).length > 0) {
            ws.send(JSON.stringify({ type: 'tickers', data: filteredData }));
        }
    }, 2000);

    ws.on('close', () => {
        clearInterval(sendTimer);
    });
});

server.listen(port, () => {
    console.log(`Ticker Station LIVE on ${port}`);
    startTickerEngine();
});
