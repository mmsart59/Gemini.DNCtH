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

// --- HTTP SERVER (Keep Render Alive & Web View & REST Proxy) ---
const server = http.createServer((req, res) => {
    const parsedUrl = urlParser.parse(req.url, true);
    console.log(`[REQUEST] ${req.method} ${req.url}`);

    if (parsedUrl.pathname === '/ping') {
        res.writeHead(200, { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' });
        res.end('PONG');
        return;
    }

    // --- CATCH-ALL PROXY FOR BINANCE ---
    if (parsedUrl.pathname.includes('/fapi/') || parsedUrl.pathname.includes('/klines')) {
        const fapiIndex = req.url.indexOf('/fapi/');
        const path = fapiIndex !== -1 ? req.url.substring(fapiIndex) : req.url;

        // Comprehensive list of Binance Futures mirrors for high availability
        const binanceMirrors = [
            'https://fapi.binance.com',
            'https://fapi.binance.me',
            'https://fapi.binance.info',
            'https://fapi.binancezh.me',
            'https://fapi1.binance.com',
            'https://fapi2.binance.com',
            'https://fapi3.binance.com'
        ];

        const tryProxy = (index) => {
            if (index >= binanceMirrors.length) {
                res.writeHead(502, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
                res.end(JSON.stringify({
                    error: 'All Binance mirrors failed',
                    status: 502,
                    path: path
                }));
                return;
            }

            const target = binanceMirrors[index] + path;
            console.log(`[PROXY ATTEMPT ${index}] -> ${target}`);

            axios.get(target, {
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'application/json',
                    'Cache-Control': 'no-cache'
                },
                timeout: 10000 // Slightly longer timeout for deep mirror attempts
            })
            .then(response => {
                res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
                res.end(JSON.stringify(response.data));
            })
            .catch(err => {
                const status = err.response ? err.response.status : 500;
                console.warn(`[PROXY FAILED] Mirror ${binanceMirrors[index]} failed with ${status}: ${err.message}`);

                // If it's a 403, 418, 429 or 451 (Geo), try the next mirror immediately
                if ([403, 418, 429, 451, 500, 502, 503, 504].includes(status)) {
                    tryProxy(index + 1);
                } else {
                    // For other errors (like 400 Bad Request), return the error JSON
                    res.writeHead(status, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
                    res.end(JSON.stringify({ error: err.message, status }));
                }
            });
        };

        tryProxy(0);
        return;
    }

    // Web Dashboard for verification
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(`
        <!DOCTYPE html>
        <html>
        <head>
            <title>TICKER STATION (FRANKFURT)</title>
            <style>
                body { background: #000; color: #fff; font-family: 'JetBrains Mono', monospace; padding: 20px; }
                .container { background: rgba(20,20,20,0.7); backdrop-filter: blur(10px); -webkit-backdrop-filter: blur(10px); border: 1px solid rgba(255,255,255,0.1); border-radius: 12px; padding: 20px; box-shadow: 0 8px 32px rgba(0,0,0,0.5); }
                .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); gap: 12px; }
                .item { background: rgba(255,255,255,0.03); padding: 15px; border: 1px solid rgba(255,255,255,0.05); border-radius: 8px; transition: all 0.3s ease; }
                .item:hover { background: rgba(255,255,255,0.07); border-color: rgba(255,255,255,0.1); transform: translateY(-2px); }
                .val { font-weight: 800; font-size: 1.3em; color: #fff; margin-top: 5px; }
                .sym { color: #666; font-size: 0.9em; letter-spacing: 1px; font-weight: bold; }
                h1 { font-weight: 800; border-bottom: 1px solid rgba(255,255,255,0.1); padding-bottom: 15px; margin-top: 0; display: flex; justify-content: space-between; align-items: center; }
                #status { font-size: 10px; color: #45F7B9; text-transform: uppercase; letter-spacing: 2px; margin-bottom: 20px; }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>📡 TICKER STATION <span style="font-size: 10px; color: #444;">V2.0 PRO</span></h1>
                <div id="status">INITIALIZING...</div>
                <div id="g" class="grid"></div>
            </div>
            <script>
                const g = document.getElementById('g');
                const s = document.getElementById('status');

                const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
                const ws = new WebSocket(protocol + '//' + window.location.host);

                ws.onopen = () => {
                    s.innerText = 'CONNECTED - ENCRYPTED TUNNEL ACTIVE';
                    ws.send(JSON.stringify({
                        op: 'subscribe_tickers',
                        args: ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'XRPUSDT', 'ADAUSDT', 'DOGEUSDT', 'TRXUSDT', 'DOTUSDT', 'MATICUSDT']
                    }));
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
                ws.onclose = () => { s.innerText = 'DISCONNECTED'; s.style.color = '#F83A7A'; };
            </script>
        </body>
        </html>
    `);
});

// --- WEBSOCKET SERVER (For App/Web) ---
const wss = new WebSocket.Server({ server });
let tickerCache = {};
let engineActive = false;

// --- BINANCE TICKER ENGINE ---
const startTickerEngine = () => {
    if (engineActive) return;
    engineActive = true;

    console.log('>>> [TICKER STATION] Connecting to Binance Market Stream (Global)...');

    // Switch to !ticker@arr for 24h change % + individual streams for Funding/OI
    const url = 'wss://fstream.binance.com/ws/!ticker@arr';
    const ws = new WebSocket(url);

    ws.on('open', () => {
        console.log('>>> [TICKER STATION] Binance Ticker Connected.');
        ws.pingTimer = setInterval(() => {
            if(ws.readyState === WebSocket.OPEN) ws.ping();
        }, 30000);

        // Subscribe to Mark Price (Funding) and Open Interest for top coins
        startIndicatorEngine();
    });

    ws.on('message', (data) => {
        try {
            const arr = JSON.parse(data);
            if (!Array.isArray(arr)) return;
            arr.forEach(item => {
                const sym = normalize(item.s);
                if (APP_COINS.has(sym)) {
                    tickerCache[sym] = {
                        p: item.c, // Last Price
                        v: item.q, // Quote Volume
                        c: item.P, // 24h Price Change Percent
                        r: tickerCache[sym]?.r || 0, // Preserved from Indicator Engine
                        o: tickerCache[sym]?.o || 0
                    };
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
                if (msg.data.e === 'markPriceUpdate') {
                    if (!tickerCache[sym]) tickerCache[sym] = { p: "0", v: "0", c: "0", r: 0, o: 0 };
                    tickerCache[sym].r = parseFloat(msg.data.r);
                } else if (msg.data.e === 'openInterestUpdate') {
                    if (!tickerCache[sym]) tickerCache[sym] = { p: "0", v: "0", c: "0", r: 0, o: 0 };
                    tickerCache[sym].o = parseFloat(msg.data.o);
                }
            } catch (e) {}
        });

        ws.on('close', () => {
            // No auto-reconnect here to save resources; let startTickerEngine re-trigger
        });
    }
};

// --- CLIENT MANAGEMENT (Smart On-Demand Subscriptions) ---
wss.on('connection', (ws) => {
    ws.subscribedTickers = new Set();
    console.log(`[TICKER STATION] New Client Connected.`);

    ws.on('message', (msg) => {
        try {
            const j = JSON.parse(msg);
            if (j.op === 'subscribe_tickers') {
                console.log(`[TICKER STATION] App requested: ${j.args.length} coins`);
                j.args.forEach(s => ws.subscribedTickers.add(s.toUpperCase()));
            }
        } catch (e) {}
    });

    // BROADCAST LOOP: 10 Seconds
    const sendTimer = setInterval(() => {
        if (ws.readyState !== WebSocket.OPEN) return;

        const filteredData = {};
        ws.subscribedTickers.forEach(sym => {
            if (tickerCache[sym]) filteredData[sym] = tickerCache[sym];
        });

        if (Object.keys(filteredData).length > 0) {
            ws.send(JSON.stringify({ type: 'tickers', data: filteredData }));
        }
    }, 10000);

    ws.on('close', () => {
        clearInterval(sendTimer);
        console.log(`[TICKER STATION] Client disconnected.`);
    });
});

server.listen(port, () => {
    console.log(`Ticker Station LIVE on ${port}`);
    startTickerEngine();
});
