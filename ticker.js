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

// --- HTTP SERVER (Keep Render Alive & Web View & REST Proxy) ---
const server = http.createServer((req, res) => {
    const parsedUrl = urlParser.parse(req.url, true);

    if (parsedUrl.pathname === '/ping') {
        res.writeHead(200, { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' });
        res.end('PONG');
        return;
    }

    // --- KLINE PROXY (Bypass 403 on App) ---
    if (parsedUrl.pathname === '/fapi/v1/klines') {
        const target = 'https://fapi.binance.com' + req.url;
        const headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Cache-Control': 'no-cache'
        };

        axios.get(target, { headers, timeout: 5000 })
            .then(response => {
                res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
                res.end(JSON.stringify(response.data));
            })
            .catch(err => {
                const status = err.response ? err.response.status : 500;
                console.error(`[PROXY ERROR] Kline fetch failed (${status}):`, err.message);
                res.writeHead(status);
                res.end(`Error: ${err.message}`);
            });
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
                body { background: #000; color: #0f8; font-family: monospace; padding: 20px; }
                .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); gap: 10px; }
                .item { background: #111; padding: 15px; border: 1px solid #222; border-radius: 6px; }
                .val { font-weight: bold; font-size: 1.3em; color: #fff; margin-top: 5px; }
                .sym { color: #888; font-size: 0.9em; letter-spacing: 1px; }
                h1 { border-bottom: 1px solid #222; padding-bottom: 10px; color: #fff; }
            </style>
        </head>
        <body>
            <h1>📡 TICKER STATION LIVE (FRANKFURT)</h1>
            <div id="status">CONNECTING...</div>
            <div id="g" class="grid"></div>
            <script>
                const g = document.getElementById('g');
                const s = document.getElementById('status');

                const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
                const ws = new WebSocket(protocol + '//' + window.location.host);

                ws.onopen = () => {
                    s.innerText = 'CONNECTED - REQUESTING FEED';
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
                ws.onclose = () => s.innerText = 'DISCONNECTED';
                ws.onerror = (e) => s.innerText = 'WS ERROR: ' + e.message;
            </script>
        </body>
        </html>
    `);
});

// --- WEBSOCKET SERVER (For App/Web) ---
const wss = new WebSocket.Server({ server });
let tickerCache = {};
let engineActive = false;

// --- BINANCE TICKER ENGINE (Subscribes to EVERYTHING) ---
const startTickerEngine = () => {
    if (engineActive) return;
    engineActive = true;

    console.log('>>> [TICKER STATION] Connecting to Binance Market Stream (Global)...');
    const url = 'wss://fstream.binance.com/market/ws/!miniTicker@arr';
    const ws = new WebSocket(url);

    ws.on('open', () => {
        console.log('>>> [TICKER STATION] Binance Stream Connected. Caching all market data.');
        ws.pingTimer = setInterval(() => {
            if(ws.readyState === WebSocket.OPEN) ws.ping();
        }, 30000);
    });

    ws.on('message', (data) => {
        try {
            const arr = JSON.parse(data);
            if (!Array.isArray(arr)) return;
            arr.forEach(item => {
                if (item.s.endsWith('USDT')) {
                    tickerCache[normalize(item.s)] = { p: item.c, v: item.q, c: "0" };
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

    // BROADCAST LOOP: 10 Seconds (As requested to reduce noise)
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
