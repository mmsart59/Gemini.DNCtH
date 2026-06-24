const WebSocket = require('ws');
const http = require('http');

/**
 * TICKER STATION (June 2026)
 * Standalone server for fast Binance Ticker updates.
 * Separation ensures liquidation engine stability.
 */

const port = process.env.PORT || 10001;
const normalize = (s) => s.replace('USDT', '').toUpperCase();

// --- HTTP SERVER (Keep Render Alive) ---
const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('TICKER STATION LIVE');
});

// --- WEBSOCKET SERVER (For App/Web) ---
const wss = new WebSocket.Server({ server });
let clients = new Set();
let tickerCache = {}; // Normalized cache: { BTC: { p, v, c } }
let engineActive = false;

const broadcast = (payload) => {
    const data = JSON.stringify(payload);
    let sentCount = 0;
    clients.forEach(c => {
        if (c.readyState === WebSocket.OPEN) {
            c.send(data);
            sentCount++;
        }
    });
    return sentCount;
};

// --- BINANCE TICKER ENGINE (2026 Routed Market) ---
const startTickerEngine = () => {
    if (engineActive) return;
    engineActive = true;

    console.log('>>> [TICKER STATION] Connecting to Binance Market Stream...');
    const url = 'wss://fstream.binance.com/market/ws/!miniTicker@arr';
    const ws = new WebSocket(url);

    ws.on('open', () => {
        console.log('>>> [TICKER STATION] Binance Stream Connected. Receiving live prices.');
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
                    tickerCache[normalize(item.s)] = {
                        p: item.c, // Last Price
                        v: item.q, // 24h Volume (Quote)
                        c: "0"     // Change (MiniTicker doesn't have percent change)
                    };
                }
            });
        } catch (e) {
            console.error('[TICKER ERROR] Message parsing failed:', e.message);
        }
    });

    ws.on('close', () => {
        console.log('--- [TICKER STATION] Connection lost. Reconnecting in 5s... ---');
        clearInterval(ws.pingTimer);
        engineActive = false;
        setTimeout(startTickerEngine, 5000);
    });

    ws.on('error', (e) => console.error('[TICKER ERROR] Socket error:', e.message));
};

// --- CLIENT MANAGEMENT ---
wss.on('connection', (ws) => {
    clients.add(ws);
    console.log(`[TICKER STATION] New Client Connected. Total Clients: ${clients.size}`);

    if (Object.keys(tickerCache).length > 0) {
        ws.send(JSON.stringify({ type: 'tickers', data: tickerCache }));
    }

    ws.on('close', () => {
        clients.delete(ws);
        console.log(`[TICKER STATION] Client Disconnected. Remaining: ${clients.size}`);
    });
});

// --- BROADCAST LOOP (Every 2 seconds) ---
setInterval(() => {
    const tickerCount = Object.keys(tickerCache).length;
    if (clients.size > 0 && tickerCount > 0) {
        const sentTo = broadcast({ type: 'tickers', data: tickerCache });
        console.log(`[TICKER STATION] Broadcasted ${tickerCount} prices to ${sentTo} clients`);
    } else if (clients.size > 0 && tickerCount === 0) {
        console.log('[TICKER STATION] Warning: Clients connected but no ticker data available from Binance');
    }
}, 2000);

server.listen(port, () => {
    console.log(`Ticker Station LIVE on ${port}`);
    startTickerEngine();
});
