const WebSocket = require('ws');
const http = require('http');

// This script acts as your Palace Server
const port = process.env.PORT || 3000;
const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end('<h1>Palace is Running</h1>');
});

const wss = new WebSocket.Server({ server });

wss.on('connection', (client) => {
    console.log('App/Browser connected to Palace');
    
    // 1. Connect to Binance (Using the dstream mirror you wanted)
    // We set a real User-Agent to avoid being flagged as a bot
    const binance = new WebSocket('wss://dstream.binance.me/ws/!forceOrder@arr', {
        headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' }
    });

    binance.on('message', (data) => {
        const d = JSON.parse(data);
        if (d.e === "forceOrder") {
            const payload = {
                source: 'Binance',
                symbol: d.o.s,
                side: d.o.S === 'BUY' ? 'short' : 'long',
                value: Math.round(d.o.q * d.o.p),
                price: d.o.p
            };
            // Send to your App or Browser
            if (client.readyState === WebSocket.OPEN) {
                client.send(JSON.stringify(payload));
            }
        }
    });

    binance.on('error', (e) => console.log('Binance Error:', e.message));
    
    client.on('close', () => binance.close());
});

server.listen(port, () => console.log(`Palace LIVE on port ${port}`));
