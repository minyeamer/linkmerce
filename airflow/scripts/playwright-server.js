/**
 * Playwright WebSocket server that launches Chromium with headless=false
 * and AutomationControlled disabled to bypass bot detection.
 *
 * Requires:
 *   - DISPLAY env var (e.g. Xvfb :99)
 *   - `playwright` npm package installed in working directory
 *
 * Python client connects as usual:
 *   playwright.chromium.connect("ws://playwright:3000")
 */
const http = require('http');
const { chromium } = require('playwright');

const WS_PORT  = parseInt(process.env.PLAYWRIGHT_PORT  || '3000', 10);
const HEALTH_PORT = WS_PORT + 1;  // 3001 for healthcheck

(async () => {
    console.log(`Starting Playwright server (headless=false) on port ${WS_PORT}...`);

    const server = await chromium.launchServer({
        headless: false,
        port: WS_PORT,
        host: '0.0.0.0',
        wsPath: '/',          // ws://host:3000/ — same path as run-server default
        args: [
            '--disable-blink-features=AutomationControlled',
            '--disable-infobars',
            '--no-sandbox',
            '--disable-dev-shm-usage',
            '--disable-setuid-sandbox',
        ],
    });

    console.log(`Playwright WS server ready: ${server.wsEndpoint()}`);

    // Minimal HTTP health endpoint (used by docker-compose healthcheck)
    http.createServer((req, res) => {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('OK\n');
    }).listen(HEALTH_PORT, '0.0.0.0', () => {
        console.log(`Health endpoint: http://0.0.0.0:${HEALTH_PORT}/health`);
    });

    const shutdown = () => server.close().then(() => process.exit(0));
    process.on('SIGTERM', shutdown);
    process.on('SIGINT',  shutdown);
})().catch(err => {
    console.error('Failed to start server:', err.message);
    process.exit(1);
});
