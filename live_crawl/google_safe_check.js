const { chromium } = require("playwright");

process.on('unhandledRejection', (reason) => {
    console.log(JSON.stringify({ error: String(reason) }));
    process.exit(1);
});
process.on('uncaughtException', (err) => {
    console.log(JSON.stringify({ error: String(err) }));
    process.exit(1);
});

async function checkTransparency(targetUrl) {
    const encodedUrl = encodeURIComponent(new URL(targetUrl).hostname);
    const transparencyUrl = `https://transparencyreport.google.com/safe-browsing/search?url=${encodedUrl}&hl=en`;

    const browser = await chromium.launch({ headless: true });
    const context = await browser.newContext();
    const page = await context.newPage();

    let status = "unknown";
    try {
        await page.goto(transparencyUrl, { waitUntil: 'networkidle' });
        const pageContent = await page.content();

        if (pageContent.includes('This site is unsafe')) {
            status = "unsafe";
        } else if (pageContent.includes('No unsafe content found')) {
            status = "safe";
        }
    } catch (err) {
        console.error(JSON.stringify({ error: err.message }));
    } finally {
        await browser.close();
    }

    console.log(JSON.stringify({ url: targetUrl, status }));
}

const urlArgIndex = process.argv.findIndex(arg => arg === '--url');
const targetUrl = urlArgIndex !== -1 ? process.argv[urlArgIndex + 1] : null;

if (!targetUrl) {
    console.error(JSON.stringify({ error: "Usage: node google_safe_check.js --url <url>" }));
    process.exit(1);
}

checkTransparency(targetUrl);

