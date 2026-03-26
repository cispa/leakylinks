const { chromium } = require('playwright');
const { load } = require('cheerio');
const fs = require('fs');
const path = require('path');
const os = require('os');
const { hasRedirect } = require('./har_redirection_detection.js');
const { spawn } = require('child_process');

class LiveCrawl {
    stripTsParam(url) {
        const [baseUrl, fragment] = url.split('#');
        
        let cleanedBase = baseUrl.replace(/([?&])_ts=\d+(&)?/, (_, p1, p2) => {
            if (p2) return p1;
            return '';
        }).replace(/[?&]$/, '');
        
        return fragment ? `${cleanedBase}#${fragment}` : cleanedBase;
    }

    looksLikeCaptchaChallenge({ $, html, status, url }) {
        const s = html.toLowerCase();
        const title = ($('title').text() || '').toLowerCase();

        const hasCFChallenge =
            $('#cf-challenge, #cf-error-details, #challenge-form, .cf-turnstile, iframe[src*="challenges.cloudflare.com"]').length > 0;
        const hasReCaptchaWidget =
            $('.g-recaptcha, iframe[src*="google.com/recaptcha/"]').length > 0 &&
            $('form[action*="captcha"], input[name*="captcha"]').length > 0;

        const cfPath = /\/cdn-cgi\/(challenge|trace|bm)/i.test(url || '');
        const waitingTitle = title.includes('just a moment');
        const challengeStatus = [403, 429, 503].includes(status ?? 0);
        const hasRayId = s.includes('ray id') && (s.includes('cloudflare') || $('#cf-error-details').length > 0);

        const strong = hasCFChallenge || hasReCaptchaWidget || hasRayId;
        const context = challengeStatus || waitingTitle || cfPath;

        return strong && context;
    }

    async getResponseInfo(page, response) {
        const renderedContent = await page.content();
        const responseSize = renderedContent.length;
    
        let statusCode = null;
        const lastResponse = response || page.lastMainFrameResponse;
        if (lastResponse) {
            statusCode = lastResponse.status();
        }
    
        const $ = load(renderedContent);
        const titles = $('title').map((i, el) => $(el).text().trim()).get();
        const headings = $('h1, h2, h3').map((i, el) => $(el).text().trim()).get();
        const paragraphs = $('p').map((i, el) => $(el).text().trim()).get();
        const staticTextLength = [...titles, ...headings, ...paragraphs].reduce((sum, text) => sum + text.length, 0);
    
        return { statusCode, responseSize, staticTextLength, $, htmlContent: renderedContent };
    }
    
    async dumpStorage(page, artifactPaths, stage) {
        if (!artifactPaths) return null;
    
        const storageKey = `storage_${stage}`;
        if (!artifactPaths[storageKey]) return null;
    
        const dir = path.dirname(artifactPaths[storageKey]);
        await fs.promises.mkdir(dir, { recursive: true });
    
        try {
            const data = await page.evaluate(() => {
                const ls = {};
                const ss = {};
    
                for (let i = 0; i < localStorage.length; i++) {
                    const key = localStorage.key(i);
                    ls[key] = localStorage.getItem(key);
                }
    
                for (let i = 0; i < sessionStorage.length; i++) {
                    const key = sessionStorage.key(i);
                    ss[key] = sessionStorage.getItem(key);
                }
    
                return { localStorage: ls, sessionStorage: ss };
            });
    
            await fs.promises.writeFile(
                artifactPaths[storageKey],
                JSON.stringify(data, null, 2),
                'utf8'
            );
            return data;
        } catch (e) {
            console.error(`[WARN] Failed to dump storage: ${e.message}`);
            return null;
        }
    }
    
    async saveArtifacts(page, artifactPaths, stage) {
        if (!artifactPaths) return;

        const screenshotKey = `screenshot_${stage}`;
        const domKey = `dom_${stage}`;

        async function removeIfEmpty(dir) {
            try {
                const files = await fs.promises.readdir(dir);
                if (files.length === 0) {
                    await fs.promises.rmdir(dir);
                }
            } catch (e) {
            }
        }

        if (artifactPaths[screenshotKey]) {
            const dir = path.dirname(artifactPaths[screenshotKey]);
            await fs.promises.mkdir(dir, { recursive: true });
            try {
                await page.screenshot({ path: artifactPaths[screenshotKey], fullPage: true });
            } catch (e) {
                await removeIfEmpty(dir);
                throw e;
            }
        }

        if (artifactPaths[domKey]) {
            const dir = path.dirname(artifactPaths[domKey]);
            await fs.promises.mkdir(dir, { recursive: true });
            try {
                const content = await page.content();
                await fs.promises.writeFile(artifactPaths[domKey], content, 'utf8');
            } catch (e) {
                await removeIfEmpty(dir);
                throw e;
            }
        }
    }

    async checkGoogleTransparency(url) {
        return new Promise((resolve) => {
            const scriptPath = path.join(__dirname, 'google_safe_check.js');
            const proc = spawn('node', [scriptPath, '--url', url], {
                stdio: ['ignore', 'pipe', 'pipe']
            });

            let stdout = '';
            let stderr = '';

            proc.stdout.on('data', (data) => {
                stdout += data.toString();
            });

            proc.stderr.on('data', (data) => {
                stderr += data.toString();
            });

            proc.on('close', (code) => {
                if (code !== 0 || !stdout) {
                    resolve({ status: 'unknown', error: stderr || 'No output' });
                    return;
                }

                try {
                    const data = JSON.parse(stdout.trim().split('\n').pop());
                    resolve({ status: data.status || 'unknown', error: data.error || null });
                } catch (e) {
                    resolve({ status: 'unknown', error: `Failed to parse output: ${e.message}` });
                }
            });

            proc.on('error', (err) => {
                resolve({ status: 'unknown', error: err.message });
            });
        });
    }

    async crawl(url, artifactPaths = null) {
        const logs = [];
        const data = {
            url: url,
            before: {},
            after: {},
            redirects: {},
            googleTransparency: null,
            error: null
        };

        // Generate default HAR paths if not provided (for manual testing)
        if (!artifactPaths) {
            artifactPaths = {};
        }
        if (!artifactPaths[`har_before`]) {
            const tmpDir = path.join(os.tmpdir(), `live_crawl_${Date.now()}`);
            artifactPaths[`har_before`] = path.join(tmpDir, 'before.har');
            artifactPaths[`har_after`] = path.join(tmpDir, 'after.har');
            logs.push(`[INFO] Using default HAR paths: ${tmpDir}`);
        }

        let page;
        let contextBefore, contextAfter;
        let anchor;

        logs.push(`\n[PROCESSING] ${url}`);

        process.env.PLAYWRIGHT_TAG = `live-crawl-${Date.now()}`;

        const browser = await chromium.launch({
            headless: false,
            args: [
                '--no-sandbox',
                '--disable-gpu',
                '--disable-dev-shm-usage',
                '--disable-setuid-sandbox',
                '--no-first-run',
                '--no-zygote',
                '--ignore-certificate-errors',
                '--allow-insecure-localhost'
            ]
        });

        try {
            logs.push('[GOOGLE TRANSPARENCY] Checking...');
            const transparencyResult = await this.checkGoogleTransparency(url);
            data.googleTransparency = transparencyResult;
            logs.push(`[GOOGLE TRANSPARENCY] Status: ${transparencyResult.status}`);

            if (artifactPaths?.[`har_before`]) {
                try {
                    await fs.promises.mkdir(path.dirname(artifactPaths[`har_before`]), { recursive: true });
                } catch (e) {
                }
            }
            contextBefore = await browser.newContext({
                userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                ignoreHTTPSErrors: true,
                recordHar: artifactPaths?.[`har_before`] ? {
                    path: artifactPaths[`har_before`],
                    content: 'embed',
                    mode: 'full'
                } : undefined
            });

            page = await contextBefore.newPage();

            page.lastMainFrameResponse = null;
            page.on('response', response => {
                if (response.request().frame() === page.mainFrame() && response.request().resourceType() === 'document') {
                    page.lastMainFrameResponse = response;
                }
            });

            let response = null;
            try {
                response = await page.goto(url, {
                    waitUntil: 'domcontentloaded',
                    timeout: 30000
                });
                await page.waitForTimeout(3000);
                anchor = page.url();
            } catch (e) {
                const failureResp = page.lastMainFrameResponse;
                if (failureResp) {
                    logs.push(`[WARN] page.goto failed, but captured response: ${failureResp.status()}`);
                    response = failureResp;
                } else {
                    logs.push(`[WARN] page.goto threw: ${e.message}`);
                }
            }

            try {
                await page.goto(anchor, { waitUntil: 'domcontentloaded', timeout: 30000 });
                await page.waitForTimeout(3000);
            } catch (e) {
                logs.push(`[WARN] page.reload threw: ${e.message}`);
            }
        
            const finalUrlBefore = page.url();
            const cleanBefore = this.stripTsParam(finalUrlBefore);
            logs.push(`[FINAL URL BEFORE] ${finalUrlBefore}`);
            logs.push(`[CLEAN FINAL BEFORE] ${cleanBefore}`);

            data.before.finalUrlBefore = finalUrlBefore;
            data.before.cleanUrlBefore = cleanBefore;

            const mainResponse = page.lastMainFrameResponse || response;

            if (mainResponse) {
                const beforeData = await this.getResponseInfo(page, mainResponse);
                const beforeHtml = beforeData.htmlContent;

                const beforeCapMarkers = this.looksLikeCaptchaChallenge({
                    $: beforeData.$,
                    html: beforeHtml,
                    status: beforeData.statusCode,
                    url: finalUrlBefore || ''
                });

                data.before.statusCode = beforeData.statusCode;
                data.before.responseSize = beforeData.responseSize;
                data.before.staticTextLength = beforeData.staticTextLength;
                data.before.hasCaptchaMarkers = beforeCapMarkers;
            
                logs.push(`[STATUS] Before: ${beforeData.statusCode} | Size: ${beforeData.responseSize} | Chars: ${beforeData.staticTextLength}`);
                if (beforeCapMarkers) {
                    logs.push('[CAPTCHA MARKERS BEFORE] challenge-like signals present');
                }
            
                await this.saveArtifacts(page, artifactPaths, 'before');
                const storageBefore = await this.dumpStorage(page, artifactPaths, 'before');
                data.before.storage = storageBefore;

                await contextBefore.close();
            } else {
                logs.push('[WARN] No main frame response — possibly due to early redirect, network error, or CSP block');
                const beforeHtml = await page.content().catch(() => '<html></html>');
                const beforeData = {
                    statusCode: null,
                    responseSize: 0,
                    staticTextLength: 0,
                    $: load(beforeHtml),
                    htmlContent: beforeHtml
                };

                data.before.statusCode = null;
                data.before.responseSize = 0;
                data.before.staticTextLength = 0;
                data.before.hasCaptchaMarkers = false;

                await this.saveArtifacts(page, artifactPaths, 'before');
                const storageBefore = await this.dumpStorage(page, artifactPaths, 'before');
                data.before.storage = storageBefore;

                await contextBefore.close();
            }
            
            let hasRedirectBefore = false;
            if (artifactPaths?.[`har_before`]) {
                hasRedirectBefore = hasRedirect(artifactPaths[`har_before`], url);
                logs.push(`[HAR REDIRECT CHECK BEFORE] ${hasRedirectBefore ? 'Redirect detected' : 'No redirect detected'}`);
            }
            data.redirects.before = hasRedirectBefore;

            if (artifactPaths?.[`har_after`]) {
                try {
                    await fs.promises.mkdir(path.dirname(artifactPaths[`har_after`]), { recursive: true });
                } catch (e) {
                }
            }
            contextAfter = await browser.newContext({
                userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                ignoreHTTPSErrors: true,
                recordHar: artifactPaths?.[`har_after`] ? {
                    path: artifactPaths[`har_after`],
                    content: 'embed',
                    mode: 'full'
                } : undefined
            });

            page = await contextAfter.newPage();

            await contextAfter.clearCookies();
            await contextAfter.addInitScript(() => {
                try {
                    window.localStorage.clear();
                    window.sessionStorage.clear();
                } catch (e) {
                }
            });

            const reloadUrl = finalUrlBefore;
            logs.push(`[RELOADING] Using URL: ${reloadUrl}`);
            page.lastMainFrameResponse = null;
            let newResponse = null;
            try {
                newResponse = await page.goto(reloadUrl, {
                    waitUntil: 'domcontentloaded',
                    timeout: 30000
                });
                await page.waitForTimeout(3000);
            } catch (e) {
                const failureResp = page.lastMainFrameResponse;
                if (failureResp) {
                    logs.push(`[WARN] reload goto failed, but captured response: ${failureResp.status()}`);
                    newResponse = failureResp;
                } else {
                    logs.push(`[WARN] reload page.goto threw: ${e.message}`);
                }
            }

            const finalUrlAfter = page.url();
            const cleanAfter = this.stripTsParam(finalUrlAfter);
            logs.push(`[FINAL URL AFTER] ${finalUrlAfter}`);
            logs.push(`[CLEAN FINAL AFTER] ${cleanAfter}`);

            data.after.finalUrlAfter = finalUrlAfter;
            data.after.cleanUrlAfter = cleanAfter;

            if (newResponse) {
                const afterData = await this.getResponseInfo(page, page.lastMainFrameResponse || newResponse);
                const afterHtml = afterData.htmlContent;
                logs.push(`[STATUS] After: ${afterData.statusCode} | Size: ${afterData.responseSize} | Chars: ${afterData.staticTextLength}`);

                data.after.statusCode = afterData.statusCode;
                data.after.responseSize = afterData.responseSize;
                data.after.staticTextLength = afterData.staticTextLength;

                const capMarkers = this.looksLikeCaptchaChallenge({
                    $: afterData.$,
                    html: afterHtml,
                    status: afterData.statusCode,
                    url: finalUrlAfter || ''
                });
                data.after.hasCaptchaMarkers = capMarkers;
                if (capMarkers) {
                    logs.push('[CAPTCHA MARKERS AFTER] challenge-like signals present');
                }

                await this.saveArtifacts(page, artifactPaths, 'after');
                const storageAfter = await this.dumpStorage(page, artifactPaths, 'after');
                data.after.storage = storageAfter;
            } else {
                logs.push('[WARN] Reload failed — no response received after state clear');
                data.after.statusCode = null;
                data.after.responseSize = 0;
                data.after.staticTextLength = 0;
                data.after.hasCaptchaMarkers = false;
            }

            await contextAfter.close();

            let hasRedirectAfter = false;
            if (artifactPaths?.[`har_after`]) {
                hasRedirectAfter = hasRedirect(artifactPaths[`har_after`], reloadUrl);
                logs.push(`[HAR REDIRECT CHECK AFTER] ${hasRedirectAfter ? 'Redirect detected' : 'No redirect detected'}`);
            }
            data.redirects.after = hasRedirectAfter;

        } catch (e) {
            data.error = e.message;
            logs.push(`[ERROR] ${data.error}`);
        } finally {
            if (browser.isConnected()) {
                await browser.close();
            }
        }

        return { logs, data };
    }
}

function parseArgs() {
    const args = process.argv.slice(2);
    const argMap = {};
    for (let i = 0; i < args.length; i++) {
        if (args[i].startsWith('--')) {
            const key = args[i].replace(/^--/, '');
            if (i + 1 < args.length && !args[i + 1].startsWith('--')) {
                argMap[key] = args[i + 1];
                i++;
            } else {
                argMap[key] = true;
            }
        }
    }
    return argMap;
}

async function main() {
    const args = parseArgs();
    const url = args.url;
    const timestampStart = Date.now();

    if (!url) {
        console.error(JSON.stringify({ url: null, error: 'No URL provided' }));
        process.exit(1);
    }

    let artifactPaths = null;
    if (args.artifact_paths) {
        try {
            artifactPaths = JSON.parse(args.artifact_paths);
        } catch (e) {
            console.error("Failed to parse artifact_paths JSON:", e);
            process.exit(1);
        }
    }

    const crawler = new LiveCrawl();
    const result = await crawler.crawl(url, artifactPaths);

    result.logs.forEach(log => console.error(log));

    const output = {
        ...result.data,
        startTime: timestampStart,
        endTime: Date.now(),
        durationMs: Date.now() - timestampStart,
    };

    console.log(JSON.stringify(output, null, 2));
}

if (require.main === module) {
    main().catch(console.error);
}

module.exports = { LiveCrawl };

