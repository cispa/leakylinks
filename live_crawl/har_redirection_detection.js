const fs = require('fs');
const { URL } = require('url');

const ALLOWED_3XX = new Set([300, 301, 302, 303, 307, 308]);

class HarRedirectionDetector {
    hasScheme(u) {
        return typeof u === 'string' && (u.startsWith('http://') || u.startsWith('https://'));
    }

    ensureScheme(u) {
        return this.hasScheme(u) ? u : `https://${u}`;
    }

    normalizeUrl(u) {
        if (!u) return u;
        try {
            const url = new URL(u);
            const port = url.port ? `:${url.port}` : '';
            const hash = url.hash || '';
            return `${url.protocol.toLowerCase()}//${url.hostname.toLowerCase()}${port}${url.pathname || '/'}${url.search || ''}${hash}`;
        } catch {
            return u;
        }
    }

    hostOf(u) {
        try {
            const url = new URL(u);
            return url.hostname.toLowerCase();
        } catch {
            return '';
        }
    }

    urlsEqual(a, b) {
        return this.normalizeUrl(a) === this.normalizeUrl(b);
    }

    urlsEqualIgnoreScheme(a, b) {
        if (!a || !b) return false;
        
        try {
            const urlA = new URL(this.ensureScheme(a));
            const urlB = new URL(this.ensureScheme(b));
            const portA = urlA.port ? `:${urlA.port}` : '';
            const portB = urlB.port ? `:${urlB.port}` : '';
            const keyA = `${urlA.hostname.toLowerCase()}${portA}${urlA.pathname || '/'}${urlA.search || ''}${urlA.hash || ''}`;
            const keyB = `${urlB.hostname.toLowerCase()}${portB}${urlB.pathname || '/'}${urlB.search || ''}${urlB.hash || ''}`;
            return keyA === keyB;
        } catch {
            return this.urlsEqual(a, b);
        }
    }

    getHeader(headers, name) {
        const lname = name.toLowerCase();
        for (const h of headers || []) {
            if (h && typeof h === 'object' && (h.name || '').toLowerCase() === lname) {
                return h.value;
            }
        }
        return null;
    }

    robustLoad(harPath) {
        try {
            const content = fs.readFileSync(harPath, 'utf-8');
            return [JSON.parse(content), null];
        } catch (e1) {
            try {
                const content = fs.readFileSync(harPath, 'utf-8');
                const start = content.indexOf('{');
                const end = content.lastIndexOf('}');
                if (start !== -1 && end > start) {
                    return [JSON.parse(content.substring(start, end + 1)), null];
                }
            } catch {
            }
            return [null, `JSONDecodeError: ${e1.message}`];
        }
    }

    isTopLevelNav(entry) {
        if (entry?._resourceType === 'document') {
            return true;
        }
        const req = entry?.request || {};
        const hdrs = req.headers || [];
        const dest = (this.getHeader(hdrs, 'Sec-Fetch-Dest') || '').toLowerCase();
        const mode = (this.getHeader(hdrs, 'Sec-Fetch-Mode') || '').toLowerCase();
        const user = (this.getHeader(hdrs, 'Sec-Fetch-User') || '').toLowerCase();
        const upg = this.getHeader(hdrs, 'Upgrade-Insecure-Requests') || '';
        return dest === 'document' || mode === 'navigate' || user === '?1' || upg === '1';
    }

    isHttpToHttpsOnly(a, b) {
        try {
            const urlA = new URL(a);
            const urlB = new URL(b);
            
            if (urlA.protocol.toLowerCase() !== 'http:' || urlB.protocol.toLowerCase() !== 'https:') {
                return false;
            }
            
            if (urlA.hostname.toLowerCase() !== urlB.hostname.toLowerCase()) {
                return false;
            }
            
            const aHasExplicitPort = /:[0-9]+/.test(a);
            const bHasExplicitPort = /:[0-9]+/.test(b);
            if (aHasExplicitPort !== bHasExplicitPort) {
                return false;
            }
            
            const portA = urlA.port ? parseInt(urlA.port, 10) : 80;
            const portB = urlB.port ? parseInt(urlB.port, 10) : 443;
            if (portA !== 80 || portB !== 443) {
                return false;
            }
            
            if ((urlA.pathname || '/') !== (urlB.pathname || '/')) {
                return false;
            }
            if ((urlA.search || '') !== (urlB.search || '')) {
                return false;
            }
            
            if ((urlA.hash || '') !== (urlB.hash || '')) {
                return false;
            }
            
            return true;
        } catch {
            return false;
        }
    }

    findEntryByUrl(entries, targetUrl) {
        for (const e of entries) {
            if (this.isTopLevelNav(e)) {
                const req = e.request || {};
                const reqUrl = req.url || '';
                if (this.urlsEqual(reqUrl, targetUrl)) {
                    return e;
                }
            }
        }
        for (const e of entries) {
            const req = e.request || {};
            const reqUrl = req.url || '';
            if (this.urlsEqual(reqUrl, targetUrl)) {
                return e;
            }
        }
        
        for (const e of entries) {
            if (this.isTopLevelNav(e)) {
                const req = e.request || {};
                const reqUrl = req.url || '';
                if (this.urlsEqualIgnoreScheme(reqUrl, targetUrl)) {
                    return e;
                }
            }
        }
        for (const e of entries) {
            const req = e.request || {};
            const reqUrl = req.url || '';
            if (this.urlsEqualIgnoreScheme(reqUrl, targetUrl)) {
                return e;
            }
        }
        return null;
    }

    nextRedirectTarget(resp, currentUrl) {
        let loc = resp.redirectURL || resp.redirectUrl;
        if (!loc) {
            const hdrs = resp.headers || [];
            loc = this.getHeader(hdrs, 'Location') || this.getHeader(hdrs, 'Content-Location');
        }
        if (!loc) return null;
        try {
            return new URL(loc, currentUrl).href;
        } catch {
            return null;
        }
    }

    hasRedirect(harPath, initiatingUrl) {
        if (!harPath || !fs.existsSync(harPath)) {
            return false;
        }

        const [data, err] = this.robustLoad(harPath);
        if (err || !data) {
            return false;
        }

        const entries = data.log?.entries || [];
        if (!entries || entries.length === 0) {
            return false;
        }

        const startUrl = this.ensureScheme(initiatingUrl);

        const nav = this.findEntryByUrl(entries, startUrl);
        if (!nav) {
            return false;
        }

        let currentEntry = nav;
        const seen = new Set();

        for (let i = 0; i < 20; i++) {
            const resp = currentEntry.response || {};
            const status = resp.status;
            if (typeof status !== 'number' || !ALLOWED_3XX.has(status)) {
                break;
            }

            const curUrl = (currentEntry.request || {}).url || '';
            const target = this.nextRedirectTarget(resp, curUrl);
            if (!target) {
                break;
            }

            const tnorm = this.normalizeUrl(target);
            if (seen.has(tnorm)) {
                break;
            }
            seen.add(tnorm);

            if (this.urlsEqual(curUrl, target) || this.isHttpToHttpsOnly(curUrl, target)) {
                const nxt = this.findEntryByUrl(entries, target);
                if (!nxt) {
                    break;
                }
                currentEntry = nxt;
                continue;
            }

            return true;
        }

        return false;
    }
}

function hasRedirect(harPath, initiatingUrl) {
    const detector = new HarRedirectionDetector();
    return detector.hasRedirect(harPath, initiatingUrl);
}

module.exports = { HarRedirectionDetector, hasRedirect };

