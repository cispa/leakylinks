# Honeylink Experiment

Overview
Identify who monitors the public live feeds of URL‑scanning services and what they fetch once our controlled “honeylinks” appear there.

How it works
- Publish deterministic pages (“honeylinks”) with synthetic content under `honey/site/`.
- Ensure those URLs surface in public feeds of services.
- Front the site with NGINX → Caddy and log full request metadata (timestamps, UA, referrer, full URI).
- Analyze logs to attribute hits to feed watchers versus the services themselves and characterize behavior.

Usage
- Host `site/` behind your reverse proxy with access logging enabled.
- Make the URLs appear in service public feeds ; wait for feed‑polling traffic.
- Export logs and run `analysis/parse-logs.py` to segment and summarize behavior.

Key scripts
- `analysis/parse-logs.py`: Parse and segment access logs; identify services and watchers.
- `analysis/run_ipinfo.py`: Optional IP enrichment during analysis.

What’s included
- `site/`: Static pages and assets (forms, links, assets, canary values) to reveal crawler behavior.
- `site/base/`: Canonical honey page used for consistent submissions.