# SCRAPERS

The scraper is the block that monitors the live feed endpoints for example: `urlscan.io/json/live`, `hybrids-analysis.com/feed?json`, `radar.cloudflare.com/scan` to extract the response body and store it in a PostgreSQL database.

Each scraper has its unique way of monitoring the live feed and appending the new URLs to the database, and some would need an api key set in .env