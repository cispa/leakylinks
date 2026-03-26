# URL Token Checker

This module analyzes URLs to detect high-entropy tokens that may indicate session identifiers or other sensitive parameters.

## Purpose

The URL Token Checker:
- Parses URLs including full path and query parameters
- Calculates entropy for each token/parameter
- Flags URLs containing high-entropy tokens as potentially sensitive
- Filters out common patterns (UUIDs, timestamps, etc.)

## How It Works

1. Extracts all path segments and query parameters from the URL
2. Applies basic sanity checks (length, character composition)
3. Calculates Shannon entropy for each token
4. Compares entropy against thresholds to identify session-like tokens

## Usage

This module is called by the `URLTokenCheckWorker` in the pipeline (Phase 2):

```bash
python pipeline/pipeline/run_pipeline.py --url_token_check
```

URLs with detected tokens are flagged in the `analysis_output` table with `finalurlbefore_has_token = True`.

