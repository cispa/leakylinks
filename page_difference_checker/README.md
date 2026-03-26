# Page Difference Checker

This module compares before/after HTML page snapshots to detect session state differences.

## Purpose

The Page Difference Checker analyzes URLs that don't have tokens in the URL itself but may still leak sensitive information through page content changes.

## How It Works

1. Takes two HTML snapshots: "before" (normal visit) and "after" (session dropped)
2. Extracts and normalizes text content from both pages
3. Calculates similarity using text comparison algorithms
4. Flags pages with significant differences as potentially sensitive

## Usage

This module is called by the `PageDifferenceCheckWorker` in the pipeline (Phase 3):

```bash
python pipeline/pipeline/run_pipeline.py --page_difference_check
```

**Note:** This phase only processes URLs where `finalurlbefore_has_token = False` (no tokens detected in URL).

Pages with significant differences are flagged in the `analysis_output` table with `page_different = True`.

