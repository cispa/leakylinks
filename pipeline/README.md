# Pipeline Module

This module contains the live analysis pipeline workers that process URLs through multiple phases.

For detailed pipeline architecture, phases, and usage instructions, see the [main README](../README.md).

## Module Structure

- `pipeline_controller.py`: Main controller for running the pipeline
- `run_pipeline.py`: CLI entry point for running individual phases
- `crawl_worker.py`: Live crawl worker (Phase 1)
- `url_token_check_worker.py`: Token detection worker (Phase 2)
- `page_difference_check_worker.py`: Page difference checker (Phase 3)
- `screenshot_analysis_worker.py`: Screenshot analyzer (Phase 4)
- `db.py`: Database interface and task tracking
- `utils.py`: Utility functions

## Quick Reference

Run phases using:
```bash
python pipeline/pipeline/run_pipeline.py --crawl
python pipeline/pipeline/run_pipeline.py --url_token_check
python pipeline/pipeline/run_pipeline.py --page_difference_check
python pipeline/pipeline/run_pipeline.py --spi_detector
```
