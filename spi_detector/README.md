# SPI Detector (Screenshot Analyzer)

This module uses vision-based LLM analysis to detect sensitive personal information (SPI) in page screenshots.

## Purpose

The SPI Detector is the final filtering stage that analyzes screenshots to determine if a page actually contains sensitive personal information. It uses a vision-language model (VLM) to understand page content and context.

## How It Works

1. Loads screenshots from URLs flagged as potentially sensitive
2. Optionally performs OCR to extract text content
3. Sends screenshot + OCR text to vision-based LLM (Qwen-VL)
4. LLM analyzes the image following strict sensitivity criteria
5. Returns structured output with sensitivity assessment and evidence

## Features

- **OCR preprocessing**: Extracts text with Tesseract for better analysis
- **Smart filtering**: Skips non-Latin scripts, overly large images, etc.
- **Batch processing**: Processes multiple screenshots concurrently
- **Round-robin load balancing**: Distributes requests across multiple model servers
- **Checkpoint support**: Resumes from interruptions

## Usage

This module is called by the `ScreenshotAnalysisWorker` in the pipeline (Phase 4):

```bash
python pipeline/pipeline/run_pipeline.py --spi_detector
```

**Note:** This phase only processes URLs where `finalurlbefore_has_token = True` OR `page_different = True`.

Results are stored in the `screenshot_analysis_results` table.

## Configuration

Vision model settings are configured in `config/settings.py`:
- Model name (default: `qwen3-vl:2b-instruct`)
- Round-robin server ports
- OCR and filtering thresholds
