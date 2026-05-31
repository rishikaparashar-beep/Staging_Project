# Staging Hub

Real-time warehouse staging tracker built with Flask. Workers scan barcodes at each stage (conveyer → spiral → area → trolley → grid), and the system validates, logs to Google Sheets, and auto-syncs to HMS via Playwright.

## Features

- Barcode scan validation at every staging step
- Live dashboard with per-grid statistics
- In-memory cache for instant reads, background Google Sheets writes
- Automated HMS data entry (triple-browser async Playwright)
- Daily backup to Google Drive with automatic sheet reset at 07:10 IST

## Tech Stack

Python 3 · Flask · Google Sheets API · Playwright · APScheduler · Waitress

## Quick Start

```bash
git clone https://github.com/rishikaparashar-beep/Staging_Project.git
cd Staging_Project
python -m venv venv && venv\Scripts\activate
pip install -r requirements.txt
playwright install chromium
```

Create a `.env` file:

```env
SPREADSHEET_ID=<your-sheet-id>
GRIDWISE_SPREADSHEET_ID=<gridwise-sheet-id>
DRIVE_BACKUP_FOLDER_ID=<drive-folder-id>
```

Place `credentials.json` (Google OAuth) in the root, then:

```bash
python app.py
```

Server runs at `http://localhost:5000`.

## Project Structure

```
app.py                 – Flask server & API endpoints
sheets.py              – Google Sheets read/write with caching
hms_sync.py            – Playwright automation for HMS
local_store_grid.py    – Grid pre-fetch cache
local_store_hms.py     – HMS sync queue & retry logic
daily_backup.py        – Daily CSV export & sheet cleanup
templates/             – Worker portal, dashboard, barcode generator
_cache/                – Local JSON cache files
```
