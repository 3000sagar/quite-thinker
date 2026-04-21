# Quiet Patterns – YouTube Shorts Automation System

A fully autonomous YouTube Shorts production and publishing system for the **Quiet Patterns** channel.
Psychology-based, calm-dark, silent, minimal text-only vertical Shorts — generated, rendered, and uploaded automatically.

---

## Project Structure

```
quiet_patterns/
├── core/
│   ├── database.py        # SQLite schema, CRUD helpers, quota tracking
│   ├── script_engine.py   # Psychology script generator + duplicate detection
│   ├── scoring_engine.py  # Rule-based retention scoring (viral score 0–100)
│   ├── video_engine.py    # moviepy/ffmpeg 1080×1920 video renderer
│   ├── metadata_engine.py # Title / description / tag generator
│   ├── uploader.py        # YouTube Data API v3 OAuth uploader
│   ├── analytics.py       # Analytics fetch + self-optimization
│   └── scheduler.py       # APScheduler async intelligent scheduler
├── assets/                # (reserved for future font/image assets)
├── output/                # Rendered MP4 files (auto-created)
├── tokens/                # OAuth token storage (auto-created)
├── config.py              # Centralized settings
├── main.py                # CLI entrypoint
├── requirements.txt
└── README.md
```

---

## Prerequisites

### 1. Python 3.12+

```powershell
# Check your version
python --version
```

### 2. ffmpeg Installation

**Windows:**
```powershell
# Using winget (recommended)
winget install Gyan.FFmpeg

# Or download from https://ffmpeg.org/download.html
# Add ffmpeg/bin to your PATH environment variable
ffmpeg -version   # verify
```

**Linux/Ubuntu:**
```bash
sudo apt update && sudo apt install ffmpeg -y
ffmpeg -version
```

### 3. Python Dependencies

```powershell
# Create and activate a virtual environment (recommended)
python -m venv .venv
.venv\Scripts\Activate.ps1          # Windows
# source .venv/bin/activate         # Linux/macOS

# Install dependencies
pip install -r requirements.txt
```

---

## Google Cloud & YouTube API Setup

### Step 1 – Create a Google Cloud Project

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Click **New Project** → name it `quiet-patterns`
3. Select the project

### Step 2 – Enable Required APIs

In the API Library, enable:
- **YouTube Data API v3**
- **YouTube Analytics API**

### Step 3 – Create OAuth 2.0 Credentials

1. Go to **APIs & Services → Credentials**
2. Click **Create Credentials → OAuth client ID**
3. Application type: **Desktop app**
4. Name: `quiet-patterns-desktop`
5. Download the JSON file
6. Rename it to `client_secrets.json`
7. Place it in the `quiet_patterns/` folder (same level as `main.py`)

### Step 4 – Configure OAuth Consent Screen

1. Go to **APIs & Services → OAuth consent screen**
2. User type: **External**
3. Fill in App name: `Quiet Patterns`, support email
4. Add scopes:
   - `https://www.googleapis.com/auth/youtube.upload`
   - `https://www.googleapis.com/auth/youtube.readonly`
5. Add your Google account under **Test users**

> [!IMPORTANT]
> The `client_secrets.json` must be in `quiet_patterns/` before running `--auth`.
> Never commit this file to version control.

---

## Running the System

All commands are run from the `quiet_patterns/` directory:

```powershell
cd quiet_patterns
```

### 1. Initialize Database

```powershell
python main.py --init-db
```

Creates `quiet_patterns.db` with all tables. Safe to run multiple times.

### 2. Authenticate with YouTube

```powershell
python main.py --auth
```

Opens a browser window for Google OAuth. After granting access, the token is saved at `tokens/youtube_token.json`. **Only needed once** — it auto-refreshes afterward.

### 3. Run Pipeline Once (Test)

```powershell
python main.py --once
```

Runs the full pipeline:
1. Generates a psychology script
2. Scores for retention (uses adaptive viral threshold by default)
3. Auto-rewrites weak scripts and rescoring pass (if enabled)
4. Runs A/B variant scoring and keeps the strongest candidate
5. Renders 1080×1920 MP4
6. Generates metadata
7. Uploads to YouTube

### 3.0 Build Dashboard

```powershell
python main.py --dashboard
```

Builds a local dashboard at:

`output/reports/index.html`

Shows run history, thresholds, attempt counts, and viral trend.

### 3.1 Dry Run (No Upload)

```powershell
python main.py --once --dry-run
```

Runs generation + scoring + render + metadata and skips YouTube upload.
Useful for testing quality safely without consuming quota.

### 3.2 Rewrite + A/B Controls

In `config.py`:

```python
ENABLE_AUTO_REWRITE = True
AUTO_REWRITE_PASSES = 1
ENABLE_VARIANT_AB_TEST = True
AB_VARIANT_COUNT = 4
```

These controls improve weak scripts automatically and select the best-scoring variant before rendering.

### 3.2 Local Ollama Generation (Optional, Recommended)

You can generate scripts with a local LLM via Ollama.

1. Start Ollama server:
```powershell
ollama serve
```

2. Pull a model (example):
```powershell
ollama pull llama3.1:8b
```

3. In `config.py`, ensure:
```python
SCRIPT_GENERATOR_BACKEND = "ollama"
OLLAMA_BASE_URL = "http://127.0.0.1:11434"
OLLAMA_MODEL = "llama3.1:8b"
```

If Ollama is unavailable, generator can fall back to templates when
`SCRIPT_GENERATOR_FALLBACK_TO_TEMPLATE = True`.

### 3.1 Use An Excel Script Bank (Recommended)

To avoid limited/repeated content, add your own rows in:

`assets/script_bank.xlsx`

Use sheet name: `scripts` (or first sheet), with headers:
- `hook`
- `body`
- `closing`

Example row:
- `hook`: `if you feel exhausted for no clear reason —`
- `body`: `you go quiet. you pull away...`
- `closing`: `and the cycle continues — quietly.`

When the file exists, the generator prefers Excel rows first, then falls back to built-in templates.

### 4. Daemon Mode (24/7 Automation)

```powershell
python main.py --daemon
```

Runs continuously. Posts at **13:00 and 19:00** (IST by default). After 10 uploads, automatically adjusts posting time to the best-performing hour.

### 5. Fetch Analytics

```powershell
python main.py --analytics
```

Pulls view/like/comment data for all uploads older than 24 hours and stores in the database. Also surfaces optimal posting hours.

### Pipeline Reports

Each pipeline run can emit a JSON report to:

`output/reports/`

The report includes:
- effective threshold used for that run
- per-attempt scores/rejection reasons
- final output status (video path, upload status)

---

## Changing Timezone / Post Hours

Edit `config.py`:

```python
from zoneinfo import ZoneInfo
TIMEZONE           = ZoneInfo("America/New_York")   # change to your timezone
DEFAULT_POST_HOURS = [12, 18]                        # 12 PM and 6 PM
```

All [valid timezone names](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones) from the `zoneinfo` module are supported.

---

## Running 24/7 on Windows

### Option A – Task Scheduler (Simple)

1. Open **Task Scheduler** → Create Basic Task
2. Trigger: At startup
3. Action: Start a program
   - Program: `C:\path\to\.venv\Scripts\python.exe`
   - Arguments: `main.py --daemon`
   - Start in: `C:\Users\ASUS\Downloads\Youtube-automation\quiet_patterns`
4. Set to run whether user is logged on or not

### Option B – NSSM (Non-Sucking Service Manager)

```powershell
# Download NSSM from https://nssm.cc/download
nssm install QuietPatterns "C:\path\to\.venv\Scripts\python.exe" "main.py --daemon"
nssm set QuietPatterns AppDirectory "C:\...\quiet_patterns"
nssm start QuietPatterns
```

## Running 24/7 on Linux

```bash
# Create a systemd service
sudo nano /etc/systemd/system/quiet-patterns.service
```

```ini
[Unit]
Description=Quiet Patterns YouTube Automation
After=network.target

[Service]
Type=simple
User=your_user
WorkingDirectory=/path/to/quiet_patterns
ExecStart=/path/to/.venv/bin/python main.py --daemon
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable quiet-patterns
sudo systemctl start quiet-patterns
sudo systemctl status quiet-patterns
```

---

## Database Schema

| Table        | Purpose                                    |
|--------------|------------------------------------------- |
| `scripts`    | All generated scripts with scores + hashes |
| `videos`     | Rendered video file paths                  |
| `uploads`    | YouTube upload records + post time         |
| `analytics`  | Per-video performance metrics              |
| `quota_log`  | Daily API quota tracking                   |

View the database:
```powershell
# Windows - using sqlite3 CLI (bundled with Python)
python -c "import sqlite3; conn=sqlite3.connect('quiet_patterns.db'); [print(r) for r in conn.execute(\"SELECT name FROM sqlite_master WHERE type='table'\")]"
```

---

## Self-Optimization Behaviour

| Trigger                         | Optimization                                  |
|---------------------------------|-----------------------------------------------|
| After 10 uploads                | Analyzes best posting hour by retention %     |
| Daily at 00:05                  | Re-evaluates and rebuilds posting schedule    |
| Continuous                      | Script scorer weighted toward high-retention patterns |

Analytics data after 100+ uploads will reveal which:
- Emotional triggers drive the most retention
- Hook patterns have the highest loop completion
- Word count ranges perform best

---

## Example Generated Short

**Script (generated by system):**
```
hook:    if you feel exhausted for no clear reason —
body:    you go quiet. you pull away. you tell yourself you are fine.
         but this is not weakness — it is a nervous system response.
         your brain learned, early on, that stillness was safer than speech.
         so it defaults there. and it silently rewires your brain.
closing: and the cycle continues — quietly.
```

**Metadata:**
```
title:       the psychology behind feeling exhausted   (47 chars)
description: calm exploration of emotional suppression through a psychological lens.
tags:        #shorts #psychologyfacts #humanbehavior #emotionalintelligence #mindpsychology #anxietyawareness
```

**Video:**
- Resolution: 1080×1920 (9:16)
- Duration: ~24 seconds
- Background: Animated #0f1115 gradient with particle noise
- Text: Staggered fade-in, sentence by sentence
- Close: 3% zoom-in on final line
- Loop echo: Hook repeats dimly at end
- Audio: None

---

## Growth Tuning Strategy

1. **First 30 days**: Post 2× daily. Let the analytics engine collect data.
2. **Day 31+**: Daemon automatically shifts to top-2 performing hours.
3. **Manual tuning**: After 50 uploads, run `--analytics` and check `analytics` table.
   Identify which `post_hour` and `emotional_score` columns correlate with highest `retention_pct`.
4. **Hook pattern A/B**: The `hook` column in `scripts` table lets you see which hook templates
   appeared in the highest-retention videos.
5. **Quota**: Each upload = 1,600 units. Default 10,000/day limit = 6 uploads/day max.
   Apply for quota increase in Google Cloud Console → APIs → YouTube Data API v3.

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| `moviepy` install fails | Install `imageio[ffmpeg]` separately: `pip install imageio[ffmpeg]` |
| `ffmpeg not found` | Add ffmpeg to system PATH and restart terminal |
| `client_secrets.json not found` | Download from Google Cloud Console (see OAuth setup above) |
| `Token expired` | Just run `python main.py --auth` again |
| `Quota exceeded` | Wait until UTC midnight; quota resets daily |
| Font not rendering | Install DejaVu fonts or set `PREFERRED_FONTS` in config.py |

---

## Security Guardrail

This repo includes a pre-commit secret scanner at:

`tools/secret_scan.py`

Git hook path:

`.githooks/pre-commit`

Enable hooks locally (one-time):

```powershell
git config core.hooksPath .githooks
```

It scans staged files and blocks commits if likely secrets are detected
(`client_secrets.json`, `tokens/`, private key blocks, token/key patterns).

---

> [!NOTE]
> All content generated by this system is original, template-based, and psychology-informed.
> No copyrighted content is scraped or reused. Each script is SHA-256 hashed to prevent exact duplicates.
