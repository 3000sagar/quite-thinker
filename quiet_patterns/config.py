"""
config.py – Centralized configuration for Quiet Patterns YouTube Shorts system.
"""

from pathlib import Path
from zoneinfo import ZoneInfo
import os

# ─── Paths ───────────────────────────────────────────────────────────────────
BASE_DIR       = Path(__file__).parent
ASSETS_DIR     = BASE_DIR / "assets"
MUSIC_DIR      = ASSETS_DIR / "music"
OUTPUT_DIR     = BASE_DIR / "output"
TOKENS_DIR     = BASE_DIR / "tokens"
...
DB_PATH = Path(os.environ.get("QP_DB_PATH", str(BASE_DIR / "quiet_patterns.db")))


# Ensure dirs exist at import time
for _d in (ASSETS_DIR, MUSIC_DIR, OUTPUT_DIR, TOKENS_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# ─── Channel Identity ────────────────────────────────────────────────────────
CHANNEL_NAME   = "Quiet Patterns"
CHANNEL_NICHE  = "therapist_advice"

# ─── Video Settings ──────────────────────────────────────────────────────────
VIDEO_WIDTH    = 1080
VIDEO_HEIGHT   = 1920
VIDEO_FPS      = 30
VIDEO_CODEC    = "libx264"
VIDEO_BITRATE  = "4000k"
VIDEO_DURATION_MIN = 20   # seconds  (shorter = higher retention)
VIDEO_DURATION_MAX = 25   # seconds
VIDEO_QA_ENABLED = True
VIDEO_QA_MIN_DURATION_SEC = 24.0
VIDEO_QA_MAX_DURATION_SEC = 32.0
VIDEO_QA_HOOK_MAX_START_SEC = 0.6
VIDEO_QA_HOOK_MIN_DURATION_SEC = 1.2
VIDEO_QA_MAX_WORDS_PER_LINE = 8
VIDEO_QA_MAX_CHARS_PER_LINE = 90
VIDEO_QA_MIN_SEGMENT_SEC = 1.2
VIDEO_QA_MAX_SEGMENT_SEC = 3.2
VIDEO_QA_MAX_AVG_SEGMENT_SEC = 2.6
VIDEO_QA_PAYOFF_MIN_SHARE = 0.72
VIDEO_QA_PAYOFF_MAX_SHARE = 0.92

BACKGROUND_COLOR = "#0f1115"   # deep charcoal

# ─── Text Rendering ──────────────────────────────────────────────────────────
FONT_SIZE_HOOK   = 34
FONT_SIZE_BODY   = 34
FONT_SIZE_CLOSE  = 34
FONT_COLOR       = "#e8e8e8"
FONT_DIM_COLOR   = "#a0a0a0"
PRIMARY_CAPTION_FONT = "Benguiat Regular"
PRIMARY_CAPTION_FONT_FILE = r"C:/Users/ASUS/OneDrive/Desktop/Youtube-automation/quiet_patterns/assets/font/benguiat.ttf"
# Preferred fonts (will pick first available)
PREFERRED_FONTS  = [
    "Benguiat Regular",
    "Montserrat-Light",
    "DejaVu-Sans",
    "Arial",
    "Liberation-Sans",
]

# ─── Audio (Optional Royalty-Free Background Music) ─────────────────────────
ENABLE_BG_MUSIC = True
MUSIC_VOLUME    = 0.18


# ─── Script Settings ─────────────────────────────────────────────────────────
SCRIPT_MIN_WORDS = 40
SCRIPT_MAX_WORDS = 90
SCRIPT_SCORE_THRESHOLD = 38          # reject below this viral score
PIPELINE_MAX_SCRIPT_ATTEMPTS = 5
PIPELINE_MAX_VIDEO_QA_REJECT_RETRIES = 8
ENABLE_DYNAMIC_SCORE_THRESHOLD = True
DYNAMIC_SCORE_LOOKBACK = 40
DYNAMIC_SCORE_FLOOR = 36.0
DYNAMIC_SCORE_CEILING = 62.0
ENABLE_AUTO_REWRITE = True
AUTO_REWRITE_PASSES = 2
ENABLE_VARIANT_AB_TEST = True
AB_VARIANT_COUNT = 4
ENABLE_CONTENT_EXPERIMENTS = True
EXPERIMENT_LOOKBACK_SCRIPTS = 30
EXPERIMENT_ARMS = ["pattern_interrupt", "micro_action", "hidden_cost"]  # identity_mirror dropped (worst performer)
SCRIPT_GENERATOR_BACKEND = "auto"  # options: "ollama", "template", "auto"
SCRIPT_GENERATOR_FALLBACK_TO_TEMPLATE = True
SCRIPT_BUILD_FROM_SCRATCH = True
SCRIPT_SEGMENT_MAX_WORDS_HOOK = 6
SCRIPT_SEGMENT_MAX_WORDS_BODY = 12
SCRIPT_SEGMENT_MAX_WORDS_CLOSING = 8
MODEL_OUTPUT_LOG_CSV = OUTPUT_DIR / "model_outputs.csv"
PIPELINE_REPORTS_ENABLED = True
PIPELINE_REPORTS_DIR = OUTPUT_DIR / "reports"
PIPELINE_DASHBOARD_HTML = PIPELINE_REPORTS_DIR / "index.html"
THEME_COOLDOWN_RECENT_SCRIPTS = 4
NOVELTY_WINDOW = 200
NOVELTY_MIN_SCORE = 0.28
GENERATE_HOOK_VARIANTS = False
HOOK_VARIANT_COUNT = 3
GENERATE_MULTILINGUAL_VARIANTS = False
SCRIPT_VARIANT_LANGUAGES = ["en", "hi"]

# ─── Ollama Generation ───────────────────────────────────────────────────────
OLLAMA_BASE_URL = "http://127.0.0.1:11434"
OLLAMA_MODEL = "llama3:latest"
OLLAMA_TIMEOUT_SEC = 10
OLLAMA_TEMPERATURE = 0.6

# ─── Scheduler Settings ──────────────────────────────────────────────────────
TIMEZONE               = ZoneInfo("Asia/Kolkata")
DEFAULT_POST_HOURS     = [9, 11, 18, 21]  # Analytics-optimized: 9am, 11am, 6pm, 9pm IST
UPLOADS_BEFORE_OPTIMIZE = 10         # analyze after this many uploads

# ─── YouTube API ─────────────────────────────────────────────────────────────
YT_CLIENT_SECRETS_FILE = BASE_DIR / "client_secrets.json"
YT_TOKEN_FILE          = TOKENS_DIR / "youtube_token.json"
YT_SCOPES              = ["https://www.googleapis.com/auth/youtube.upload",
                           "https://www.googleapis.com/auth/youtube.readonly"]
YT_CATEGORY_ID         = "27"        # Education
YT_DEFAULT_PRIVACY     = "public"
YT_QUOTA_DAILY_LIMIT   = 10_000
YT_UPLOAD_COST         = 1_600       # quota units per upload
YT_OAUTH_PORT_CANDIDATES = [8080, 8081, 8090, 8888]
ENABLE_UPLOAD = True
DELETE_VIDEO_AFTER_SUCCESS_UPLOAD = True
DELETE_VIDEO_ON_QA_REJECT = True

# ─── Instagram Graph API (Optional) ─────────────────────────────────────────
# Requires an Instagram Professional account connected to a Facebook Page,
# with a long-lived access token and a PUBLIC video URL.
ENABLE_INSTAGRAM_UPLOAD = False
IG_USER_ID = ""                  # numeric Instagram Business/Creator User ID
IG_ACCESS_TOKEN = ""             # long-lived Graph API token
IG_API_VERSION = "v23.0"
IG_VIDEO_URL_TEMPLATE = ""       # e.g. "https://cdn.example.com/reels/{filename}"
IG_SHARE_TO_FEED = True
IG_PUBLISH_POLL_INTERVAL_SEC = 5
IG_PUBLISH_TIMEOUT_SEC = 180

# ─── Analytics Pull ──────────────────────────────────────────────────────────
ANALYTICS_FETCH_HOURS_AFTER = 24     # fetch stats this many hours post-upload
RETENTION_READY_MIN_POSITIVE_SAMPLES = 10  # Start optimizing sooner (was 20)
RETENTION_READY_MIN_DISTINCT_HOURS = 2

# ─── Logging ─────────────────────────────────────────────────────────────────
LOG_LEVEL  = "INFO"
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
LOG_FILE   = BASE_DIR / "quiet_patterns.log"
