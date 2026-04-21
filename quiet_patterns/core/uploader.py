"""
core/uploader.py – YouTube Data API v3 upload with OAuth, refresh, quota safety.

Features:
  • OAuth 2.0 flow with token persistence (refresh without user interaction)
  • Pre-upload quota check (daily limit guard)
  • Exponential backoff retry (3 attempts)
  • Logs each upload to the `uploads` DB table
  • Returns YouTube video ID on success
"""

import json
import logging
import time
from pathlib import Path
from typing import Optional
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import (
    YT_CLIENT_SECRETS_FILE, YT_TOKEN_FILE, YT_SCOPES,
    YT_CATEGORY_ID, YT_DEFAULT_PRIVACY,
    YT_QUOTA_DAILY_LIMIT, YT_UPLOAD_COST,
    YT_OAUTH_PORT_CANDIDATES,
    TIMEZONE,
)
from core.database import execute_write, get_quota_used_today, log_quota_usage
from core.metadata_engine import Metadata

logger = logging.getLogger(__name__)

# Optional imports — will raise a clear RuntimeError if missing
try:
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
    from googleapiclient.errors import HttpError
    _GOOGLE_OK = True
except ImportError:
    _GOOGLE_OK = False
    logger.error(
        "Google API libraries missing. Run: "
        "pip install google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client"
    )


class QuotaExceededError(Exception):
    """Raised when the daily quota would be exceeded."""


class Uploader:
    """Handles OAuth authentication and uploading videos to YouTube."""

    MAX_RETRIES = 3
    RETRY_WAIT  = [5, 30, 90]   # seconds between retries

    def __init__(self) -> None:
        if not _GOOGLE_OK:
            raise RuntimeError("Google API libraries not installed.")
        self._service = None

    # ── Authentication ────────────────────────────────────────────────────────

    def authenticate(self) -> None:
        """Load or refresh OAuth credentials; prompt user if first run."""
        creds: Optional[Credentials] = None

        if YT_TOKEN_FILE.exists():
            with open(YT_TOKEN_FILE) as f:
                creds = Credentials.from_authorized_user_info(json.load(f), YT_SCOPES)

        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                logger.info("OAuth token refreshed successfully.")
            except Exception as exc:
                logger.warning("Token refresh failed (%s), re-authorizing.", exc)
                creds = None

        if not creds or not creds.valid:
            if not YT_CLIENT_SECRETS_FILE.exists():
                raise FileNotFoundError(
                    f"client_secrets.json not found at {YT_CLIENT_SECRETS_FILE}. "
                    "Download it from Google Cloud Console -> APIs & Services -> Credentials."
                )
            flow = InstalledAppFlow.from_client_secrets_file(
                str(YT_CLIENT_SECRETS_FILE), YT_SCOPES
            )
            creds = None
            last_exc: Optional[Exception] = None
            for port in YT_OAUTH_PORT_CANDIDATES:
                try:
                    logger.info("Starting OAuth local callback server on http://127.0.0.1:%d/", port)
                    creds = flow.run_local_server(host="127.0.0.1", port=port)
                    break
                except OSError as exc:
                    last_exc = exc
                    logger.warning("Port %d unavailable for OAuth callback: %s", port, exc)
                    continue
            if creds is None:
                raise RuntimeError(
                    f"Could not bind any OAuth callback port: {YT_OAUTH_PORT_CANDIDATES}. "
                    "Allow localhost listening or change YT_OAUTH_PORT_CANDIDATES in config.py."
                ) from last_exc
            logger.info("OAuth authorization completed.")

        # Persist refreshed token
        YT_TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(YT_TOKEN_FILE, "w") as f:
            f.write(creds.to_json())

        self._service = build("youtube", "v3", credentials=creds)
        logger.info("YouTube API service built successfully.")

    # ── Upload ────────────────────────────────────────────────────────────────

    def upload(
        self,
        video_path: Path,
        metadata: Metadata,
        video_db_id: int,
        schedule_time: Optional[str] = None,
    ) -> str:
        """
        Upload video_path to YouTube.
        Returns the YouTube video ID.
        Raises QuotaExceededError, RuntimeError, or HttpError on failure.
        """
        self._check_service()
        self._check_quota()

        tags_str = json.dumps(metadata.tags)

        body = {
            "snippet": {
                "title":       metadata.title,
                "description": metadata.description,
                "tags":        metadata.tags,
                "categoryId":  YT_CATEGORY_ID,
            },
            "status": {
                "privacyStatus":           YT_DEFAULT_PRIVACY,
                "selfDeclaredMadeForKids": False,
            },
        }

        media = MediaFileUpload(
            str(video_path),
            mimetype="video/mp4",
            resumable=True,
            chunksize=1024 * 1024 * 5,   # 5 MB chunks
        )

        last_exc: Optional[Exception] = None
        for attempt in range(self.MAX_RETRIES):
            try:
                request = self._service.videos().insert(
                    part="snippet,status",
                    body=body,
                    media_body=media,
                )
                response = None
                while response is None:
                    status, response = request.next_chunk()
                    if status:
                        pct = int(status.progress() * 100)
                        logger.info("Uploading... %d%%", pct)

                youtube_id = response["id"]
                log_quota_usage(YT_UPLOAD_COST)
                logger.info("Upload successful — YouTube ID: %s", youtube_id)

                # Persist to DB
                self._save_upload(
                    video_db_id, youtube_id, metadata, tags_str, schedule_time
                )
                return youtube_id

            except HttpError as exc:
                logger.warning(
                    "Upload attempt %d/%d failed (HTTP %s): %s",
                    attempt + 1, self.MAX_RETRIES, exc.resp.status, exc.content
                )
                last_exc = exc
                if exc.resp.status in (400, 401, 403):
                    raise   # Non-retriable
            except Exception as exc:
                logger.warning(
                    "Upload attempt %d/%d failed: %s",
                    attempt + 1, self.MAX_RETRIES, exc
                )
                last_exc = exc

            if attempt < self.MAX_RETRIES - 1:
                wait = self.RETRY_WAIT[attempt]
                logger.info("Retrying in %ds...", wait)
                time.sleep(wait)

        raise RuntimeError(f"All {self.MAX_RETRIES} upload attempts failed.") from last_exc

    # ── Thumbnail ─────────────────────────────────────────────────────────────

    def upload_thumbnail(self, video_id: str, thumbnail_path: Path) -> None:
        """
        Upload a custom thumbnail image for a video.

        NOTE: Your YouTube account must be verified (phone-verified) before
        custom thumbnails can be uploaded. Check YouTube Studio →
        Customization → Custom thumbnails. If the call fails silently, the
        account likely isn't verified yet.
        """
        self._check_service()
        try:
            self._service.thumbnails().set(
                videoId=video_id,
                media_body=MediaFileUpload(str(thumbnail_path), mimetype="image/jpeg"),
            ).execute()
            logger.info("Thumbnail uploaded for video %s", video_id)
        except HttpError as exc:
            logger.warning("Thumbnail upload failed (HTTP %s): %s", exc.resp.status, exc.content)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _check_service(self) -> None:
        if self._service is None:
            raise RuntimeError("Uploader not authenticated. Call authenticate() first.")

    def _check_quota(self) -> None:
        used = get_quota_used_today()
        if used + YT_UPLOAD_COST > YT_QUOTA_DAILY_LIMIT:
            raise QuotaExceededError(
                f"Daily quota exhausted ({used}/{YT_QUOTA_DAILY_LIMIT} units used)."
            )
        logger.info("Quota OK: %d units used today.", used)

    def _save_upload(
        self,
        video_db_id: int,
        youtube_id: str,
        metadata: Metadata,
        tags_str: str,
        schedule_time: Optional[str],
    ) -> None:
        from datetime import datetime
        post_hour = datetime.now(TIMEZONE).hour
        execute_write(
            """
            INSERT INTO uploads
                (video_id, youtube_id, title, description, tags,
                 privacy, category_id, scheduled_time, post_hour)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                video_db_id, youtube_id, metadata.title,
                metadata.description, tags_str,
                YT_DEFAULT_PRIVACY, YT_CATEGORY_ID,
                schedule_time, post_hour,
            )
        )
        # Mark video as uploaded
        execute_write(
            "UPDATE videos SET uploaded = 1 WHERE id = ?", (video_db_id,)
        )
