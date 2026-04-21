"""
core/analytics.py – Fetch YouTube analytics and self-optimize posting strategy.

Responsibilities:
  • Pull view/retention/like stats via YouTube Analytics API
  • Persist each fetch to `analytics` DB table
  • Auto-adjust optimal post hour after N uploads
  • Surface hook pattern and script feature signals
"""

import json
import logging
import statistics
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import (
    YT_TOKEN_FILE, YT_SCOPES, YT_CLIENT_SECRETS_FILE,
    UPLOADS_BEFORE_OPTIMIZE, DEFAULT_POST_HOURS,
    ANALYTICS_FETCH_HOURS_AFTER, TIMEZONE,
    RETENTION_READY_MIN_POSITIVE_SAMPLES,
    RETENTION_READY_MIN_DISTINCT_HOURS,
)
from core.database import execute_query, execute_write, execute_many

logger = logging.getLogger(__name__)

try:
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
    _GOOGLE_OK = True
except ImportError:
    _GOOGLE_OK = False


class AnalyticsEngine:
    """Fetches per-video analytics and drives self-optimization."""

    def __init__(self) -> None:
        self._yt_service    = None
        self._anal_service  = None

    # ── Auth ──────────────────────────────────────────────────────────────────

    def authenticate(self) -> None:
        if not _GOOGLE_OK:
            raise RuntimeError("Google libraries not installed.")
        creds: Optional[Credentials] = None
        if YT_TOKEN_FILE.exists():
            with open(YT_TOKEN_FILE) as f:
                creds = Credentials.from_authorized_user_info(json.load(f), YT_SCOPES)
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        if not creds or not creds.valid:
            raise RuntimeError("No valid YouTube credentials found. Run the uploader auth first.")

        self._yt_service   = build("youtube",          "v3",  credentials=creds)
        self._anal_service = build("youtubeAnalytics", "v2",  credentials=creds)
        logger.info("AnalyticsEngine authenticated.")

    # ── Fetch & persist ───────────────────────────────────────────────────────

    def fetch_pending_analytics(self) -> None:
        """
        Find uploads that are > ANALYTICS_FETCH_HOURS_AFTER hours old
        and haven't been fetched recently, then pull their stats.
        """
        cutoff_expr = f"-{int(ANALYTICS_FETCH_HOURS_AFTER)} hours"

        uploads = execute_query(
            """
            SELECT u.id, u.youtube_id, u.post_hour, u.uploaded_at,
                   s.word_count, s.viral_score as emotional_score,
                   v.id as video_id
            FROM uploads u
            JOIN videos  v ON v.id = u.video_id
            JOIN scripts s ON s.id = v.script_id
            WHERE datetime(u.uploaded_at) <= datetime('now', ?)
              AND (
                  NOT EXISTS (
                      SELECT 1 FROM analytics a WHERE a.upload_id = u.id
                  )
                  OR EXISTS (
                      SELECT 1
                      FROM analytics a0
                      WHERE a0.upload_id = u.id
                        AND a0.id = (
                            SELECT MAX(a1.id)
                            FROM analytics a1
                            WHERE a1.upload_id = u.id
                        )
                        AND COALESCE(a0.retention_pct, 0) <= 0
                        AND datetime(a0.fetched_at) <= datetime('now', '-6 hours')
                  )
              )
            """,
            (cutoff_expr,)
        )

        if not uploads:
            logger.info("No pending analytics to fetch.")
            return

        for row in uploads:
            try:
                stats = self._fetch_video_stats(
                    row["youtube_id"],
                    uploaded_at=row["uploaded_at"],
                )
                if stats:
                    self._upsert_analytics_for_upload(
                        upload_id=row["id"],
                        views=stats.get("views", 0),
                        avg_view_duration=stats.get("avg_view_duration", 0.0),
                        retention_pct=stats.get("retention_pct", 0.0),
                        likes=stats.get("likes", 0),
                        comments=stats.get("comments", 0),
                        post_hour=self._derive_local_post_hour(row["uploaded_at"], row["post_hour"]),
                        script_word_count=row["word_count"],
                        emotional_score=row["emotional_score"],
                    )
                    logger.info(
                        "Analytics stored for video %s: views=%d",
                        row["youtube_id"], stats.get("views", 0)
                    )
            except Exception as exc:
                logger.warning("Failed fetching analytics for %s: %s", row["youtube_id"], exc)

    # ── Optimization ──────────────────────────────────────────────────────────

    def get_optimal_post_hours(self) -> list[int]:
        """
        After UPLOADS_BEFORE_OPTIMIZE uploads, analyze average retention by hour
        and return the top-2 performing hours. Falls back to defaults.
        """
        total_uploads = execute_query("SELECT COUNT(*) as cnt FROM uploads", ())[0]["cnt"]
        if total_uploads < UPLOADS_BEFORE_OPTIMIZE:
            logger.info(
                "Not enough data (%d/%d uploads) to optimize posting hours.",
                total_uploads, UPLOADS_BEFORE_OPTIMIZE
            )
            return DEFAULT_POST_HOURS

        rows = execute_query(
            """
            SELECT post_hour,
                   AVG(retention_pct) as avg_retention,
                   AVG(views)         as avg_views,
                   COUNT(*)           as sample_count
            FROM analytics
            WHERE post_hour IS NOT NULL
            GROUP BY post_hour
            HAVING sample_count >= 2
            ORDER BY avg_retention DESC, avg_views DESC
            """,
            ()
        )

        if len(rows) < 2:
            logger.info("Insufficient hour-level data; using default hours.")
            return DEFAULT_POST_HOURS

        all_retention_zero = all(float(r["avg_retention"] or 0.0) <= 0.0 for r in rows)
        if all_retention_zero:
            # Analytics API retention unavailable: fall back to views-only ranking.
            ranked = sorted(
                rows,
                key=lambda r: (float(r["avg_views"] or 0.0), int(r["sample_count"] or 0)),
                reverse=True,
            )
            top_hours = [ranked[0]["post_hour"], ranked[1]["post_hour"]]
            logger.info(
                "Retention unavailable (all 0.0). Using views-only hour ranking: %s (views: %.1f, %.1f)",
                top_hours, float(ranked[0]["avg_views"] or 0.0), float(ranked[1]["avg_views"] or 0.0)
            )
            return top_hours

        top_hours = [rows[0]["post_hour"], rows[1]["post_hour"]]
        logger.info(
            "Optimal post hours updated: %s (retention: %.1f%%, %.1f%%)",
            top_hours, rows[0]["avg_retention"], rows[1]["avg_retention"]
        )
        return top_hours

    def get_best_script_features(self) -> dict:
        """
        Return mean word count and emotional score of top-performing scripts.
        Used by ScriptEngine for self-tuning (logged; not yet fed back automatically).
        """
        rows = execute_query(
            """
            SELECT a.script_word_count, a.emotional_score, a.retention_pct
            FROM analytics a
            WHERE a.retention_pct > 0
            ORDER BY a.retention_pct DESC
            LIMIT 20
            """,
            ()
        )
        if not rows:
            logger.info("No positive retention data yet; skipping best-script feature extraction.")
            return {}

        word_counts = [r["script_word_count"] for r in rows if r["script_word_count"]]
        emo_scores  = [r["emotional_score"]   for r in rows if r["emotional_score"]]

        result = {}
        if word_counts:
            result["optimal_word_count"] = round(statistics.mean(word_counts), 1)
        if emo_scores:
            result["optimal_emotional_score"] = round(statistics.mean(emo_scores), 2)

        logger.info("Best script features: %s", result)
        return result

    def get_retention_readiness(self) -> dict:
        """
        Return whether retention data volume/coverage is sufficient
        to trust retention-driven optimization.
        """
        rows = execute_query(
            """
            SELECT
                COUNT(*) AS total_rows,
                SUM(CASE WHEN COALESCE(retention_pct, 0) > 0 THEN 1 ELSE 0 END) AS positive_rows,
                COUNT(DISTINCT CASE WHEN COALESCE(retention_pct, 0) > 0 THEN post_hour END) AS hour_coverage,
                AVG(CASE WHEN COALESCE(retention_pct, 0) > 0 THEN retention_pct END) AS avg_positive_retention
            FROM analytics
            """,
            (),
        )
        row = rows[0] if rows else {}
        total_rows = int(row["total_rows"] or 0)
        positive_rows = int(row["positive_rows"] or 0)
        hour_coverage = int(row["hour_coverage"] or 0)
        avg_positive_retention = float(row["avg_positive_retention"] or 0.0)

        ready = (
            positive_rows >= int(RETENTION_READY_MIN_POSITIVE_SAMPLES)
            and hour_coverage >= int(RETENTION_READY_MIN_DISTINCT_HOURS)
        )
        status = "ready" if ready else "warming_up"
        return {
            "status": status,
            "ready": bool(ready),
            "total_rows": total_rows,
            "positive_rows": positive_rows,
            "hour_coverage": hour_coverage,
            "avg_positive_retention": round(avg_positive_retention, 2),
            "required_positive_rows": int(RETENTION_READY_MIN_POSITIVE_SAMPLES),
            "required_hour_coverage": int(RETENTION_READY_MIN_DISTINCT_HOURS),
        }

    def get_experiment_arm_performance(self) -> list[dict]:
        """
        Return arm-level performance summary using retention when available,
        with views as fallback context.
        """
        rows = execute_query(
            """
            SELECT
                s.experiment_arm AS arm,
                COUNT(*) AS samples,
                AVG(a.views) AS avg_views,
                AVG(a.retention_pct) AS avg_retention
            FROM analytics a
            JOIN uploads u ON u.id = a.upload_id
            JOIN videos v ON v.id = u.video_id
            JOIN scripts s ON s.id = v.script_id
            WHERE s.experiment_arm IS NOT NULL
            GROUP BY s.experiment_arm
            ORDER BY AVG(a.retention_pct) DESC, AVG(a.views) DESC
            """,
            (),
        )
        out: list[dict] = []
        for r in rows:
            out.append(
                {
                    "arm": str(r["arm"] or "").strip(),
                    "samples": int(r["samples"] or 0),
                    "avg_views": round(float(r["avg_views"] or 0.0), 2),
                    "avg_retention": round(float(r["avg_retention"] or 0.0), 2),
                }
            )
        return out

    def backfill_upload_post_hours_local(self) -> int:
        """
        One-time repair utility:
        Normalize historical uploads.post_hour values to local TIMEZONE.
        """
        rows = execute_query(
            "SELECT id, uploaded_at, post_hour FROM uploads ORDER BY id ASC",
            (),
        )
        if not rows:
            logger.info("No uploads found for post-hour backfill.")
            return 0

        updates: list[tuple[int, int]] = []
        for row in rows:
            repaired = self._derive_local_post_hour(row["uploaded_at"], row["post_hour"])
            if repaired is None:
                continue
            current = row["post_hour"]
            if current is None or int(current) != int(repaired):
                updates.append((int(repaired), int(row["id"])))

        if updates:
            execute_many("UPDATE uploads SET post_hour = ? WHERE id = ?", updates)
            # Keep analytics hour aligned with repaired uploads where rows already exist.
            execute_write(
                """
                UPDATE analytics
                SET post_hour = (
                    SELECT u.post_hour
                    FROM uploads u
                    WHERE u.id = analytics.upload_id
                )
                WHERE EXISTS (
                    SELECT 1
                    FROM uploads u
                    WHERE u.id = analytics.upload_id
                )
                """,
                (),
            )
            logger.info("Backfilled local post_hour for %d uploads.", len(updates))
        else:
            logger.info("All upload post_hour values already aligned to local timezone.")
        return len(updates)

    # ── Private API calls ─────────────────────────────────────────────────────

    def _fetch_video_stats(self, youtube_id: str, uploaded_at: Optional[str] = None) -> Optional[dict]:
        """
        Fetch video stats from both Data API and Analytics API.
        Falls back gracefully when Analytics data is not available yet.
        """
        if self._yt_service is None or self._anal_service is None:
            raise RuntimeError("Not authenticated.")

        public_stats = self._fetch_public_video_stats(youtube_id) or {}
        views = int(public_stats.get("views", 0))
        likes = int(public_stats.get("likes", 0))
        comments = int(public_stats.get("comments", 0))
        avg_view_duration = 0.0
        retention_pct = 0.0

        start_date = self._derive_start_date(uploaded_at)
        end_date = datetime.now(timezone.utc).date().isoformat()

        try:
            resp = self._anal_service.reports().query(
                ids="channel==MINE",
                startDate=start_date,
                endDate=end_date,
                metrics="views,averageViewDuration,averageViewPercentage,likes,comments",
                filters=f"video=={youtube_id}",
            ).execute()
            rows = resp.get("rows", []) or []
            if rows:
                headers = [h.get("name") for h in resp.get("columnHeaders", [])]
                mapped = dict(zip(headers, rows[0]))
                views = max(views, int(float(mapped.get("views", 0) or 0)))
                likes = max(likes, int(float(mapped.get("likes", 0) or 0)))
                comments = max(comments, int(float(mapped.get("comments", 0) or 0)))
                avg_view_duration = float(mapped.get("averageViewDuration", 0.0) or 0.0)
                retention_pct = float(mapped.get("averageViewPercentage", 0.0) or 0.0)
                retention_pct = max(0.0, min(100.0, retention_pct))
        except Exception as exc:
            logger.warning("YouTube Analytics query failed for %s: %s", youtube_id, exc)

        return {
            "views": views,
            "likes": likes,
            "comments": comments,
            "avg_view_duration": avg_view_duration,
            "retention_pct": retention_pct,
        }

    def _fetch_public_video_stats(self, youtube_id: str) -> Optional[dict]:
        """Fetch basic public statistics from Data API."""
        try:
            resp = self._yt_service.videos().list(part="statistics", id=youtube_id).execute()
            items = resp.get("items", [])
            if not items:
                return None
            stats = items[0].get("statistics", {})
            return {
                "views": int(stats.get("viewCount", 0)),
                "likes": int(stats.get("likeCount", 0)),
                "comments": int(stats.get("commentCount", 0)),
            }
        except Exception as exc:
            logger.warning("Public stats fetch failed for %s: %s", youtube_id, exc)
            return None

    @staticmethod
    def _upsert_analytics_for_upload(
        upload_id: int,
        views: int,
        avg_view_duration: float,
        retention_pct: float,
        likes: int,
        comments: int,
        post_hour: Optional[int],
        script_word_count: Optional[int],
        emotional_score: Optional[float],
    ) -> None:
        """
        Keep one current analytics row per upload by updating latest row if present.
        """
        existing = execute_query(
            """
            SELECT id
            FROM analytics
            WHERE upload_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (upload_id,),
        )
        if existing:
            execute_write(
                """
                UPDATE analytics
                SET fetched_at = datetime('now'),
                    views = ?,
                    avg_view_duration = ?,
                    retention_pct = ?,
                    like_count = ?,
                    comment_count = ?,
                    post_hour = ?,
                    script_word_count = ?,
                    emotional_score = ?
                WHERE id = ?
                """,
                (
                    int(views),
                    float(avg_view_duration or 0.0),
                    float(retention_pct or 0.0),
                    int(likes),
                    int(comments),
                    post_hour,
                    script_word_count,
                    emotional_score,
                    int(existing[0]["id"]),
                ),
            )
            return

        execute_write(
            """
            INSERT INTO analytics
                (upload_id, views, avg_view_duration, retention_pct,
                 like_count, comment_count, post_hour,
                 script_word_count, emotional_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                upload_id,
                int(views),
                float(avg_view_duration or 0.0),
                float(retention_pct or 0.0),
                int(likes),
                int(comments),
                post_hour,
                script_word_count,
                emotional_score,
            ),
        )

    @staticmethod
    def _derive_start_date(uploaded_at: Optional[str]) -> str:
        """
        Convert DB uploaded_at timestamp to API yyyy-mm-dd.
        Adds a small backward buffer so early-day uploads are captured.
        """
        if not uploaded_at:
            return (datetime.now(timezone.utc) - timedelta(days=30)).date().isoformat()
        try:
            dt = datetime.strptime(str(uploaded_at), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            dt = dt - timedelta(days=1)
            return dt.date().isoformat()
        except Exception:
            return (datetime.now(timezone.utc) - timedelta(days=30)).date().isoformat()

    @staticmethod
    def _derive_local_post_hour(uploaded_at: Optional[str], fallback_hour: Optional[int]) -> Optional[int]:
        """
        Normalize post hour to configured local timezone for optimization logic.
        """
        try:
            dt = datetime.strptime(str(uploaded_at), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            local_dt = dt.astimezone(TIMEZONE)
            return int(local_dt.hour)
        except Exception:
            try:
                return int(fallback_hour) if fallback_hour is not None else None
            except Exception:
                return None
