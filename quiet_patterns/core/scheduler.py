"""
core/scheduler.py – Intelligent APScheduler-based posting scheduler.

Behaviour:
  • Runs the full pipeline (generate → score → render → upload) at post hours
  • Default schedule: 13:00 and 19:00 local time (Asia/Kolkata by default)
  • After UPLOADS_BEFORE_OPTIMIZE uploads: pulls optimal hours from AnalyticsEngine
    and rebuilds the schedule dynamically
  • Uses AsyncIOScheduler for asyncio compatibility
"""

import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Callable, Coroutine, Any
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import (
    DEFAULT_POST_HOURS, TIMEZONE, UPLOADS_BEFORE_OPTIMIZE
)
from core.database import execute_query

logger = logging.getLogger(__name__)

try:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.cron import CronTrigger
    _APScheduler_OK = True
except ImportError:
    _APScheduler_OK = False
    logger.error("APScheduler not installed — pip install apscheduler")


class SmartScheduler:
    """
    Wraps APScheduler with dynamic hour optimization.

    Usage:
        scheduler = SmartScheduler(pipeline_coroutine_fn)
        await scheduler.start()
        # Runs indefinitely until stopped
        await scheduler.stop()
    """

    JOB_ID_PREFIX = "post_hour_"

    def __init__(self, pipeline_fn: Callable[[], Coroutine[Any, Any, None]]) -> None:
        if not _APScheduler_OK:
            raise RuntimeError("APScheduler not installed.")
        self._pipeline_fn = pipeline_fn
        self._scheduler   = AsyncIOScheduler(timezone=TIMEZONE)
        self._post_hours  = list(DEFAULT_POST_HOURS)
        logger.info("SmartScheduler initialized with hours: %s", self._post_hours)

    async def start(self) -> None:
        """Start the scheduler and block until stopped (via asyncio signals)."""
        self._apply_schedule(self._post_hours)
        # Optimization check every day at midnight
        self._scheduler.add_job(
            self._maybe_optimize,
            CronTrigger(hour=0, minute=5, timezone=TIMEZONE),
            id="daily_optimize",
            replace_existing=True,
        )
        self._scheduler.start()
        logger.info("Scheduler started. Active jobs: %s", self._scheduler.get_jobs())
        try:
            # Keep the event loop alive
            while True:
                await asyncio.sleep(60)
        except (asyncio.CancelledError, KeyboardInterrupt):
            logger.info("Scheduler stopping…")
            self._scheduler.shutdown(wait=False)

    async def stop(self) -> None:
        if self._scheduler.running:
            self._scheduler.shutdown(wait=False)
            logger.info("Scheduler stopped.")

    # ── Internal ─────────────────────────────────────────────────────────────

    def _apply_schedule(self, hours: list[int]) -> None:
        """Remove old posting jobs and re-add for the given hours."""
        for job in self._scheduler.get_jobs():
            if job.id.startswith(self.JOB_ID_PREFIX):
                job.remove()

        for hour in hours:
            job_id = f"{self.JOB_ID_PREFIX}{hour:02d}"
            self._scheduler.add_job(
                self._run_pipeline_safe,
                CronTrigger(hour=hour, minute=0, timezone=TIMEZONE),
                id=job_id,
                replace_existing=True,
                misfire_grace_time=300,
            )
            logger.info("Scheduled pipeline job at %02d:00 (job_id=%s)", hour, job_id)

    async def _run_pipeline_safe(self) -> None:
        """Wrapper that catches all exceptions so the scheduler stays alive."""
        logger.info("Pipeline triggered at %s", datetime.now(TIMEZONE).isoformat())
        try:
            await self._pipeline_fn()
        except Exception as exc:
            logger.exception("Pipeline run failed: %s", exc)

    async def _maybe_optimize(self) -> None:
        """Re-evaluate best posting hours after enough uploads accumulate."""
        total = execute_query("SELECT COUNT(*) as cnt FROM uploads", ())[0]["cnt"]
        if total < UPLOADS_BEFORE_OPTIMIZE:
            return
        try:
            from core.analytics import AnalyticsEngine
            engine = AnalyticsEngine()
            engine.authenticate()
            engine.fetch_pending_analytics()
            optimal = engine.get_optimal_post_hours()
            if sorted(optimal) != sorted(self._post_hours):
                logger.info(
                    "Updating post schedule: %s → %s", self._post_hours, optimal
                )
                self._post_hours = optimal
                self._apply_schedule(optimal)
        except Exception as exc:
            logger.warning("Hour optimization failed: %s", exc)
