"""
main.py – Entrypoint for Quiet Patterns YouTube Shorts automation system.

Modes:
  --once     Run the pipeline once immediately (generate → score → render → upload)
  --daemon   Run continuously using the APScheduler at configured post hours
  --init-db  Initialize / migrate the SQLite database only
  --auth     Trigger OAuth authentication flow only

Usage examples:
  python main.py --init-db
  python main.py --auth
  python main.py --once
  python main.py --daemon
"""

import argparse
import asyncio
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

# ─── Logging setup (before any imports that log) ─────────────────────────────
from config import (
    LOG_LEVEL,
    LOG_FORMAT,
    LOG_FILE,
    PIPELINE_MAX_SCRIPT_ATTEMPTS,
    PIPELINE_MAX_VIDEO_QA_REJECT_RETRIES,
    PIPELINE_REPORTS_ENABLED,
    PIPELINE_REPORTS_DIR,
    PIPELINE_DASHBOARD_HTML,
    ENABLE_AUTO_REWRITE,
    AUTO_REWRITE_PASSES,
    ENABLE_VARIANT_AB_TEST,
    AB_VARIANT_COUNT,
    ENABLE_UPLOAD,
    ENABLE_INSTAGRAM_UPLOAD,
    DELETE_VIDEO_AFTER_SUCCESS_UPLOAD,
    DELETE_VIDEO_ON_QA_REJECT,
)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format=LOG_FORMAT,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
logger = logging.getLogger("main")

# ─── Core imports ─────────────────────────────────────────────────────────────
from core.database       import initialize_database
from core.script_engine  import ScriptEngine
from core.scoring_engine import ScoringEngine
from core.video_engine   import VideoEngine
from core.metadata_engine import MetadataEngine
from core.uploader       import Uploader, QuotaExceededError
from core.instagram_uploader import InstagramUploader, InstagramUploadError
from core.scheduler      import SmartScheduler
from core.analytics      import AnalyticsEngine
from core.report_dashboard import build_reports_dashboard


# ─── Pipeline ────────────────────────────────────────────────────────────────

def _write_pipeline_report(payload: dict) -> Path | None:
    if not PIPELINE_REPORTS_ENABLED:
        return None
    try:
        report_dir = Path(PIPELINE_REPORTS_DIR)
        report_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = report_dir / f"pipeline_report_{stamp}.json"
        report_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        logger.info("Pipeline report written: %s", report_path)
        return report_path
    except Exception as exc:
        logger.warning("Could not write pipeline report: %s", exc)
        return None


def _refresh_dashboard() -> None:
    if not PIPELINE_REPORTS_ENABLED:
        return
    try:
        count = build_reports_dashboard(Path(PIPELINE_REPORTS_DIR), Path(PIPELINE_DASHBOARD_HTML))
        logger.info("Dashboard refreshed: %s (reports=%d)", PIPELINE_DASHBOARD_HTML, count)
    except Exception as exc:
        logger.warning("Dashboard refresh failed: %s", exc)


def _cleanup_video_file(video_path: Path, reason: str) -> bool:
    """Best-effort deletion of local rendered MP4."""
    try:
        if video_path.exists() and video_path.is_file():
            video_path.unlink()
            logger.info("Deleted local video (%s): %s", reason, video_path)
            return True
    except Exception as exc:
        logger.warning("Could not delete local video (%s): %s | %s", reason, video_path, exc)
    return False


async def run_pipeline(dry_run: bool = False) -> None:
    """
    Full end-to-end pipeline:
      1. Generate psychology script
      2. Score for retention
      3. Render vertical video
      4. Generate metadata
      5. Upload to YouTube
    """
    logger.info("=" * 60)
    logger.info("Pipeline started at %s", datetime.now().isoformat())
    if dry_run:
        logger.info("Dry-run mode enabled: upload step will be skipped.")

    script_eng  = ScriptEngine()
    score_eng   = ScoringEngine()
    video_eng   = VideoEngine()
    meta_eng    = MetadataEngine()
    uploader    = Uploader()
    ig_uploader = InstagramUploader()

    # ── 1. Generate + QA loop ────────────────────────────────────────────────
    script = None
    result = None
    threshold = score_eng.effective_threshold()
    experiment_arm = script_eng.pick_experiment_arm()
    logger.info("Using viral threshold: %.1f", threshold)
    logger.info("Using experiment arm: %s", experiment_arm)

    report_attempts: list[dict] = []
    report_payload: dict = {
        "started_at": datetime.now().isoformat(),
        "dry_run": bool(dry_run),
        "threshold": threshold,
        "experiment_arm": experiment_arm,
        "attempts": report_attempts,
        "result": {},
    }
    qa_cycle = 0
    while qa_cycle < int(PIPELINE_MAX_VIDEO_QA_REJECT_RETRIES):
        qa_cycle += 1
        script = None
        result = None
        previous_viral = 0.0

        for attempt in range(int(PIPELINE_MAX_SCRIPT_ATTEMPTS)):
            candidate = script_eng.generate(
                retry_index=attempt,
                previous_viral=previous_viral,
                experiment_arm=experiment_arm,
            )
            if candidate is None:
                report_attempts.append(
                    {
                        "qa_cycle": qa_cycle,
                        "attempt": attempt + 1,
                        "status": "no_candidate",
                        "reason": "generator_exhausted_attempt_budget",
                    }
                )
                continue
            result = score_eng.score(candidate)
            previous_viral = result.viral_score
            ok, reasons = score_eng.quality_gate(candidate, result, threshold=threshold)

            report_attempts.append(
                {
                    "qa_cycle": qa_cycle,
                    "attempt": attempt + 1,
                    "status": "accepted" if ok else "rejected",
                    "viral_score": result.viral_score,
                    "retention_pct": result.retention_pct,
                    "word_count": candidate.word_count,
                    "theme": getattr(candidate, "_theme", ""),
                    "experiment_arm": getattr(candidate, "_experiment_arm", experiment_arm),
                    "hook": candidate.hook,
                    "reasons": reasons,
                }
            )
            if ok:
                script = candidate
                break

            if ENABLE_AUTO_REWRITE:
                for rewrite_pass in range(max(1, int(AUTO_REWRITE_PASSES))):
                    rewritten = script_eng.rewrite_candidate(
                        candidate,
                        reasons=reasons,
                        score_hint={
                            "emotional_score": result.emotional_score,
                            "identity_score": result.identity_score,
                            "rhythm_score": result.rhythm_score,
                            "loop_score": result.loop_score,
                            "curiosity_score": result.curiosity_score,
                        },
                        attempt_index=rewrite_pass,
                    )
                    rewrite_result = score_eng.score(rewritten)
                    setattr(rewritten, "_experiment_arm", experiment_arm)
                    previous_viral = max(previous_viral, rewrite_result.viral_score)
                    rewrite_ok, rewrite_reasons = score_eng.quality_gate(rewritten, rewrite_result, threshold=threshold)
                    report_attempts.append(
                        {
                            "qa_cycle": qa_cycle,
                            "attempt": f"{attempt + 1}.r{rewrite_pass + 1}",
                            "status": "accepted" if rewrite_ok else "rejected",
                            "viral_score": rewrite_result.viral_score,
                            "retention_pct": rewrite_result.retention_pct,
                            "word_count": rewritten.word_count,
                            "theme": getattr(rewritten, "_theme", ""),
                            "experiment_arm": getattr(rewritten, "_experiment_arm", experiment_arm),
                            "hook": rewritten.hook,
                            "reasons": rewrite_reasons,
                            "source": "rewrite",
                        }
                    )
                    if rewrite_ok:
                        script = rewritten
                        result = rewrite_result
                        logger.info(
                            "Script rescued via rewrite pass %d (viral=%.1f)",
                            rewrite_pass + 1, rewrite_result.viral_score
                        )
                        break
                if script is not None:
                    break

            if reasons:
                logger.info("Quality gate failed: %s", ", ".join(reasons))
            logger.info("Script rejected (viral=%.1f), retrying...", result.viral_score)

        if script is None or result is None:
            logger.error("Could not generate an acceptable script in QA cycle %d.", qa_cycle)
            continue

        logger.info(
            "Script accepted: %d words, viral=%.1f, retention=%.1f%%",
            script.word_count, result.viral_score, result.retention_pct
        )

        if ENABLE_VARIANT_AB_TEST:
            variants = script_eng.build_ab_variants(script, count=max(1, int(AB_VARIANT_COUNT)))
            best_script = script
            best_result = result
            for ix, variant in enumerate(variants, start=1):
                vr = score_eng.score(variant)
                is_ok, _ = score_eng.quality_gate(variant, vr, threshold=threshold)
                if is_ok and vr.viral_score > best_result.viral_score:
                    best_script = variant
                    best_result = vr
                report_attempts.append(
                    {
                        "qa_cycle": qa_cycle,
                        "attempt": f"ab.{ix}",
                        "status": "candidate",
                        "viral_score": vr.viral_score,
                        "retention_pct": vr.retention_pct,
                        "word_count": variant.word_count,
                        "experiment_arm": getattr(variant, "_experiment_arm", experiment_arm),
                        "hook": variant.hook,
                        "source": "ab_variant",
                    }
                )
            if best_script.content_hash != script.content_hash:
                logger.info(
                    "A/B selected stronger variant: viral %.1f -> %.1f",
                    result.viral_score, best_result.viral_score
                )
                script = best_script
                result = best_result

        qa_preview = video_eng.preview_plan(script)
        report_payload["video_qa_preflight"] = qa_preview
        report_attempts.append(
            {
                "qa_cycle": qa_cycle,
                "attempt": "video_qa_preflight",
                "status": "accepted" if qa_preview.get("passed", False) else "rejected",
                "reasons": qa_preview.get("reasons", []),
                "metrics": qa_preview.get("metrics", {}),
                "duration_sec": qa_preview.get("duration_sec", 0),
            }
        )
        if qa_preview.get("passed", False):
            report_payload["result"] = {
                "status": "script_selected",
                "experiment_arm": getattr(script, "_experiment_arm", experiment_arm),
                "viral_score": result.viral_score,
                "retention_pct": result.retention_pct,
                "word_count": script.word_count,
                "qa_cycle": qa_cycle,
                "finished_at": datetime.now().isoformat(),
            }
            _write_pipeline_report(report_payload)
            _refresh_dashboard()
            break

        logger.warning(
            "Video QA preflight rejected cycle %d/%d: %s",
            qa_cycle, int(PIPELINE_MAX_VIDEO_QA_REJECT_RETRIES), qa_preview.get("reasons", [])
        )

    if script is None or result is None or not report_payload.get("video_qa_preflight", {}).get("passed", False):
        logger.error("Could not produce a QA-worthy video candidate after %d cycles. Aborting pipeline.", int(PIPELINE_MAX_VIDEO_QA_REJECT_RETRIES))
        report_payload["result"] = {
            "status": "failed",
            "reason": "video_qa_preflight_exhausted",
            "finished_at": datetime.now().isoformat(),
        }
        _write_pipeline_report(report_payload)
        _refresh_dashboard()
        return

    # ── 2. Save script to DB ─────────────────────────────────────────────────
    script_id = script_eng.save(script, result.viral_score, result.retention_pct)

    # ── 3. Render video ───────────────────────────────────────────────────────
    logger.info("Rendering video...")
    try:
        video_path = video_eng.render(script)
    except RuntimeError as exc:
        logger.error("Video rendering failed: %s", exc)
        report_payload["result"] = {
            "status": "failed",
            "reason": "video_render_failed",
            "error": str(exc),
            "finished_at": datetime.now().isoformat(),
        }
        _write_pipeline_report(report_payload)
        _refresh_dashboard()
        return

    # Save video record
    from core.database import execute_write
    video_id = execute_write(
        "INSERT INTO videos (script_id, file_path, duration_sec) VALUES (?, ?, ?)",
        (script_id, str(video_path), 24.0)  # approximate; actual set by renderer
    )

    # ── 4. Generate metadata ──────────────────────────────────────────────────
    metadata = meta_eng.generate(script)
    logger.info("Metadata: title='%s', tags=%s", metadata.title, metadata.tags)

    # ── 5. Upload (YouTube + Instagram) ──────────────────────────────────────
    upload_status = "skipped"  # backward-compatible summary (mirrors YouTube status)
    youtube_status = "skipped"
    instagram_status = "disabled"
    yt_id = None
    ig_media_id = None
    ig_permalink = None
    qa_final = getattr(video_eng, "_last_qa", None) or {}
    report_payload["video_qa_final"] = qa_final
    if qa_final and not qa_final.get("passed", False):
        logger.warning("Upload skipped — video QA failed: %s", qa_final.get("reasons", []))
        youtube_status = "qa_rejected"
        instagram_status = "qa_rejected"
    elif dry_run:
        logger.info("Upload skipped (dry_run=True).")
        youtube_status = "disabled"
        instagram_status = "disabled"
    else:
        if ENABLE_UPLOAD:
            try:
                uploader.authenticate()
                yt_id = uploader.upload(video_path, metadata, video_id)
                logger.info("Successfully uploaded: https://youtube.com/shorts/%s", yt_id)
                youtube_status = "uploaded"
            except QuotaExceededError as exc:
                logger.warning("YouTube upload skipped — quota exceeded: %s", exc)
                youtube_status = "quota_exceeded"
            except FileNotFoundError as exc:
                logger.error("OAuth client secrets not found: %s", exc)
                logger.error("Please complete OAuth setup first — see README.md")
                youtube_status = "oauth_missing"
            except Exception as exc:
                logger.exception("YouTube upload failed: %s", exc)
                youtube_status = f"error:{type(exc).__name__}"
        else:
            youtube_status = "disabled"

        if ENABLE_INSTAGRAM_UPLOAD:
            try:
                ig_result = ig_uploader.upload(video_path, metadata, video_id)
                instagram_status = str(ig_result.get("status", "unknown"))
                ig_media_id = ig_result.get("media_id")
                ig_permalink = ig_result.get("permalink")
                if instagram_status == "published":
                    logger.info("Successfully published to Instagram: %s", ig_permalink or ig_media_id)
                elif instagram_status == "not_configured":
                    logger.warning("Instagram upload enabled but not fully configured in config.py.")
                else:
                    logger.info("Instagram upload result: %s", instagram_status)
            except InstagramUploadError as exc:
                logger.warning("Instagram upload failed: %s", exc)
                instagram_status = "error:InstagramUploadError"
            except Exception as exc:
                logger.exception("Instagram upload failed: %s", exc)
                instagram_status = f"error:{type(exc).__name__}"
        else:
            instagram_status = "disabled"

    upload_status = youtube_status

    deleted_local_video = False
    if upload_status == "qa_rejected" and DELETE_VIDEO_ON_QA_REJECT:
        deleted_local_video = _cleanup_video_file(video_path, "qa_rejected")
    else:
        enabled_platforms: list[tuple[str, bool]] = []
        if ENABLE_UPLOAD:
            enabled_platforms.append(("youtube", youtube_status == "uploaded"))
        if ENABLE_INSTAGRAM_UPLOAD:
            enabled_platforms.append(("instagram", instagram_status == "published"))
        if enabled_platforms and DELETE_VIDEO_AFTER_SUCCESS_UPLOAD and all(ok for _, ok in enabled_platforms):
            deleted_local_video = _cleanup_video_file(video_path, "all_enabled_uploads_completed")

    report_payload["result"] = {
        "status": "completed",
        "experiment_arm": getattr(script, "_experiment_arm", experiment_arm),
        "script_id": script_id,
        "video_id": video_id,
        "video_path": str(video_path),
        "viral_score": result.viral_score,
        "retention_pct": result.retention_pct,
        "video_qa_passed": bool(qa_final.get("passed", False)),
        "video_qa_reasons": qa_final.get("reasons", []),
        "upload_status": upload_status,
        "youtube_status": youtube_status,
        "instagram_status": instagram_status,
        "local_video_deleted": deleted_local_video,
        "youtube_id": yt_id,
        "instagram_media_id": ig_media_id,
        "instagram_permalink": ig_permalink,
        "finished_at": datetime.now().isoformat(),
    }
    _write_pipeline_report(report_payload)
    _refresh_dashboard()

    logger.info("Pipeline completed.")
    logger.info("=" * 60)


# ─── Analytics pull ───────────────────────────────────────────────────────────

def run_analytics() -> None:
    """Fetch pending analytics from YouTube; safe to call anytime."""
    try:
        engine = AnalyticsEngine()
        engine.authenticate()
        engine.fetch_pending_analytics()
        features = engine.get_best_script_features()
        hours    = engine.get_optimal_post_hours()
        readiness = engine.get_retention_readiness()
        arms = engine.get_experiment_arm_performance()
        logger.info(
            "Retention readiness: %s (positive=%d/%d required=%d, hour_coverage=%d required=%d, avg_positive_retention=%.2f)",
            readiness["status"],
            readiness["positive_rows"],
            readiness["total_rows"],
            readiness["required_positive_rows"],
            readiness["hour_coverage"],
            readiness["required_hour_coverage"],
            readiness["avg_positive_retention"],
        )
        logger.info("Analytics summary: features=%s, optimal_hours=%s", features, hours)
        if arms:
            logger.info("Experiment arms: %s", arms)
        else:
            logger.info("Experiment arms: no samples yet.")
    except Exception as exc:
        logger.warning("Analytics run failed: %s", exc)


def run_dashboard() -> None:
    """Build local HTML dashboard from pipeline reports."""
    _refresh_dashboard()


def run_backfill_hours() -> None:
    """Backfill historical uploads.post_hour to configured local timezone."""
    try:
        engine = AnalyticsEngine()
        changed = engine.backfill_upload_post_hours_local()
        logger.info("Backfill complete. Rows updated: %d", changed)
    except Exception as exc:
        logger.warning("Backfill failed: %s", exc)


# ─── CLI ──────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Quiet Patterns – YouTube Shorts Automation"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--init-db",  action="store_true", help="Initialize the SQLite database")
    group.add_argument("--auth",     action="store_true", help="Run OAuth authentication flow")
    group.add_argument("--once",     action="store_true", help="Run pipeline once immediately")
    group.add_argument("--daemon",   action="store_true", help="Run continuously via scheduler")
    group.add_argument("--analytics",action="store_true", help="Fetch analytics for recent uploads")
    group.add_argument("--dashboard",action="store_true", help="Build local HTML dashboard from reports")
    group.add_argument("--backfill-hours", action="store_true", help="Repair historical upload post_hour values to local timezone")
    parser.add_argument("--dry-run", action="store_true", help="Generate/render/metadata only; skip upload")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Always ensure DB is initialized
    initialize_database()

    if args.init_db:
        logger.info("Database initialized successfully.")
        return

    if args.auth:
        uploader = Uploader()
        uploader.authenticate()
        logger.info("Authentication complete. Token saved.")
        return

    if args.once:
        asyncio.run(run_pipeline(dry_run=args.dry_run))
        return

    if args.analytics:
        run_analytics()
        return

    if args.dashboard:
        run_dashboard()
        return

    if args.backfill_hours:
        run_backfill_hours()
        return

    if args.daemon:
        logger.info("Starting daemon mode — Ctrl+C to stop.")

        async def daemon_main() -> None:
            scheduler = SmartScheduler(run_pipeline)
            await scheduler.start()

        asyncio.run(daemon_main())


if __name__ == "__main__":
    main()
