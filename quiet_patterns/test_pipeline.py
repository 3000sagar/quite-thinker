"""
test_pipeline.py – Generates one script, scores it, renders a video.
No upload. Just produces an MP4 to visually inspect.
"""
import sys
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("test_pipeline")

from core.database       import initialize_database
from core.script_engine  import ScriptEngine
from core.scoring_engine import ScoringEngine
from core.video_engine   import VideoEngine
from core.metadata_engine import MetadataEngine


def main() -> None:
    # 1. Init DB
    initialize_database()

    # 2. Generate a script
    script_eng = ScriptEngine()
    score_eng  = ScoringEngine()
    video_eng  = VideoEngine()
    meta_eng   = MetadataEngine()

    logger.info("Generating script...")
    script = None
    result = None
    for attempt in range(20):
        candidate = script_eng.generate()
        if candidate is None:
            continue
        result = score_eng.score(candidate)
        if score_eng.is_acceptable(result):
            script = candidate
            break
        logger.info("  attempt %d: viral=%.1f (below threshold), retrying", attempt+1, result.viral_score)

    if script is None:
        logger.error("Could not generate an acceptable script!")
        return

    logger.info("=" * 60)
    logger.info("SCRIPT ACCEPTED")
    logger.info("  Hook:    %s", script.hook)
    logger.info("  Body:    %s", script.body[:100] + "...")
    logger.info("  Closing: %s", script.closing)
    logger.info("  Words:   %d", script.word_count)
    logger.info("  Viral:   %.1f", result.viral_score)
    logger.info("  Retention: %.1f%%", result.retention_pct)
    logger.info("=" * 60)

    # 3. Generate metadata
    metadata = meta_eng.generate(script)
    logger.info("METADATA")
    logger.info("  Title: %s (%d chars)", metadata.title, len(metadata.title))
    logger.info("  Desc:  %s", metadata.description)
    logger.info("  Tags:  %s", metadata.tags)

    # 4. Render video
    logger.info("Rendering video (this may take 30–60 seconds)...")
    try:
        video_path = video_eng.render(script, output_filename="test_short.mp4")
        logger.info("=" * 60)
        logger.info("VIDEO SAVED: %s", video_path.resolve())
        logger.info("Open this file to preview the Short!")
        logger.info("=" * 60)
    except Exception as exc:
        logger.exception("Video rendering failed: %s", exc)


if __name__ == "__main__":
    main()
