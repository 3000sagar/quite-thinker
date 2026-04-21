"""
core/video_engine.py – Renders vertical 1080×1920 Shorts using moviepy + ffmpeg.

Features:
  • Deep charcoal background (#0f1115)
  • Programmatic particle noise overlay (no external assets)
  • Animated gradient color shift across frames
  • Kinetic minimal typography with soft fade transitions
  • 3% zoom on final closing line
  • Smooth loop: fade-back of first line at end
  • Silent MP4 export (H.264, 30 fps)
"""

import logging
import math
import os
import random
import re
import shutil
import subprocess
from pathlib import Path
from typing import Optional
import sys

import numpy as np
from PIL import Image as PILImage, ImageDraw, ImageFont

# Pillow>=10 removed ANTIALIAS; moviepy still references it in some paths.
if not hasattr(PILImage, "ANTIALIAS") and hasattr(PILImage, "Resampling"):
    PILImage.ANTIALIAS = PILImage.Resampling.LANCZOS

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import (
    VIDEO_WIDTH, VIDEO_HEIGHT, VIDEO_FPS,
    VIDEO_CODEC, VIDEO_BITRATE, VIDEO_DURATION_MIN, VIDEO_DURATION_MAX,
    VIDEO_QA_ENABLED, VIDEO_QA_MIN_DURATION_SEC, VIDEO_QA_MAX_DURATION_SEC,
    VIDEO_QA_HOOK_MAX_START_SEC, VIDEO_QA_HOOK_MIN_DURATION_SEC,
    VIDEO_QA_MAX_WORDS_PER_LINE, VIDEO_QA_MAX_CHARS_PER_LINE,
    VIDEO_QA_MIN_SEGMENT_SEC, VIDEO_QA_MAX_SEGMENT_SEC, VIDEO_QA_MAX_AVG_SEGMENT_SEC,
    VIDEO_QA_PAYOFF_MIN_SHARE, VIDEO_QA_PAYOFF_MAX_SHARE,
    OUTPUT_DIR, PREFERRED_FONTS, MUSIC_DIR, ENABLE_BG_MUSIC, MUSIC_VOLUME,
    FONT_SIZE_HOOK, FONT_SIZE_BODY, FONT_SIZE_CLOSE,
    FONT_COLOR, FONT_DIM_COLOR,
    ASSETS_DIR, PRIMARY_CAPTION_FONT, PRIMARY_CAPTION_FONT_FILE,
)
from core.script_engine import Script

logger = logging.getLogger(__name__)
VIDEO_BG_DIR = ASSETS_DIR / "video"

# ─── Optional moviepy import with helpful error ───────────────────────────────
try:
    from moviepy.config import change_settings
    from moviepy.editor import (
        VideoClip, VideoFileClip, ImageClip, CompositeVideoClip, AudioFileClip,
        concatenate_videoclips, afx, vfx,
    )
    try:
        from moviepy.video.fx import fadein, fadeout
    except ImportError:
        from moviepy.video.fx.fadein import fadein
        from moviepy.video.fx.fadeout import fadeout

    # Ensure TextClip uses ImageMagick (and not Windows convert.exe).
    _magick = shutil.which("magick.exe") or shutil.which("magick")
    if not _magick:
        program_files = [
            os.environ.get("ProgramFiles", r"C:\Program Files"),
            os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)"),
        ]
        for base in program_files:
            try:
                root = Path(base)
                for candidate in sorted(root.glob("ImageMagick*\\magick.exe"), reverse=True):
                    if candidate.exists():
                        _magick = str(candidate)
                        break
                if _magick:
                    break
            except Exception:
                continue
    if _magick:
        change_settings({"IMAGEMAGICK_BINARY": _magick})

    _MOVIEPY_OK = True
except ImportError:
    _MOVIEPY_OK = False
    logger.error("moviepy not installed — pip install moviepy")


# ─── Colour helpers ───────────────────────────────────────────────────────────

def _hex_to_rgb(h: str) -> tuple[int, int, int]:
    h = h.lstrip("#")
    return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)


BG_RGB = _hex_to_rgb("#0f1115")   # (15, 17, 21)


def _list_available_fonts() -> set[str]:
    """Read available ImageMagick font names."""
    commands = [
        ["magick", "-list", "font"],
        ["convert", "-list", "font"],
    ]
    for cmd in commands:
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=8)
            if result.returncode != 0 or not result.stdout:
                continue
            fonts: set[str] = set()
            for line in result.stdout.splitlines():
                line = line.strip()
                if line.lower().startswith("font:"):
                    font_name = line.split(":", 1)[1].strip()
                    if font_name:
                        fonts.add(font_name)
            if fonts:
                return fonts
        except Exception:
            continue
    return set()


def _match_available_font(candidates: list[str], available: set[str]) -> Optional[str]:
    """Return first candidate found in available fonts (case-insensitive)."""
    if not available:
        return None
    lower_map = {f.lower(): f for f in available}
    for candidate in candidates:
        if candidate.lower() in lower_map:
            return lower_map[candidate.lower()]
    return None


def _find_font() -> str:
    """Return the first preferred font available to ImageMagick/moviepy."""
    for font in PREFERRED_FONTS:
        try:
            result = subprocess.run(
                ["convert", "-list", "font"],
                capture_output=True, text=True, timeout=5
            )
            if font.lower() in result.stdout.lower():
                return font
        except Exception:
            pass
    return "DejaVu-Sans"   # universal fallback


# ─── Background frame generator ──────────────────────────────────────────────

def _make_bg_frame(t: float, duration: float) -> np.ndarray:
    """
    Generate a single background frame:
    - Deep charcoal base
    - Subtle amplitude-modulated gradient shift (bluish tint drifts slowly)
    - Sparse white pixel noise at very low opacity
    """
    w, h = VIDEO_WIDTH, VIDEO_HEIGHT
    frame = np.zeros((h, w, 3), dtype=np.uint8)

    # Base colour
    frame[:, :] = BG_RGB

    # Gradient shift: a soft vertical gradient that slowly oscillates
    progress = t / max(duration, 1)
    intensity = 0.5 + 0.5 * math.sin(2 * math.pi * progress)  # 0..1 cycle
    for row in range(h):
        frac = row / h
        r_add = int(3 * frac * intensity)
        b_add = int(8 * (1 - frac) * intensity)
        frame[row, :, 0] = min(255, BG_RGB[0] + r_add)
        frame[row, :, 2] = min(255, BG_RGB[2] + b_add)

    # Sparse particle noise
    n_particles = 80
    rng = random.Random(int(t * 10))   # deterministic per-frame seed
    for _ in range(n_particles):
        px = rng.randint(0, w - 1)
        py = rng.randint(0, h - 1)
        val = rng.randint(30, 70)
        frame[py, px] = (val, val, val)

    return frame


# ─── Text segment definition ──────────────────────────────────────────────────

class _Segment:
    def __init__(
        self, text: str, start: float, end: float,
        font_size: int, color: str, font: str,
        zoom_factor: float = 1.0,
        role: str = "body",
    ) -> None:
        self.text        = text
        self.start       = start
        self.end         = end
        self.font_size   = font_size
        self.color       = color
        self.font        = font
        self.zoom_factor = zoom_factor
        self.role        = role

    def make_clip(self, width: int, height: int) -> Optional[object]:
        """Build a centered text clip using Pillow-rendered RGBA image."""
        if not _MOVIEPY_OK:
            return None
        duration = self.end - self.start
        fade_in_sec = min(0.45, max(0.2, duration * 0.22))
        fade_out_sec = min(0.40, max(0.18, duration * 0.20))
        try:
            font_obj = ImageFont.truetype(self.font, self.font_size)
            draw_probe = ImageDraw.Draw(PILImage.new("RGBA", (10, 10), (0, 0, 0, 0)))
            spacing = max(6, int(self.font_size * 0.28))
            safe_text_width = int(width * 0.82)  # strong side margins
            wrapped_text = self._wrap_text_to_width(self.text, font_obj, draw_probe, safe_text_width)
            bbox = draw_probe.multiline_textbbox(
                (0, 0), wrapped_text, font=font_obj, align="center", spacing=spacing
            )
            text_w = max(1, bbox[2] - bbox[0])
            text_h = max(1, bbox[3] - bbox[1])
            pad_x = max(16, int(self.font_size * 0.40))
            pad_y = max(12, int(self.font_size * 0.28))
            canvas_w = int(min(int(width * 0.90), text_w + pad_x * 2))
            canvas_h = int(text_h + pad_y * 2)
            img = PILImage.new("RGBA", (canvas_w, canvas_h), (0, 0, 0, 0))
            draw = ImageDraw.Draw(img)
            fill = self.color if self.color.startswith("#") else "#e8e8e8"
            draw.multiline_text(
                (canvas_w // 2, pad_y),
                wrapped_text,
                font=font_obj,
                fill=fill,
                anchor="ma",
                align="center",
                spacing=spacing,
            )
            clip = ImageClip(np.array(img)).set_start(self.start).set_duration(duration)
            # Fade text opacity (mask) so it fades transparently, not to black.
            if clip.mask is not None:
                mask = clip.mask.fx(fadein.fadein, fade_in_sec).fx(fadeout.fadeout, fade_out_sec)
                clip = clip.set_mask(mask)
            else:
                clip = clip.fx(fadein.fadein, fade_in_sec).fx(fadeout.fadeout, fade_out_sec)
            # Center on screen
            clip = clip.set_position(("center", "center"))
            return clip
        except Exception as exc:
            logger.exception("Failed to create TextClip for '%s': %s", self.text[:30], exc)
            return None

    @staticmethod
    def _wrap_text_to_width(text: str, font_obj: ImageFont.FreeTypeFont, draw_probe: ImageDraw.ImageDraw, max_width: int) -> str:
        """Wrap by pixel width and avoid tiny orphan ending lines."""
        paragraphs = [p.strip() for p in text.split("\n")]
        wrapped_paragraphs: list[str] = []
        for para in paragraphs:
            if not para:
                continue
            words = para.split()
            if not words:
                continue
            lines: list[str] = []
            current = words[0]
            for word in words[1:]:
                candidate = f"{current} {word}"
                b = draw_probe.textbbox((0, 0), candidate, font=font_obj)
                if (b[2] - b[0]) <= max_width:
                    current = candidate
                else:
                    lines.append(current)
                    current = word
            lines.append(current)

            # Rebalance: avoid last line with 1-2 words when previous is long.
            if len(lines) >= 2:
                last_words = lines[-1].split()
                prev_words = lines[-2].split()
                if len(last_words) <= 2 and len(prev_words) >= 5:
                    moved = prev_words[-1:] if len(prev_words) == 5 else prev_words[-2:]
                    lines[-2] = " ".join(prev_words[:-len(moved)])
                    lines[-1] = " ".join(moved + last_words)
                    lines = [ln for ln in lines if ln.strip()]

            wrapped_paragraphs.append("\n".join(lines))

        return "\n".join(wrapped_paragraphs).strip() or text


# ─── Main engine ─────────────────────────────────────────────────────────────

class VideoEngine:
    """Renders a Script object into a vertical MP4 Short."""

    def __init__(self) -> None:
        self._available_fonts = _list_available_fonts()
        preferred = _match_available_font([PRIMARY_CAPTION_FONT], self._available_fonts)
        font_file_value = str(PRIMARY_CAPTION_FONT_FILE or "").strip()
        font_file = Path(font_file_value) if font_file_value else None
        if font_file and font_file.is_file():
            self._font = str(font_file)
        elif preferred:
            self._font = preferred
        else:
            self._font = _find_font()
            logger.warning(
                "Requested font '%s' not found. Install Benguiat or set PRIMARY_CAPTION_FONT_FILE. Using %s.",
                PRIMARY_CAPTION_FONT, self._font
            )
        self._last_qa: dict | None = None
        logger.info("VideoEngine using font: %s", self._font)

    def preview_plan(self, script: Script) -> dict:
        """Build segment plan and return QA report before expensive rendering."""
        text_font, _ = self._choose_context_font(script)
        segments, total_duration = self._build_segments(script, text_font)
        qa = self._assess_segments(segments, total_duration)
        self._last_qa = qa
        return qa

    def render(
        self,
        script: Script,
        output_filename: Optional[str] = None,
        planned: Optional[tuple[list["_Segment"], float]] = None,
    ) -> Path:
        """
        Render script to MP4 and return its Path.
        Raises RuntimeError if moviepy is not available.
        """
        if not _MOVIEPY_OK:
            raise RuntimeError("moviepy is required: pip install moviepy")

        # Determine output path
        if output_filename is None:
            safe_name = re.sub(r"\W+", "_", script.hook[:30]).strip("_").lower()
            output_filename = f"{safe_name}.mp4"
        out_path = OUTPUT_DIR / output_filename

        # Split script into visual segments
        text_font, tone = self._choose_context_font(script)
        logger.info("Context font selected: %s (tone=%s)", text_font, tone)
        if planned is not None:
            segments, total_duration = planned
        else:
            segments, total_duration = self._build_segments(script, text_font)

        logger.info(
            "Rendering video: %d segments, %.1fs duration -> %s",
            len(segments), total_duration, out_path
        )
        qa = self._assess_segments(segments, total_duration)
        self._last_qa = qa
        if VIDEO_QA_ENABLED and not qa.get("passed", False):
            reasons = ", ".join(qa.get("reasons", [])) or "unknown_video_qa_failure"
            raise RuntimeError(f"Video QA rejected plan: {reasons}")

        # Background clip (2-3 random files from assets/video when available).
        bg_clip = self._build_background_clip(total_duration)

        # Text clips
        text_clips = [seg.make_clip(VIDEO_WIDTH, VIDEO_HEIGHT) for seg in segments]
        text_clips = [c for c in text_clips if c is not None]
        if not text_clips:
            raise RuntimeError(
                "Text rendering unavailable. Install and configure ImageMagick so moviepy TextClip can run."
            )

        # Composite
        all_clips = [bg_clip] + text_clips
        composite = CompositeVideoClip(
            all_clips, size=(VIDEO_WIDTH, VIDEO_HEIGHT)
        ).set_fps(VIDEO_FPS)

        # Optional royalty-free background audio
        audio_clip = self._build_audio(total_duration)
        if audio_clip is not None:
            composite = composite.set_audio(audio_clip)

        # Export (with audio when a music track is available)
        composite.write_videofile(
            str(out_path),
            fps=VIDEO_FPS,
            codec=VIDEO_CODEC,
            bitrate=VIDEO_BITRATE,
            audio=audio_clip is not None,
            logger=None,          # suppress moviepy progress bars
            threads=2,
        )

        logger.info("Video rendered successfully: %s (%.1fs)", out_path, total_duration)
        return out_path

    def _build_background_clip(self, total_duration: float):
        """
        Stitch 2-3 random clips from assets/video for a simple dynamic background.
        Falls back to procedural background if no usable files are found.
        """
        patterns = ("*.mp4", "*.mov", "*.m4v", "*.webm")
        bg_files: list[Path] = []
        for pattern in patterns:
            bg_files.extend(VIDEO_BG_DIR.glob(pattern))

        if not bg_files:
            logger.warning("No background videos found in %s. Using procedural background.", VIDEO_BG_DIR)
            return VideoClip(
                lambda t: _make_bg_frame(t, total_duration),
                duration=total_duration
            ).set_fps(VIDEO_FPS)

        clip_count = min(len(bg_files), random.randint(3, 4))
        selected = random.sample(bg_files, k=clip_count)
        logger.info("Background video selection: %s", ", ".join(p.name for p in selected))

        segment_duration = total_duration / float(clip_count)
        stitched = []
        for path in selected:
            try:
                clip = VideoFileClip(str(path))
                if clip.duration > segment_duration:
                    max_start = max(0.0, clip.duration - segment_duration)
                    start = random.uniform(0.0, max_start) if max_start > 0 else 0.0
                    clip = clip.subclip(start, start + segment_duration)
                else:
                    clip = clip.subclip(0, min(clip.duration, segment_duration))

                clip = self._fit_vertical(clip).fx(vfx.colorx, 0.72)
                stitched.append(clip)
            except Exception as exc:
                logger.warning("Skipping background clip '%s': %s", path, exc)

        if not stitched:
            logger.warning("All background clips failed. Using procedural background.")
            return VideoClip(
                lambda t: _make_bg_frame(t, total_duration),
                duration=total_duration
            ).set_fps(VIDEO_FPS)

        background = concatenate_videoclips(stitched, method="compose")
        if background.duration < total_duration:
            background = background.loop(duration=total_duration)
        else:
            background = background.subclip(0, total_duration)
        return background.set_fps(VIDEO_FPS)

    @staticmethod
    def _fit_vertical(clip):
        """Center-crop clip to 1080x1920 without resize (avoids PIL ANTIALIAS path)."""
        src_w, src_h = clip.w, clip.h
        if src_w < VIDEO_WIDTH or src_h < VIDEO_HEIGHT:
            # Extremely small clips are returned unchanged to avoid resize code paths.
            return clip

        return clip.crop(
            x_center=src_w / 2,
            y_center=src_h / 2,
            width=VIDEO_WIDTH,
            height=VIDEO_HEIGHT,
        )

    def _build_audio(self, duration: float) -> Optional[object]:
        """Load and trim/loop a random royalty-free track from assets/music."""
        if not ENABLE_BG_MUSIC or not _MOVIEPY_OK:
            return None
        patterns = ("*.mp3", "*.wav", "*.m4a", "*.aac")
        music_files: list[Path] = []
        for pattern in patterns:
            music_files.extend(MUSIC_DIR.glob(pattern))
        if not music_files:
            logger.warning("No music files found in %s. Rendering without background music.", MUSIC_DIR)
            return None

        track = random.choice(music_files)
        try:
            clip = AudioFileClip(str(track))
            if clip.duration < duration:
                clip = afx.audio_loop(clip, duration=duration)
            else:
                clip = clip.subclip(0, duration)
            clip = clip.volumex(MUSIC_VOLUME)
            logger.info("Background music attached: %s", track.name)
            return clip
        except Exception as exc:
            logger.warning("Could not load music track '%s': %s", track, exc)
            return None

    def _choose_context_font(self, script: Script) -> tuple[str, str]:
        """Use one consistent font style for cleaner brand look."""
        return self._font, "locked"

    # ── Segment builder ───────────────────────────────────────────────────────

    def _build_segments(self, script: Script, active_font: str, forced_duration: Optional[float] = None) -> tuple[list[_Segment], float]:
        """
        Allocate on-screen timing for each part of the script.

        Timeline example (24-second video):
          0.0 – 2.5s   hook
          2.5 – 12.0s  body (multi-line, staggered)
          12.0 – 21.5s closing (with 3% zoom)
          21.5 – 24.0s loop echo: hook faded in dimly
        """
        if forced_duration is not None:
            total_dur = float(forced_duration)
        else:
            total_dur = float(random.randint(VIDEO_DURATION_MIN, VIDEO_DURATION_MAX))
        segments: list[_Segment] = []

        t = 0.0
        hook_end   = min(2.5, total_dur * 0.10)
        body_end   = total_dur * 0.80
        close_end  = total_dur * 0.92
        echo_end   = total_dur

        # Hook
        segments.append(_Segment(
            text=script.hook, start=t, end=hook_end,
            font_size=FONT_SIZE_HOOK, color=FONT_COLOR, font=active_font,
            role="hook",
        ))
        t = hook_end + 0.1

        # Body – enforce concise frame chunks (1–2 lines per frame).
        body_chunks = self._chunk_body_for_frames(script.body)
        if not body_chunks:
            body_chunks = [script.body.strip()]
        body_total = max(0.1, body_end - t)
        durations = self._allocate_chunk_durations(body_chunks, body_total)
        cursor = t
        for sentence, dur in zip(body_chunks, durations):
            seg_start = cursor
            seg_end = min(seg_start + dur, body_end)
            segments.append(_Segment(
                text=sentence, start=seg_start, end=seg_end,
                font_size=FONT_SIZE_BODY, color=FONT_COLOR, font=active_font,
                role="body",
            ))
            cursor = seg_end

        t = body_end + 0.1

        # Closing line (slight zoom = 1.03)
        segments.append(_Segment(
            text=script.closing, start=t, end=close_end,
            font_size=FONT_SIZE_CLOSE, color=FONT_COLOR, font=active_font,
            zoom_factor=1.03,
            role="closing",
        ))

        # Loop echo: dim repeat of hook
        if close_end < echo_end - 0.5:
            segments.append(_Segment(
                text=script.hook, start=close_end + 0.2, end=echo_end,
                font_size=FONT_SIZE_HOOK - 12, color=FONT_DIM_COLOR, font=active_font,
                role="echo",
            ))

        return segments, total_dur

    @staticmethod
    def _line_stats(text: str) -> tuple[int, int]:
        lines = [ln.strip() for ln in (text or "").split("\n") if ln.strip()]
        if not lines:
            return 0, 0
        max_words = max(len(ln.split()) for ln in lines)
        max_chars = max(len(ln) for ln in lines)
        return max_words, max_chars

    def _assess_segments(self, segments: list[_Segment], total_duration: float) -> dict:
        """Rule-based QA checks for readability + retention-oriented pacing."""
        reasons: list[str] = []
        summary: dict = {
            "passed": True,
            "duration_sec": round(float(total_duration), 2),
            "reasons": reasons,
            "metrics": {},
        }
        if not VIDEO_QA_ENABLED:
            return summary

        if not (float(VIDEO_QA_MIN_DURATION_SEC) <= float(total_duration) <= float(VIDEO_QA_MAX_DURATION_SEC)):
            reasons.append("duration_out_of_target_range")

        hook = next((s for s in segments if s.role == "hook"), None)
        if hook is None:
            reasons.append("missing_hook_segment")
        else:
            if float(hook.start) > float(VIDEO_QA_HOOK_MAX_START_SEC):
                reasons.append("hook_starts_too_late")
            if float(hook.end - hook.start) < float(VIDEO_QA_HOOK_MIN_DURATION_SEC):
                reasons.append("hook_too_short")

        closing = next((s for s in segments if s.role == "closing"), None)
        if closing is None:
            reasons.append("missing_closing_segment")
        else:
            start_share = float(closing.start) / max(float(total_duration), 0.01)
            if start_share < float(VIDEO_QA_PAYOFF_MIN_SHARE) or start_share > float(VIDEO_QA_PAYOFF_MAX_SHARE):
                reasons.append("closing_payoff_window_miss")

        body = [s for s in segments if s.role == "body"]
        durations = [float(s.end - s.start) for s in body]
        if durations:
            avg_d = sum(durations) / len(durations)
            if avg_d > float(VIDEO_QA_MAX_AVG_SEGMENT_SEC):
                reasons.append("pacing_too_slow_avg_segment")
            if any(d < float(VIDEO_QA_MIN_SEGMENT_SEC) for d in durations):
                reasons.append("pacing_too_fast_segment")
            if any(d > float(VIDEO_QA_MAX_SEGMENT_SEC) for d in durations):
                reasons.append("pacing_too_slow_segment")

        dense_lines = 0
        for s in segments:
            if s.role != "body":
                continue
            max_words, max_chars = self._line_stats(s.text)
            if max_words > int(VIDEO_QA_MAX_WORDS_PER_LINE) or max_chars > int(VIDEO_QA_MAX_CHARS_PER_LINE):
                dense_lines += 1
        if dense_lines > 1:
            reasons.append("subtitle_density_too_high")

        summary["passed"] = len(reasons) == 0
        summary["metrics"] = {
            "segment_count": len(segments),
            "body_segment_count": len(body),
            "avg_body_segment_sec": round((sum(durations) / len(durations)), 2) if durations else 0.0,
            "dense_segment_count": dense_lines,
        }
        return summary

    @staticmethod
    def _allocate_chunk_durations(chunks: list[str], total_duration: float) -> list[float]:
        """
        Allocate display time by visual load:
        - character count
        - punctuation pause weight
        - minimum readable display
        """
        if not chunks:
            return []
        weights = []
        for c in chunks:
            char_count = max(1, len(c))
            punct = len(re.findall(r"[,.;:!?]", c))
            wc = max(1, len(re.findall(r"\w+", c)))
            weight = (char_count / 28.0) + (punct * 0.55) + (wc / 12.0)
            weights.append(max(1.0, weight))
        sum_w = sum(weights)
        durations = [(w / sum_w) * total_duration for w in weights]

        min_time = 2.0
        if len(chunks) * min_time > total_duration:
            min_time = max(1.2, total_duration / len(chunks))

        # Raise too-short clips to minimum.
        for i, d in enumerate(durations):
            if d < min_time:
                durations[i] = min_time

        # Normalize back to total_duration.
        scale = total_duration / sum(durations)
        durations = [d * scale for d in durations]
        return durations

    @staticmethod
    def _chunk_body_for_frames(body: str, max_words_per_line: int = 8, max_lines_per_frame: int = 1) -> list[str]:
        """
        Convert body text to compact chunks suitable for shorts:
        - each chunk renders as one frame
        - each chunk has at most 1–2 lines
        - each line has short word count for readability
        - avoid orphan last-word / punctuation-only lines
        """
        text = re.sub(r"\s+", " ", body or "").strip()
        if not text:
            return []

        # Keep punctuation attached to words; this prevents '.' or ',' from landing alone.
        words = text.split()
        lines: list[str] = []
        current: list[str] = []

        for word in words:
            current.append(word)
            if len(current) >= max_words_per_line:
                lines.append(" ".join(current).strip())
                current = []

        if current:
            lines.append(" ".join(current).strip())

        # Rebalance to avoid orphan tiny ending line like "you." or "again."
        if len(lines) >= 2:
            last_words = lines[-1].split()
            prev_words = lines[-2].split()
            if len(last_words) <= 2 and len(prev_words) >= 5:
                take = 2 if len(prev_words) >= 6 else 1
                moved = prev_words[-take:]
                prev_words = prev_words[:-take]
                lines[-2] = " ".join(prev_words).strip()
                lines[-1] = " ".join(moved + last_words).strip()
                lines = [ln for ln in lines if ln]

        chunks: list[str] = []
        for i in range(0, len(lines), max_lines_per_frame):
            chunk = "\n".join(lines[i:i + max_lines_per_frame]).strip()
            if chunk:
                chunks.append(chunk)
        return chunks
