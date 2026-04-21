"""
core/thumbnail_engine.py – Generate YouTube thumbnails with high-contrast text overlay.
"""

import textwrap
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


def generate_thumbnail(
    hook_text: str,
    output_path: Path,
    font_path: Path | None = None,
    channel_name: str = "Quiet Patterns",
) -> Path:
    """
    Generate a high-contrast YouTube thumbnail.

    Layout:
        [dark background #0f1115]
        [soft purple/blue radial glow in center]
        [hook text in large white Benguiat, centered]
        [channel name at bottom left]

    Args:
        hook_text:   The hook line(s) to render.
        output_path: Where to save the JPEG.
        font_path:   Path to .ttf font file. Defaults to assets/font/benguiat.ttf.
        channel_name: Subtle label shown at bottom.

    Returns:
        Path to the saved thumbnail file.
    """
    W, H = 1280, 720  # YouTube 16:9 thumbnail

    if font_path is None:
        font_path = Path(__file__).parent.parent / "assets" / "font" / "benguiat.ttf"

    img = Image.new("RGB", (W, H), "#0f1115")
    draw = ImageDraw.Draw(img)

    # ── Soft radial glow (purple/blue tint) ──────────────────────────────────
    glow = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    glow_draw = ImageDraw.Draw(glow)
    cx, cy = W // 2, H // 2
    # Draw overlapping ellipses with decreasing opacity for a soft glow
    for radius, alpha in [
        (340, 60),
        (260, 80),
        (180, 100),
    ]:
        glow_draw.ellipse(
            [cx - radius, cy - radius // 2, cx + radius, cy + radius // 2],
            fill=(100, 60, 180, alpha),
        )
    img.paste(glow, mask=glow.split()[3])  # Use alpha as mask

    # ── Hook text ─────────────────────────────────────────────────────────────
    draw = ImageDraw.Draw(img)
    try:
        font = ImageFont.truetype(str(font_path), size=72)
    except OSError:
        font = ImageFont.load_default(size=72)

    try:
        small_font = ImageFont.truetype(str(font_path), size=28)
    except OSError:
        small_font = ImageFont.load_default(size=28)

    # Wrap text to ~22 chars per line
    lines = textwrap.wrap(hook_text, width=22)
    if not lines:
        lines = [hook_text]

    # Estimate line height from font metrics
    line_height = 90
    total_height = len(lines) * line_height
    y = (H - total_height) // 2

    for line in lines:
        bbox = draw.textbbox((0, 0), line, font=font)
        text_w = bbox[2] - bbox[0]
        x = (W - text_w) // 2
        # Subtle shadow for legibility
        draw.text((x + 2, y + 2), line, font=font, fill="#000000")
        draw.text((x, y), line, font=font, fill="#e8e8e8")
        y += line_height

    # ── Channel name at bottom ───────────────────────────────────────────────
    draw.text((40, H - 55), channel_name, font=small_font, fill="#555555")

    img.save(str(output_path), "JPEG", quality=95)
    return output_path
