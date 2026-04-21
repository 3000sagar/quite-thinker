"""
core/instagram_uploader.py - Instagram Reels publishing via Meta Graph API.

Notes:
  - Requires Instagram Professional account + connected Facebook Page.
  - Graph API reel publishing requires a PUBLIC video URL.
  - This module intentionally uses stdlib urllib (no extra dependency).
"""

from __future__ import annotations

import json
import logging
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Optional

import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import (
    ENABLE_INSTAGRAM_UPLOAD,
    IG_USER_ID,
    IG_ACCESS_TOKEN,
    IG_API_VERSION,
    IG_VIDEO_URL_TEMPLATE,
    IG_SHARE_TO_FEED,
    IG_PUBLISH_POLL_INTERVAL_SEC,
    IG_PUBLISH_TIMEOUT_SEC,
)
from core.database import execute_write
from core.metadata_engine import Metadata

logger = logging.getLogger(__name__)


class InstagramUploadError(Exception):
    """Raised when Graph API publish flow fails."""


class InstagramUploader:
    """Publish rendered videos as Reels on Instagram via Graph API."""

    def __init__(self) -> None:
        self.enabled = bool(ENABLE_INSTAGRAM_UPLOAD)

    def is_configured(self) -> bool:
        return (
            self.enabled
            and bool(str(IG_USER_ID).strip())
            and bool(str(IG_ACCESS_TOKEN).strip())
            and bool(str(IG_VIDEO_URL_TEMPLATE).strip())
        )

    def upload(self, video_path: Path, metadata: Metadata, video_db_id: int) -> dict[str, Any]:
        """
        Publish reel and return:
          {"status":"published","media_id":"...","permalink":"...","video_url":"...","caption":"..."}
        """
        if not self.enabled:
            return {"status": "disabled"}
        if not self.is_configured():
            return {"status": "not_configured"}

        video_url = self._resolve_video_url(video_path)
        caption = self._build_caption(metadata)

        logger.info("Instagram: creating media container.")
        container_id = self._create_container(video_url=video_url, caption=caption)
        self._wait_until_ready(container_id)

        logger.info("Instagram: publishing media container %s", container_id)
        media_id = self._publish_container(container_id)
        permalink = self._fetch_permalink(media_id)

        self._save_social_post(
            video_db_id=video_db_id,
            platform="instagram",
            external_id=media_id,
            permalink=permalink,
            caption=caption,
            status="published",
        )
        logger.info("Instagram: publish successful (media_id=%s)", media_id)
        return {
            "status": "published",
            "media_id": media_id,
            "permalink": permalink,
            "video_url": video_url,
            "caption": caption,
        }

    def _resolve_video_url(self, video_path: Path) -> str:
        tpl = str(IG_VIDEO_URL_TEMPLATE or "").strip()
        filename = video_path.name
        stem = video_path.stem
        ext = video_path.suffix.lstrip(".")
        url = tpl.format(filename=filename, stem=stem, ext=ext)
        if not str(url).startswith(("http://", "https://")):
            raise InstagramUploadError(
                "IG_VIDEO_URL_TEMPLATE must resolve to a public http(s) URL."
            )
        return str(url)

    def _build_caption(self, metadata: Metadata) -> str:
        tags = [str(t).strip() for t in (metadata.tags or []) if str(t).strip()]
        hash_tags = [t if t.startswith("#") else f"#{t}" for t in tags]
        caption = f"{metadata.title}\n\n{metadata.description}".strip()
        if hash_tags:
            caption = f"{caption}\n\n{' '.join(hash_tags)}"
        # Instagram caption hard limit is ~2200 chars.
        return caption[:2200]

    def _graph_url(self, path: str) -> str:
        clean = path.lstrip("/")
        return f"https://graph.facebook.com/{IG_API_VERSION}/{clean}"

    def _post_form(self, path: str, data: dict[str, Any]) -> dict[str, Any]:
        encoded = urllib.parse.urlencode({k: str(v) for k, v in data.items()}).encode("utf-8")
        req = urllib.request.Request(
            self._graph_url(path),
            data=encoded,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read().decode("utf-8", errors="ignore")
            out = json.loads(raw)
            if "error" in out:
                raise InstagramUploadError(f"Graph error: {out['error']}")
            return out
        except Exception as exc:
            raise InstagramUploadError(f"Graph POST failed on '{path}': {exc}") from exc

    def _get_json(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        query = urllib.parse.urlencode({k: str(v) for k, v in params.items()})
        url = f"{self._graph_url(path)}?{query}"
        req = urllib.request.Request(url, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read().decode("utf-8", errors="ignore")
            out = json.loads(raw)
            if "error" in out:
                raise InstagramUploadError(f"Graph error: {out['error']}")
            return out
        except Exception as exc:
            raise InstagramUploadError(f"Graph GET failed on '{path}': {exc}") from exc

    def _create_container(self, video_url: str, caption: str) -> str:
        payload = {
            "media_type": "REELS",
            "video_url": video_url,
            "caption": caption,
            "share_to_feed": str(bool(IG_SHARE_TO_FEED)).lower(),
            "access_token": IG_ACCESS_TOKEN,
        }
        out = self._post_form(f"{IG_USER_ID}/media", payload)
        cid = str(out.get("id", "")).strip()
        if not cid:
            raise InstagramUploadError("Missing container id from Instagram /media response.")
        return cid

    def _wait_until_ready(self, container_id: str) -> None:
        deadline = time.time() + int(IG_PUBLISH_TIMEOUT_SEC)
        while time.time() < deadline:
            out = self._get_json(
                container_id,
                {
                    "fields": "status_code,status",
                    "access_token": IG_ACCESS_TOKEN,
                },
            )
            status_code = str(out.get("status_code", "") or out.get("status", "")).upper()
            if status_code in {"FINISHED", "PUBLISHED"}:
                return
            if status_code in {"ERROR", "EXPIRED"}:
                raise InstagramUploadError(f"Container {container_id} failed with status {status_code}.")
            time.sleep(max(1, int(IG_PUBLISH_POLL_INTERVAL_SEC)))
        raise InstagramUploadError(
            f"Timed out waiting for Instagram container readiness after {IG_PUBLISH_TIMEOUT_SEC}s."
        )

    def _publish_container(self, container_id: str) -> str:
        out = self._post_form(
            f"{IG_USER_ID}/media_publish",
            {"creation_id": container_id, "access_token": IG_ACCESS_TOKEN},
        )
        media_id = str(out.get("id", "")).strip()
        if not media_id:
            raise InstagramUploadError("Missing media id from Instagram /media_publish response.")
        return media_id

    def _fetch_permalink(self, media_id: str) -> str:
        try:
            out = self._get_json(
                media_id,
                {"fields": "permalink", "access_token": IG_ACCESS_TOKEN},
            )
            return str(out.get("permalink", "")).strip()
        except Exception:
            return ""

    def _save_social_post(
        self,
        video_db_id: int,
        platform: str,
        external_id: str,
        permalink: str,
        caption: str,
        status: str,
    ) -> None:
        execute_write(
            """
            INSERT INTO social_posts (video_id, platform, external_id, permalink, caption, status)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (video_db_id, platform, external_id, permalink, caption, status),
        )

