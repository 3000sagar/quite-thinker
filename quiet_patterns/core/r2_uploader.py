"""
core/r2_uploader.py – Cloudflare R2 upload for Instagram public video URLs.

Requirements:
    pip install boto3

Setup:
    1. Create a Cloudflare R2 bucket at dash.cloudflare.com
    2. Add a custom domain (e.g. reels.yourdomain.com) OR use R2.dev preview URLs
    3. Create an R2 API token with Object Read/Write permissions
    4. Fill in config_secrets.py with your credentials
"""

import logging
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# Config-driven imports — fails gracefully if secrets not configured
try:
    from config import ENABLE_R2_UPLOAD
    from config_secrets import (
        R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY,
        R2_BUCKET_NAME, R2_PUBLIC_URL, R2_REGION,
    )
    _R2_CONFIGURED = bool(R2_ACCOUNT_ID and R2_ACCESS_KEY_ID and R2_BUCKET_NAME)
except ImportError:
    _R2_CONFIGURED = False
    ENABLE_R2_UPLOAD = False


class R2Uploader:
    """Upload videos to Cloudflare R2 and return a public URL for Instagram."""

    def __init__(self) -> None:
        if not ENABLE_R2_UPLOAD:
            raise RuntimeError("R2 upload is disabled in config.")
        if not _R2_CONFIGURED:
            raise RuntimeError(
                "R2 credentials not configured. "
                "Fill in config_secrets.py with your Cloudflare R2 API token and bucket details."
            )
        self._client = boto3.client(
            "s3",
            endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
            aws_access_key_id=R2_ACCESS_KEY_ID,
            aws_secret_access_key=R2_SECRET_ACCESS_KEY,
            region_name=R2_REGION or "auto",
            config=Config(signature_version="s3v4"),
        )
        self._bucket = R2_BUCKET_NAME
        self._public_url_base = R2_PUBLIC_URL.rstrip("/") if R2_PUBLIC_URL else None

    def upload(self, video_path: Path, filename: str | None = None) -> str:
        """
        Upload video_path to R2 and return its public URL.

        Args:
            video_path: Local path to the video file.
            filename:   Optional R2 key (filename). Defaults to video_path.name.

        Returns:
            Public URL to the uploaded file.

        Raises:
            RuntimeError if R2 is disabled or misconfigured.
            ClientError if the upload fails.
        """
        if filename is None:
            filename = video_path.name

        logger.info("Uploading %s to R2 bucket '%s' as '%s'", video_path, self._bucket, filename)

        try:
            self._client.upload_file(
                str(video_path),
                self._bucket,
                filename,
                ExtraArgs={"ContentType": "video/mp4"},
            )
            logger.info("R2 upload successful: %s", filename)
        except ClientError as exc:
            logger.error("R2 upload failed: %s", exc)
            raise

        public_url = self._build_url(filename)
        logger.info("Public URL: %s", public_url)
        return public_url

    def _build_url(self, filename: str) -> str:
        if self._public_url_base:
            return f"{self._public_url_base}/{filename}"
        # Fallback: R2.dev preview URL (temporary, rate-limited)
        return f"https://{self._bucket}.{R2_ACCOUNT_ID}.r2.dev/{filename}"

    def delete(self, filename: str) -> None:
        """Delete an object from R2."""
        try:
            self._client.delete_object(Bucket=self._bucket, Key=filename)
            logger.info("Deleted R2 object: %s", filename)
        except ClientError as exc:
            logger.warning("R2 delete failed for %s: %s", filename, exc)
