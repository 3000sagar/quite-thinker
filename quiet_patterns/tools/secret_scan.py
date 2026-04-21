#!/usr/bin/env python3
"""
Simple staged-file secret scanner for git pre-commit.
Blocks commit when likely credentials are detected.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path


SECRET_PATTERNS = [
    ("openai_api_key", re.compile(r"\bsk-[A-Za-z0-9]{20,}\b")),
    ("google_api_key", re.compile(r"\bAIza[0-9A-Za-z\-_]{20,}\b")),
    ("github_pat", re.compile(r"\bghp_[A-Za-z0-9]{20,}\b")),
    ("github_fine_grained_pat", re.compile(r"\bgithub_pat_[A-Za-z0-9_]{20,}\b")),
    ("slack_token", re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b")),
    ("private_key_block", re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----")),
    ("generic_assignment", re.compile(r"(?i)\b(api[_-]?key|token|secret|password)\s*[:=]\s*['\"][^'\"]{8,}['\"]")),
]

BLOCKED_PATH_PATTERNS = [
    re.compile(r"(^|[\\/])client_secrets\.json$", re.IGNORECASE),
    re.compile(r"(^|[\\/])tokens([\\/]|$)", re.IGNORECASE),
    re.compile(r"\.pem$", re.IGNORECASE),
    re.compile(r"\.p12$", re.IGNORECASE),
    re.compile(r"\.key$", re.IGNORECASE),
    re.compile(r"\.env(\..+)?$", re.IGNORECASE),
]

TEXT_EXTS = {
    ".py", ".md", ".txt", ".json", ".yaml", ".yml", ".toml", ".ini", ".cfg",
    ".env", ".sh", ".ps1", ".bat", ".cmd", ".js", ".ts", ".tsx", ".jsx", ".html", ".css",
}


def run_git(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["git"] + args, capture_output=True, text=True, check=False)


def staged_files() -> list[str]:
    proc = run_git(["diff", "--cached", "--name-only", "--diff-filter=ACMRTUXB"])
    if proc.returncode != 0:
        return []
    return [line.strip() for line in proc.stdout.splitlines() if line.strip()]


def staged_text(path: str) -> str:
    proc = run_git(["show", f":{path}"])
    if proc.returncode != 0:
        return ""
    return proc.stdout


def looks_text(path: str, content: str) -> bool:
    if Path(path).suffix.lower() in TEXT_EXTS:
        return True
    return "\x00" not in content


def scan_paths(paths: list[str]) -> list[str]:
    issues: list[str] = []
    for p in paths:
        for rx in BLOCKED_PATH_PATTERNS:
            if rx.search(p):
                issues.append(f"{p}: blocked sensitive path pattern")
                break
    return issues


def scan_content(paths: list[str]) -> list[str]:
    issues: list[str] = []
    for p in paths:
        content = staged_text(p)
        if not content or not looks_text(p, content):
            continue
        for name, rx in SECRET_PATTERNS:
            if rx.search(content):
                issues.append(f"{p}: matched {name}")
                break
    return issues


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--staged", action="store_true", help="scan staged files only")
    args = parser.parse_args()

    paths = staged_files() if args.staged else []
    if not paths:
        return 0

    issues = scan_paths(paths) + scan_content(paths)
    if not issues:
        return 0

    print("Secret scan failed. Remove secrets before committing:")
    for i in issues:
        print(f"  - {i}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
