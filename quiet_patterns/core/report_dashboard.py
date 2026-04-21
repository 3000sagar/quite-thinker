"""
core/report_dashboard.py - Build a lightweight local HTML dashboard from pipeline reports.
"""

from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path
from typing import Any

from config import (
    DB_PATH,
    RETENTION_READY_MIN_POSITIVE_SAMPLES,
    RETENTION_READY_MIN_DISTINCT_HOURS,
)


def build_reports_dashboard(reports_dir: Path, output_html: Path) -> int:
    reports_dir = Path(reports_dir)
    output_html = Path(output_html)
    reports_dir.mkdir(parents=True, exist_ok=True)

    report_files = sorted(reports_dir.glob("pipeline_report_*.json"))
    rows: list[dict[str, Any]] = []
    for path in report_files[-200:]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue

        result = payload.get("result", {}) or {}
        attempts = payload.get("attempts", []) or []
        last_attempt = attempts[-1] if attempts else {}
        rows.append(
            {
                "file": path.name,
                "started_at": payload.get("started_at", ""),
                "status": result.get("status", "unknown"),
                "upload_status": result.get("upload_status", ""),
                "threshold": payload.get("threshold", 0),
                "attempt_count": len(attempts),
                "final_viral": result.get("viral_score", last_attempt.get("viral_score", 0)),
                "final_retention": result.get("retention_pct", last_attempt.get("retention_pct", 0)),
                "video_path": result.get("video_path", ""),
            }
        )

    rows = _collapse_runs(rows)
    analytics_health = _load_analytics_health()
    uniqueness = _load_uniqueness_snapshot()
    html = _render_dashboard(rows, analytics_health, uniqueness)
    output_html.write_text(html, encoding="utf-8")
    return len(rows)


def _collapse_runs(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Keep one dashboard row per started_at:
    include only final `completed` reports.
    """
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        key = str(row.get("started_at") or "")
        grouped.setdefault(key, []).append(row)

    collapsed: list[dict[str, Any]] = []
    for key in sorted(grouped.keys()):
        bucket = grouped[key]
        completed = [r for r in bucket if r.get("status") == "completed"]
        if completed:
            # If multiple final rows somehow exist, keep the latest by filename.
            best = sorted(completed, key=lambda r: str(r.get("file") or ""))[-1]
            collapsed.append(best)
    return collapsed


def _load_analytics_health() -> dict[str, Any]:
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        row = cur.execute(
            """
            SELECT
                COUNT(*) AS total_rows,
                SUM(CASE WHEN COALESCE(retention_pct, 0) > 0 THEN 1 ELSE 0 END) AS positive_rows,
                COUNT(DISTINCT CASE WHEN COALESCE(retention_pct, 0) > 0 THEN post_hour END) AS hour_coverage
            FROM analytics
            """
        ).fetchone()
        conn.close()
        total_rows = int(row[0] or 0)
        positive_rows = int(row[1] or 0)
        hour_coverage = int(row[2] or 0)
        ready = (
            positive_rows >= int(RETENTION_READY_MIN_POSITIVE_SAMPLES)
            and hour_coverage >= int(RETENTION_READY_MIN_DISTINCT_HOURS)
        )
        return {
            "status": "ready" if ready else "warming_up",
            "total_rows": total_rows,
            "positive_rows": positive_rows,
            "hour_coverage": hour_coverage,
            "required_positive_rows": int(RETENTION_READY_MIN_POSITIVE_SAMPLES),
            "required_hour_coverage": int(RETENTION_READY_MIN_DISTINCT_HOURS),
        }
    except Exception:
        return {
            "status": "unknown",
            "total_rows": 0,
            "positive_rows": 0,
            "hour_coverage": 0,
            "required_positive_rows": int(RETENTION_READY_MIN_POSITIVE_SAMPLES),
            "required_hour_coverage": int(RETENTION_READY_MIN_DISTINCT_HOURS),
        }


def _load_uniqueness_snapshot(
    limit: int = 30,
    compare_window: int = 20,
    similarity_threshold: float = 0.55,
) -> dict[str, Any]:
    """
    Compute a hard uniqueness snapshot from recent saved scripts.
    Similarity uses max(token-jaccard, phrase-jaccard), lower is better.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT id, created_at, hook, body, closing, theme, novelty_score
            FROM scripts
            ORDER BY datetime(created_at) DESC, id DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        conn.close()
    except Exception:
        rows = []

    if not rows:
        return {
            "count": 0,
            "threshold": float(similarity_threshold),
            "avg_uniqueness": 0.0,
            "latest_uniqueness": 0.0,
            "latest_max_similarity": 0.0,
            "recent_pass_rate": 0.0,
            "items": [],
        }

    out_items: list[dict[str, Any]] = []
    for idx, row in enumerate(rows):
        text = f"{row['hook']}\n{row['body']}\n{row['closing']}"
        cand_tokens = _normalized_tokens(text)
        cand_phrases = _phrase_set(text)

        max_sim = 0.0
        for prev in rows[idx + 1 : idx + 1 + int(compare_window)]:
            prev_text = f"{prev['hook']}\n{prev['body']}\n{prev['closing']}"
            sim = max(
                _jaccard(cand_tokens, _normalized_tokens(prev_text)),
                _jaccard(cand_phrases, _phrase_set(prev_text)),
            )
            if sim > max_sim:
                max_sim = sim

        uniqueness = max(0.0, 1.0 - max_sim)
        out_items.append(
            {
                "id": int(row["id"] or 0),
                "created_at": str(row["created_at"] or ""),
                "theme": str(row["theme"] or ""),
                "novelty_score": round(float(row["novelty_score"] or 0.0), 3),
                "max_similarity": round(max_sim, 3),
                "uniqueness": round(uniqueness, 3),
                "pass": bool(max_sim < float(similarity_threshold)),
                "hook": str(row["hook"] or "")[:90],
            }
        )

    avg_uniqueness = (
        sum(float(i["uniqueness"]) for i in out_items) / len(out_items) if out_items else 0.0
    )
    pass_rate = (
        sum(1 for i in out_items if i["pass"]) / len(out_items) if out_items else 0.0
    )
    latest = out_items[0] if out_items else {"uniqueness": 0.0, "max_similarity": 0.0}

    return {
        "count": len(out_items),
        "threshold": float(similarity_threshold),
        "avg_uniqueness": round(avg_uniqueness, 3),
        "latest_uniqueness": round(float(latest["uniqueness"]), 3),
        "latest_max_similarity": round(float(latest["max_similarity"]), 3),
        "recent_pass_rate": round(pass_rate, 3),
        "items": out_items,
    }


def _normalized_tokens(text: str) -> set[str]:
    stop = {
        "a", "an", "and", "the", "to", "of", "in", "on", "for", "with", "at", "by",
        "is", "are", "was", "were", "be", "been", "being", "it", "this", "that",
        "you", "your", "i", "we", "they", "he", "she", "them", "our", "us",
        "as", "or", "if", "but", "so", "do", "does", "did", "not", "no",
        "very", "more", "most", "can", "could", "would", "should", "will", "just",
        "then", "than", "from", "into", "about", "over", "under", "again",
    }
    toks = re.findall(r"[a-z']+", str(text).lower())
    out: set[str] = set()
    for t in toks:
        if t in stop:
            continue
        t = re.sub(r"(ing|ed|ly|es|s)$", "", t)
        if len(t) >= 3:
            out.add(t)
    return out


def _phrase_set(text: str) -> set[str]:
    toks = [t for t in re.findall(r"[a-z']+", str(text).lower()) if t]
    if len(toks) < 2:
        return set(toks)
    return {f"{toks[i]} {toks[i + 1]}" for i in range(len(toks) - 1)}


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


def _render_dashboard(
    rows: list[dict[str, Any]],
    analytics_health: dict[str, Any],
    uniqueness: dict[str, Any],
) -> str:
    data_json = json.dumps(rows, ensure_ascii=True)
    health_json = json.dumps(analytics_health, ensure_ascii=True)
    uniq_json = json.dumps(uniqueness, ensure_ascii=True)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Quiet Patterns Dashboard</title>
  <style>
    :root {{
      --bg:#0b1218;
      --panel:#12202b;
      --muted:#93aab8;
      --text:#e6f1f8;
      --accent:#4dd0e1;
      --ok:#66bb6a;
      --warn:#ffb74d;
      --bad:#ef5350;
    }}
    body {{
      margin:0; background:radial-gradient(circle at 10% 10%, #163042, var(--bg) 55%);
      color:var(--text); font-family:Consolas, "Courier New", monospace;
    }}
    .wrap {{ max-width:1100px; margin:24px auto; padding:0 16px; }}
    .grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:12px; }}
    .card {{ background:var(--panel); border:1px solid #203545; border-radius:12px; padding:12px; }}
    .k {{ color:var(--muted); font-size:12px; }}
    .v {{ font-size:22px; margin-top:6px; }}
    h1 {{ font-size:24px; margin:0 0 14px 0; }}
    table {{ width:100%; border-collapse:collapse; margin-top:14px; background:var(--panel); border-radius:12px; overflow:hidden; }}
    th, td {{ padding:8px 10px; border-bottom:1px solid #203545; font-size:12px; text-align:left; }}
    th {{ color:var(--muted); }}
    .ok {{ color:var(--ok); }}
    .warn {{ color:var(--warn); }}
    .bad {{ color:var(--bad); }}
    .chart {{ margin-top:14px; background:var(--panel); border:1px solid #203545; border-radius:12px; padding:12px; }}
    svg {{ width:100%; height:200px; display:block; }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Quiet Patterns - Pipeline Dashboard</h1>
    <div class="grid">
      <div class="card"><div class="k">Runs</div><div class="v" id="runs">0</div></div>
      <div class="card"><div class="k">Success Rate</div><div class="v" id="success_rate">0%</div></div>
      <div class="card"><div class="k">Avg Viral</div><div class="v" id="avg_viral">0</div></div>
      <div class="card"><div class="k">Avg Attempts</div><div class="v" id="avg_attempts">0</div></div>
      <div class="card"><div class="k">Retention Status</div><div class="v" id="retention_status">unknown</div></div>
      <div class="card"><div class="k">Latest Uniqueness</div><div class="v" id="uniq_latest">0%</div></div>
      <div class="card"><div class="k">Max Similarity</div><div class="v" id="uniq_similarity">0%</div></div>
      <div class="card"><div class="k">Uniqueness Pass Rate</div><div class="v" id="uniq_pass_rate">0%</div></div>
    </div>
    <div class="chart">
      <div class="k">Final Viral Score Trend (latest 50 runs)</div>
      <svg viewBox="0 0 1000 220" preserveAspectRatio="none">
        <polyline id="line" fill="none" stroke="var(--accent)" stroke-width="2"></polyline>
      </svg>
    </div>
    <table>
      <thead>
        <tr>
          <th>Started</th><th>Status</th><th>Upload</th><th>Threshold</th><th>Viral</th><th>Retention</th><th>Attempts</th><th>Report</th>
        </tr>
      </thead>
      <tbody id="rows"></tbody>
    </table>
    <table>
      <thead>
        <tr>
          <th>Script ID</th><th>Created</th><th>Theme</th><th>Novelty</th><th>Max Similarity</th><th>Uniqueness</th><th>Pass</th><th>Hook Preview</th>
        </tr>
      </thead>
      <tbody id="uniq_rows"></tbody>
    </table>
  </div>
  <script>
    const rows = {data_json};
    const health = {health_json};
    const uniq = {uniq_json};
    const n = rows.length;
    const success = rows.filter(r => r.status === "completed").length;
    const avg = (arr) => arr.length ? arr.reduce((a,b)=>a+b,0)/arr.length : 0;
    const viralValues = rows.map(r => Number(r.final_viral || 0));
    const attemptValues = rows.map(r => Number(r.attempt_count || 0));
    document.getElementById("runs").textContent = n;
    document.getElementById("success_rate").textContent = n ? ((success * 100 / n).toFixed(1) + "%") : "0%";
    document.getElementById("avg_viral").textContent = avg(viralValues).toFixed(1);
    document.getElementById("avg_attempts").textContent = avg(attemptValues).toFixed(2);
    const statusText = `${{health.status}} (${{health.positive_rows}}/${{health.required_positive_rows}} rows, hours ${{health.hour_coverage}}/${{health.required_hour_coverage}})`;
    document.getElementById("retention_status").textContent = statusText;
    document.getElementById("uniq_latest").textContent = ((Number(uniq.latest_uniqueness || 0) * 100).toFixed(1)) + "%";
    document.getElementById("uniq_similarity").textContent = ((Number(uniq.latest_max_similarity || 0) * 100).toFixed(1)) + "%";
    document.getElementById("uniq_pass_rate").textContent = ((Number(uniq.recent_pass_rate || 0) * 100).toFixed(1)) + "%";

    const tbody = document.getElementById("rows");
    rows.slice().reverse().forEach(r => {{
      const tr = document.createElement("tr");
      const cls = r.status === "completed" ? "ok" : "bad";
      tr.innerHTML = `
        <td>${{(r.started_at || "").replace("T", " ").slice(0,19)}}</td>
        <td class="${{cls}}">${{r.status}}</td>
        <td>${{r.upload_status || "-"}}</td>
        <td>${{Number(r.threshold || 0).toFixed(1)}}</td>
        <td>${{Number(r.final_viral || 0).toFixed(1)}}</td>
        <td>${{Number(r.final_retention || 0).toFixed(1)}}%</td>
        <td>${{r.attempt_count}}</td>
        <td>${{r.file}}</td>`;
      tbody.appendChild(tr);
    }});

    const uniqBody = document.getElementById("uniq_rows");
    (uniq.items || []).slice(0, 20).forEach(r => {{
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${{r.id}}</td>
        <td>${{(r.created_at || "").replace("T", " ").slice(0,19)}}</td>
        <td>${{r.theme || "-"}}</td>
        <td>${{Number(r.novelty_score || 0).toFixed(3)}}</td>
        <td>${{(Number(r.max_similarity || 0) * 100).toFixed(1)}}%</td>
        <td>${{(Number(r.uniqueness || 0) * 100).toFixed(1)}}%</td>
        <td class="${{r.pass ? "ok" : "bad"}}">${{r.pass ? "pass" : "fail"}}</td>
        <td>${{r.hook || ""}}</td>`;
      uniqBody.appendChild(tr);
    }});

    const trend = viralValues.slice(-50);
    const min = trend.length ? Math.min(...trend, 0) : 0;
    const max = trend.length ? Math.max(...trend, 100) : 100;
    const span = Math.max(1, max - min);
    const points = trend.map((v, i) => {{
      const x = trend.length <= 1 ? 0 : (i * (1000 / (trend.length - 1)));
      const y = 210 - (((v - min) / span) * 180);
      return `${{x.toFixed(1)}},${{y.toFixed(1)}}`;
    }}).join(" ");
    document.getElementById("line").setAttribute("points", points);
  </script>
</body>
</html>
"""
