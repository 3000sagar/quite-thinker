"""
core/scoring_engine.py – Rule-based retention scoring for Quiet Patterns scripts.

Scoring factors (weighted):
  1. Emotional intensity word count      (25 pts)
  2. Identity trigger presence           (20 pts)
  3. Sentence length rhythm              (15 pts)
  4. Loop strength of closing line       (20 pts)
  5. Curiosity phrase presence           (20 pts)

Returns:
  viral_score   – 0–100 float
  retention_pct – 0–100 float (estimated)
"""

import re
import math
import logging
from dataclasses import dataclass
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import (
    SCRIPT_SCORE_THRESHOLD,
    ENABLE_DYNAMIC_SCORE_THRESHOLD,
    DYNAMIC_SCORE_LOOKBACK,
    DYNAMIC_SCORE_FLOOR,
    DYNAMIC_SCORE_CEILING,
)
from core.database import execute_query

from core.script_engine import (
    EMOTIONAL_TRIGGERS,
    IDENTITY_PHRASES,
    CURIOSITY_GAPS,
    LOOP_CLOSINGS,
    Script,
)

logger = logging.getLogger(__name__)


# ─── Known high-signal word lists ────────────────────────────────────────────

_HIGH_EMOTION_WORDS = set(EMOTIONAL_TRIGGERS) | {
    "silence", "weight", "heavy", "hollow", "breaking",
    "haunts", "invisible", "ache", "grief", "dread",
    "trauma", "freeze", "collapse", "spiral", "suffocate",
    "isolate", "burden", "unheard", "unworthy", "terror",
}

_LOOP_SIGNAL_WORDS = {
    "again", "always", "continues", "returns", "repeats",
    "cycle", "loop", "over", "back", "pattern",
}

_CURIOSITY_SIGNAL_WORDS = {
    "realize", "explains", "call", "hidden", "silently",
    "rewires", "deeper", "misunderstand", "connect", "science",
    "unexpected", "surprising", "believe", "think", "understand",
}


@dataclass
class ScoreResult:
    viral_score:       float
    retention_pct:     float
    emotional_score:   float
    identity_score:    float
    rhythm_score:      float
    loop_score:        float
    curiosity_score:   float
    details:           dict


class ScoringEngine:
    """Score a Script object and return a ScoreResult."""

    THRESHOLD = float(SCRIPT_SCORE_THRESHOLD)  # Minimum viral_score to proceed

    # Weight map (sum = 100)
    WEIGHTS = {
        "emotional":  25.0,
        "identity":   20.0,
        "rhythm":     15.0,
        "loop":       20.0,
        "curiosity":  20.0,
    }

    def effective_threshold(self) -> float:
        """
        Compute adaptive viral threshold from recent accepted scripts.
        Falls back to static threshold when there is not enough history.
        """
        if not ENABLE_DYNAMIC_SCORE_THRESHOLD:
            return float(self.THRESHOLD)

        rows = execute_query(
            """
            SELECT viral_score
            FROM scripts
            ORDER BY datetime(created_at) DESC, id DESC
            LIMIT ?
            """,
            (int(DYNAMIC_SCORE_LOOKBACK),),
        )
        scores = [float(r["viral_score"]) for r in rows if r["viral_score"] is not None]
        if len(scores) < 8:
            return float(self.THRESHOLD)

        scores.sort()
        p60_idx = min(len(scores) - 1, max(0, int(round((len(scores) - 1) * 0.60))))
        p60 = scores[p60_idx]

        # Keep threshold challenging but realistic relative to recent performance.
        dynamic_target = p60 * 0.95
        bounded = max(float(DYNAMIC_SCORE_FLOOR), min(float(DYNAMIC_SCORE_CEILING), dynamic_target))
        return round(bounded, 2)

    def score(self, script: Script) -> ScoreResult:
        text  = script.full_text.lower()
        words = re.findall(r"\w+", text)

        emo_score  = self._score_emotional(words)
        id_score   = self._score_identity(text)
        rhy_score  = self._score_rhythm(text)
        loop_score = self._score_loop(script.closing)
        cur_score  = self._score_curiosity(text)

        w = self.WEIGHTS
        viral = (
            emo_score  * w["emotional"]  / 100 +
            id_score   * w["identity"]   / 100 +
            rhy_score  * w["rhythm"]     / 100 +
            loop_score * w["loop"]       / 100 +
            cur_score  * w["curiosity"]  / 100
        )
        viral = round(min(viral, 100.0), 2)

        # Retention estimate: sigmoid-based mapping from viral score
        retention = round(40 + 55 * (1 / (1 + math.exp(-0.1 * (viral - 75)))), 2)

        result = ScoreResult(
            viral_score     = viral,
            retention_pct   = retention,
            emotional_score = emo_score,
            identity_score  = id_score,
            rhythm_score    = rhy_score,
            loop_score      = loop_score,
            curiosity_score = cur_score,
            details = {
                "word_count": script.word_count,
                "hook_first_word": text.split()[0] if text else "",
            }
        )
        logger.info(
            "Script scored: viral=%.1f, retention=%.1f%%", viral, retention
        )
        return result

    def is_acceptable(self, result: ScoreResult, threshold: float | None = None) -> bool:
        target = float(threshold) if threshold is not None else float(self.effective_threshold())
        return result.viral_score >= target

    def quality_gate(self, script: Script, result: ScoreResult, threshold: float | None = None) -> tuple[bool, list[str]]:
        """
        Additional hard gate before rendering:
        - hook strength
        - practical advice presence
        - emotional density
        """
        reasons: list[str] = []
        hook_words = len(re.findall(r"\w+", script.hook.lower()))
        if hook_words < 4 or hook_words > 16:
            reasons.append(f"hook_length={hook_words} out of range")

        hook_tokens = set(re.findall(r"\w+", script.hook.lower()))
        # Soft check: only warn through logs if weak, do not hard-fail by itself.
        hook_has_emotion = bool(hook_tokens & _HIGH_EMOTION_WORDS)

        advice_markers = {"pause", "name", "breathe", "write", "ask", "set", "notice", "choose", "ground", "boundary"}
        full_tokens = set(re.findall(r"\w+", script.full_text.lower()))
        if not (advice_markers & full_tokens):
            logger.info("Quality note: missing_practical_advice_step")

        emo_hits = sum(1 for w in re.findall(r"\w+", script.full_text.lower()) if w in _HIGH_EMOTION_WORDS)
        if emo_hits < 2:
            logger.info(f"Quality note: low_emotional_density={emo_hits}")

        target = float(threshold) if threshold is not None else float(self.effective_threshold())
        if result.viral_score < target:
            reasons.append(f"viral_below_threshold={result.viral_score:.1f}<{target:.1f}")

        if not hook_has_emotion:
            logger.info("Quality note: hook_missing_emotional_trigger")

        return (len(reasons) == 0), reasons

    # ── Factor scorers ──────────────────────────────────────────────────

    def _score_emotional(self, words: list[str]) -> float:
        """0–100: % of high-emotion words relative to a saturation point."""
        hits = sum(1 for w in words if w in _HIGH_EMOTION_WORDS)
        # 5+ hits = full score; scale linearly below
        return min(hits / 5.0, 1.0) * 100

    def _score_identity(self, text: str) -> float:
        """0–100: presence of identity-trigger phrases."""
        found = sum(1 for phrase in IDENTITY_PHRASES if phrase in text)
        return min(found / 1.0, 1.0) * 100  # 1 phrase → full score

    def _score_rhythm(self, text: str) -> float:
        """
        0–100: sentence-length variety.
        Good rhythm = mix of short (≤6 words) and longer sentences.
        Score = 100 if both short and long sentences present, scales down otherwise.
        """
        sentences = [s.strip() for s in re.split(r"[.\n!?]", text) if s.strip()]
        if not sentences:
            return 50.0
        lengths  = [len(re.findall(r"\w+", s)) for s in sentences]
        has_short = any(l <= 5 for l in lengths)
        has_long  = any(l >= 10 for l in lengths)
        score = 0.0
        if has_short: score += 50
        if has_long:  score += 50
        # Bonus: at least 4 sentences
        if len(sentences) >= 4:
            score = min(score + 10, 100)
        return score

    def _score_loop(self, closing: str) -> float:
        """0–100: strength of loop signal words in closing line."""
        words = re.findall(r"\w+", closing.lower())
        hits  = sum(1 for w in words if w in _LOOP_SIGNAL_WORDS)
        return min(hits / 2.0, 1.0) * 100  # 2+ loop words = full score

    def _score_curiosity(self, text: str) -> float:
        """0–100: curiosity-gap signal word density."""
        words = re.findall(r"\w+", text)
        hits  = sum(1 for w in words if w in _CURIOSITY_SIGNAL_WORDS)
        return min(hits / 3.0, 1.0) * 100  # 3+ curiosity words = full score
