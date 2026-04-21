"""
core/script_engine.py – Therapist-advice script generator for Quiet Patterns.

Responsibilities:
  • Ollama/template generation for hook/body/closing
  • Hook / body / closing template system
  • Emotional trigger word bank
  • Curiosity-gap phrase injection
  • Loop-ending generator
  • SHA-256 duplicate detection (SQLite-backed)
  • Word-count gate (40–90 words)
  • Append accepted model outputs to CSV log
"""

import csv
import hashlib
import json
import logging
import random
import re
import urllib.error
import urllib.request
from datetime import datetime
from dataclasses import dataclass, field
from collections import deque
from typing import Optional

from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import (
    SCRIPT_MIN_WORDS,
    SCRIPT_MAX_WORDS,
    SCRIPT_GENERATOR_BACKEND,
    SCRIPT_GENERATOR_FALLBACK_TO_TEMPLATE,
    SCRIPT_BUILD_FROM_SCRATCH,
    MODEL_OUTPUT_LOG_CSV,
    OLLAMA_BASE_URL,
    OLLAMA_MODEL,
    OLLAMA_TIMEOUT_SEC,
    OLLAMA_TEMPERATURE,
    THEME_COOLDOWN_RECENT_SCRIPTS,
    NOVELTY_WINDOW,
    NOVELTY_MIN_SCORE,
    GENERATE_HOOK_VARIANTS,
    HOOK_VARIANT_COUNT,
    GENERATE_MULTILINGUAL_VARIANTS,
    SCRIPT_VARIANT_LANGUAGES,
    ENABLE_CONTENT_EXPERIMENTS,
    EXPERIMENT_LOOKBACK_SCRIPTS,
    EXPERIMENT_ARMS,
)
from core.database import execute_query, execute_write

logger = logging.getLogger(__name__)

# Semantic anti-repeat guard (lightweight, local, deterministic).
_STOPWORDS = {
    "a", "an", "and", "the", "to", "of", "in", "on", "for", "with", "at", "by",
    "is", "are", "was", "were", "be", "been", "being", "it", "this", "that",
    "you", "your", "i", "we", "they", "he", "she", "them", "our", "us",
    "as", "or", "if", "but", "so", "do", "does", "did", "not", "no",
    "very", "more", "most", "can", "could", "would", "should", "will", "just",
    "then", "than", "from", "into", "about", "over", "under", "again",
}

# ─── Word & phrase banks ─────────────────────────────────────────────────────

EMOTIONAL_TRIGGERS = [
    "anxious", "exhausted", "overwhelmed", "lonely", "disconnected",
    "invisible", "misunderstood", "stuck", "numb", "afraid",
    "unworthy", "ashamed", "restless", "empty", "lost",
    "tense", "guilty", "desperate", "defeated", "hollow",
]

IDENTITY_PHRASES = [
    "people like you",
    "overthinkers",
    "highly sensitive people",
    "introverts",
    "people who grew up feeling different",
    "those who suppress their emotions",
    "perfectionists",
    "people who always put others first",
    "those who struggle to say no",
    "quiet observers",
]

CURIOSITY_GAPS = [
    "and most people never realize why",
    "there is a name for this pattern",
    "researchers call this something unexpected",
    "this behavior has a hidden cost",
    "and it silently rewires your brain",
    "the reason is deeper than you think",
    "most people misunderstand this completely",
    "very few ever connect these two things",
    "the science behind this will surprise you",
    "and it starts long before you notice it",
]

LOOP_CLOSINGS = [
    "and the cycle continues — quietly.",
    "until something finally breaks the loop.",
    "the pattern repeats. always.",
    "it starts over. every time.",
    "and you carry it again, into tomorrow.",
    "the mind returns to where it began.",
    "nothing resolves. it only pauses.",
    "and that quiet tension never fully leaves.",
    "the loop tightens. slowly.",
    "and the beginning finds you again.",
]

CTA_CLOSINGS = [
    "tonight, pause and name what you feel before you react.",
    "try one boundary today, even if your voice shakes.",
    "take one slow breath and choose a softer response.",
    "write one honest line before the loop starts again.",
    "ask for one clear need instead of hiding it again.",
]

THEME_KEYWORDS = {
    "overthinking": {"overthink", "rumination", "replay", "threat", "scan"},
    "boundaries": {"boundary", "no", "people-please", "fawn", "apologize"},
    "silence": {"quiet", "silence", "withdraw", "pull", "away"},
    "anxiety_loop": {"anxious", "restless", "loop", "cycle", "again"},
    "self_worth": {"unworthy", "ashamed", "invisible", "misunderstood"},
    "emotional_numbing": {"numb", "hollow", "empty", "swallow", "masking"},
}

# ─── Hook templates ──────────────────────────────────────────────────────────
# Placeholders: {trigger}, {identity}, {curiosity}

HOOK_TEMPLATES = [
    "if you feel {trigger} for no clear reason —",
    "most {identity} do this without realizing it.",
    "there is something {identity} quietly carry every day.",
    "the moment you feel {trigger},",
    "when silence feels heavier than words —",
    "you were never taught this about feeling {trigger}.",
    "something happens to {identity} when no one is watching.",
    "the brain of someone who often feels {trigger}:",
    "what you call being fine is often this:",
    "if you keep hiding this feeling, read this:",
    "you are not dramatic. this is a pattern:",
    "nobody told {identity} this part:",
    "your quiet habit is saying something deeper:",
    "the reason you feel {trigger} this way:",
]

# ─── Body templates (relatable behavior -> gentle therapist explanation) ─────

BODY_TEMPLATES = [
    (
        "you go quiet. you pull away. you tell yourself you are fine.\n"
        "but this is not weakness — it is a nervous system response.\n"
        "your brain learned, early on, that stillness was safer than speech.\n"
        "so it defaults there. {curiosity}."
    ),
    (
        "you replay the conversation. again. and again.\n"
        "not because you are overthinking — because your brain is threat-scanning.\n"
        "it is looking for the moment things went wrong.\n"
        "this is called rumination. {curiosity}."
    ),
    (
        "you apologize for taking up space.\n"
        "for having needs. for existing too loudly.\n"
        "this is called fawn response — a survival strategy, not a personality flaw.\n"
        "it was learned. {curiosity}."
    ),
    (
        "you feel most alone in a room full of people.\n"
        "you smile. you perform. you disappear inside yourself.\n"
        "therapists call this emotional masking.\n"
        "it protects others from your truth. {curiosity}."
    ),
    (
        "you cannot rest without guilt.\n"
        "stillness feels dangerous. productivity feels like proof you exist.\n"
        "this is not ambition — it is anxiety wearing a mask.\n"
        "and it exhausts everything underneath. {curiosity}."
    ),
    (
        "you shrink yourself to fit spaces that were never made for you.\n"
        "you edit your words before speaking. you second-guess existing.\n"
        "this pattern has a name in therapy rooms.\n"
        "and it starts much earlier than most people believe. {curiosity}."
    ),
    (
        "you feel the emotion but cannot name it.\n"
        "so you swallow it. you move on. you carry it silently.\n"
        "therapists call this alexithymia — emotional blindness.\n"
        "not cold. just untrained. {curiosity}."
    ),
    (
        "you over-explain yourself to people who were never confused.\n"
        "you justify your choices to people who never asked.\n"
        "this is hypervigilance — a mind braced for judgment.\n"
        "it is exhausting to live this way. {curiosity}."
    ),
]

BEHAVIOR_SNIPPETS = [
    "you check your tone before speaking.",
    "you reread messages before pressing send.",
    "you feel tense in calm moments.",
    "you keep conversations replaying in your head.",
    "you apologize before anyone blames you.",
    "you hide your need and call it being easy.",
    "you act fine while your chest stays heavy.",
    "you keep shrinking to avoid conflict.",
]

INSIGHT_SNIPPETS = [
    "this is often your nervous system predicting social danger.",
    "this is a learned safety pattern, not a personality flaw.",
    "your brain is prioritizing protection over expression.",
    "old emotional rules can run your present reactions.",
    "your body is trying to prevent rejection before it happens.",
    "you are not broken; this is adaptive patterning.",
]

ACTION_SNIPPETS = [
    "pause ten seconds and name one feeling out loud.",
    "write one honest sentence before you reply.",
    "choose one boundary sentence and practice it once.",
    "take one slower breath and relax your jaw before reacting.",
    "ask for one clear need instead of over-explaining.",
    "notice the trigger and delay your response by one minute.",
]

EXPERIMENT_ARM_TRANSFORMS = {
    "identity_mirror": {
        "hook_prefixes": [
            "people like you often miss this at first:",
            "this mirrors what quiet people hide:",
            "if this sounds like you, read this:",
        ],
        "body_suffixes": [
            "if this feels personal, that is the point.",
            "this pattern feels private, but it is common.",
        ],
    },
    "hidden_cost": {
        "hook_prefixes": [
            "the hidden cost nobody names:",
            "what this pattern quietly charges you:",
            "the part no one warns you about:",
        ],
        "body_suffixes": [
            "the cost grows when the pattern stays invisible.",
            "unseen patterns become expensive over time.",
        ],
    },
    "micro_action": {
        "hook_prefixes": [
            "try this tiny reset today:",
            "one small move can shift this:",
            "start with this one-minute action:",
        ],
        "body_suffixes": [
            "small actions repeated daily change the loop fastest.",
            "micro-actions beat intensity when done consistently.",
        ],
    },
    "pattern_interrupt": {
        "hook_prefixes": [
            "stop the pattern here:",
            "interrupt the loop at this moment:",
            "break the cycle at this exact point:",
        ],
        "body_suffixes": [
            "interrupting once is how the cycle weakens.",
            "one interruption is enough to reduce the loop.",
        ],
    },
}

SCRATCH_OPENERS = [
    "you look calm, but your mind is still bracing.",
    "you keep moving, but your chest never unclenches.",
    "you act fine, but your body is stuck in defense.",
    "you answer everyone, then disappear from yourself.",
    "you keep it together in public and unravel in silence.",
]

SCRATCH_CONTEXT = [
    "for {identity}, this starts quietly and becomes normal.",
    "most {identity} call this personality, but it is a pattern.",
    "many {identity} learn this early and repeat it automatically.",
    "if you often feel {trigger}, this pattern can run your day.",
]

SCRATCH_INSIGHTS = [
    "your nervous system is choosing safety over honesty.",
    "your brain is predicting rejection before it happens.",
    "old survival rules are steering present decisions.",
    "this is protective wiring, not proof that you are broken.",
]

SCRATCH_ACTIONS = [
    "before your next reply, pause ten seconds and name one feeling.",
    "today, replace one apology with one clear need.",
    "choose one boundary sentence and say it once, slowly.",
    "delay one people-pleasing response by sixty seconds.",
    "write one honest line before you answer anyone.",
]

SCRATCH_ENDINGS = [
    "if you do this once, the pattern loses some power.",
    "the loop weakens the moment your real voice appears.",
    "healing starts when truth becomes safer than performance.",
    "your life changes when safety and honesty stop fighting.",
]

# ─── Dataclass ───────────────────────────────────────────────────────────────

@dataclass
class Script:
    hook:         str
    body:         str
    closing:      str
    full_text:    str   = field(init=False)
    word_count:   int   = field(init=False)
    content_hash: str   = field(init=False)

    def __post_init__(self) -> None:
        self.full_text    = f"{self.hook}\n{self.body}\n{self.closing}"
        self.word_count   = len(re.findall(r"\w+", self.full_text))
        self.content_hash = hashlib.sha256(
            self.full_text.strip().lower().encode()
        ).hexdigest()


# ─── Engine ──────────────────────────────────────────────────────────────────

class ScriptEngine:
    """Generates original therapist-advice scripts; rejects duplicates."""

    MAX_ATTEMPTS = 16
    RECENT_SIMILARITY_WINDOW = 200
    MEANING_SIMILARITY_THRESHOLD = 0.65
    PHRASE_SIMILARITY_THRESHOLD = 0.48
    MAX_MEANING_THRESHOLD = 0.82
    MAX_PHRASE_THRESHOLD = 0.74
    MIN_SIMILARITY_WINDOW = 40
    STRUCTURE_REPEAT_WINDOW = 140
    HOOK_STEM_WORDS = 5
    CLOSING_TAIL_WORDS = 5

    def __init__(self) -> None:
        # Track generation choices to increase diversity over time.
        self._used_combos: set[str] = set()
        self._body_usage = {i: 0 for i in range(len(BODY_TEMPLATES))}
        self._recent_body_idxs: deque[int] = deque(maxlen=8)
        self._model_output_hashes_cache: Optional[set[str]] = None
        logger.info("ScriptEngine initialized")

    def pick_experiment_arm(self) -> str:
        """
        Choose an experiment arm with light balancing over recent scripts.
        """
        configured = [str(a).strip().lower() for a in EXPERIMENT_ARMS if str(a).strip()]
        arms = [a for a in configured if a in EXPERIMENT_ARM_TRANSFORMS] or list(EXPERIMENT_ARM_TRANSFORMS.keys())
        if not ENABLE_CONTENT_EXPERIMENTS:
            return arms[0]

        rows = execute_query(
            """
            SELECT experiment_arm, COUNT(*) AS cnt
            FROM (
                SELECT experiment_arm
                FROM scripts
                ORDER BY datetime(created_at) DESC, id DESC
                LIMIT ?
            )
            GROUP BY experiment_arm
            """,
            (int(EXPERIMENT_LOOKBACK_SCRIPTS),),
        )
        counts = {a: 0 for a in arms}
        for r in rows:
            arm = str(r["experiment_arm"] or "").strip().lower()
            if arm in counts:
                counts[arm] = int(r["cnt"] or 0)
        min_count = min(counts.values()) if counts else 0
        least_used = [a for a, c in counts.items() if c == min_count]
        return random.choice(least_used) if least_used else random.choice(arms)

    # ------------------------------------------------------------------
    def generate(
        self,
        retry_index: int = 0,
        previous_viral: float = 0.0,
        experiment_arm: Optional[str] = None,
    ) -> Optional[Script]:
        """
        Attempt to generate a valid, unique script.
        Returns None if all attempts fail.
        """
        chosen_arm = str(experiment_arm or self.pick_experiment_arm()).strip().lower()
        for attempt in range(self.MAX_ATTEMPTS):
            script = self._build_candidate(
                retry_index=retry_index,
                previous_viral=previous_viral,
                experiment_arm=chosen_arm,
            )
            if script is None:
                continue
            reason = self._validate(script)
            if reason:
                logger.debug("Attempt %d rejected: %s", attempt + 1, reason)
                continue
            if self._is_duplicate(script.content_hash):
                logger.debug("Attempt %d is duplicate", attempt + 1)
                continue
            if self._is_used_in_model_output_csv(script):
                logger.debug("Attempt %d rejected: already present in model output history", attempt + 1)
                continue
            if self._is_structural_repeat(script):
                logger.debug("Attempt %d rejected: structural repeat", attempt + 1)
                continue
            if self._is_semantic_repeat(script, attempt=attempt):
                logger.debug("Attempt %d rejected: semantic repeat", attempt + 1)
                continue
            theme = self._extract_theme(script)
            if self._is_theme_on_cooldown(theme):
                logger.debug("Attempt %d rejected: theme '%s' on cooldown", attempt + 1, theme)
                continue
            novelty = self._compute_novelty(script)
            if novelty < NOVELTY_MIN_SCORE:
                logger.debug(
                    "Attempt %d rejected: novelty %.2f below %.2f",
                    attempt + 1, novelty, NOVELTY_MIN_SCORE
                )
                continue
            logger.info(
                "Script generated — %d words, hash %s, theme=%s, novelty=%.2f",
                script.word_count, script.content_hash[:8], theme, novelty
            )
            setattr(script, "_theme", theme)
            setattr(script, "_novelty", novelty)
            setattr(script, "_experiment_arm", chosen_arm)
            return script
        logger.warning("ScriptEngine failed to produce a valid script after %d attempts", self.MAX_ATTEMPTS)
        return None

    def rewrite_candidate(
        self,
        script: Script,
        reasons: Optional[list[str]] = None,
        score_hint: Optional[dict] = None,
        attempt_index: int = 0,
    ) -> Script:
        """
        Improve a weak candidate deterministically (no hard bypass):
        - strengthen emotional hook
        - add practical action step
        - raise curiosity and loop signal
        - preserve word-count constraints
        """
        reasons = reasons or []
        score_hint = score_hint or {}

        hook = script.hook.strip().lower()
        body = script.body.strip().lower()
        closing = script.closing.strip().lower()

        low_emotion = float(score_hint.get("emotional_score", 100.0)) < 45.0
        low_identity = float(score_hint.get("identity_score", 100.0)) < 50.0
        low_curiosity = float(score_hint.get("curiosity_score", 100.0)) < 50.0
        low_loop = float(score_hint.get("loop_score", 100.0)) < 55.0
        weak_viral = any("viral_below_threshold" in r for r in reasons)

        if low_identity and not any(p in hook for p in IDENTITY_PHRASES):
            hook = f"most {random.choice(IDENTITY_PHRASES)} do this without realizing it."

        hook_tokens = set(re.findall(r"\w+", hook))
        if (low_emotion or weak_viral) and not (hook_tokens & set(EMOTIONAL_TRIGGERS)):
            hook = f"if you feel {random.choice(EMOTIONAL_TRIGGERS)} and cannot explain it —"

        advice_markers = {"pause", "name", "breathe", "write", "ask", "set", "notice", "choose", "ground", "boundary"}
        if not (set(re.findall(r"\w+", body)) & advice_markers):
            body = f"{body}\npause for ten seconds, name the feeling, then choose one boundary."

        if low_curiosity and not any(w in body for w in ("hidden", "realize", "deeper", "surprise")):
            body = f"{body}\nthis behavior has a hidden cost, and most people never realize why."

        if low_loop:
            loop_tail = random.choice(
                [
                    "the cycle repeats again.",
                    "and the pattern returns tomorrow.",
                    "the loop continues until you name it.",
                ]
            )
            if loop_tail not in closing:
                closing = f"{closing.rstrip('.')} {loop_tail}"

        if low_emotion:
            extra = ", ".join(random.sample(EMOTIONAL_TRIGGERS, k=3))
            body = f"{body}\nyou may feel {extra} before you even notice the pattern."

        upgraded = Script(hook=hook, body=body, closing=closing)
        upgraded = self._trim_script_to_bounds(upgraded)
        logger.info(
            "Rewrote weak script (attempt=%d) from %d -> %d words",
            attempt_index + 1, script.word_count, upgraded.word_count
        )
        return upgraded

    def build_ab_variants(self, script: Script, count: int = 4) -> list[Script]:
        """
        Build controlled variants of an accepted script for pre-render A/B scoring.
        """
        count = max(1, int(count))
        variants: list[Script] = [script]
        arm = str(getattr(script, "_experiment_arm", "") or "").strip().lower()
        seen_hashes = {script.content_hash}

        for idx in range(max(0, count - 1)):
            hook_variant = random.choice(HOOK_TEMPLATES).format(
                trigger=random.choice(EMOTIONAL_TRIGGERS),
                identity=random.choice(IDENTITY_PHRASES),
                curiosity=random.choice(CURIOSITY_GAPS),
            ).strip().lower()
            closing_variant = random.choice(LOOP_CLOSINGS).strip().lower()
            candidate = Script(hook=hook_variant, body=script.body, closing=closing_variant)
            candidate = self._trim_script_to_bounds(candidate)

            if candidate.content_hash in seen_hashes:
                # Fallback: rewrite base softly when template collisions happen.
                candidate = self.rewrite_candidate(
                    script,
                    reasons=["ab_variant_collision"],
                    score_hint={"emotional_score": 50.0, "loop_score": 45.0, "curiosity_score": 45.0},
                    attempt_index=idx,
                )

            if candidate.content_hash not in seen_hashes:
                if arm:
                    setattr(candidate, "_experiment_arm", arm)
                variants.append(candidate)
                seen_hashes.add(candidate.content_hash)

        return variants

    # ------------------------------------------------------------------
    def save(self, script: Script, viral_score: float, retention_pct: float) -> int:
        """Persist script to DB; return row id."""
        theme = getattr(script, "_theme", self._extract_theme(script))
        novelty = float(getattr(script, "_novelty", self._compute_novelty(script)))
        experiment_arm = str(getattr(script, "_experiment_arm", "") or "").strip().lower() or None
        row_id = execute_write(
            """
            INSERT INTO scripts
                (content_hash, hook, body, closing, theme, experiment_arm, full_text, word_count, novelty_score, viral_score, retention_pct)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                script.content_hash,
                script.hook,
                script.body,
                script.closing,
                theme,
                experiment_arm,
                script.full_text,
                script.word_count,
                novelty,
                viral_score,
                retention_pct,
            )
        )
        logger.info("Script saved to DB with id=%d", row_id)
        self._update_theme_cooldown(theme, row_id)
        self._append_model_output_log(script, viral_score)
        if GENERATE_HOOK_VARIANTS:
            self._save_hook_variants(row_id, script, count=max(1, int(HOOK_VARIANT_COUNT)))
        if GENERATE_MULTILINGUAL_VARIANTS:
            self._save_language_variants(row_id, script)
        return row_id

    # ------------------------------------------------------------------
    def _build_candidate(
        self,
        retry_index: int = 0,
        previous_viral: float = 0.0,
        experiment_arm: Optional[str] = None,
    ) -> Optional[Script]:
        backend = SCRIPT_GENERATOR_BACKEND.strip().lower()

        if backend == "ollama":
            from_ollama = self._build_candidate_from_ollama(
                retry_index=retry_index,
                previous_viral=previous_viral,
                experiment_arm=experiment_arm,
            )
            if from_ollama is not None:
                return from_ollama
            if SCRIPT_GENERATOR_FALLBACK_TO_TEMPLATE:
                logger.warning("Ollama generation failed; falling back to template generator.")
                return self._build_candidate_from_templates(experiment_arm=experiment_arm)
            return None

        if backend == "template":
            return self._build_candidate_from_templates(experiment_arm=experiment_arm)

        # auto mode: ollama -> template
        from_ollama = self._build_candidate_from_ollama(
            retry_index=retry_index,
            previous_viral=previous_viral,
            experiment_arm=experiment_arm,
        )
        if from_ollama is not None:
            return from_ollama
        return self._build_candidate_from_templates(experiment_arm=experiment_arm)

    def _build_candidate_from_templates(self, experiment_arm: Optional[str] = None) -> Optional[Script]:
        trigger  = random.choice(EMOTIONAL_TRIGGERS)
        identity = random.choice(IDENTITY_PHRASES)
        curiosity = random.choice(CURIOSITY_GAPS)
        body_idx = self._choose_body_index()
        if SCRIPT_BUILD_FROM_SCRATCH:
            full_raw = self._compose_scratch_script(trigger=trigger, identity=identity, curiosity=curiosity)
            hook, body, closing = self._split_scratch_script(full_raw)
            combo_key = f"scratch|{hashlib.sha256(full_raw.encode('utf-8')).hexdigest()[:20]}"
        else:
            closing  = random.choice(LOOP_CLOSINGS)
            hook_tpl = random.choice(HOOK_TEMPLATES)
            use_compositional = random.random() < 0.62
            body_tpl = BODY_TEMPLATES[body_idx]
            if use_compositional:
                behavior = random.choice(BEHAVIOR_SNIPPETS)
                insight = random.choice(INSIGHT_SNIPPETS)
                action = random.choice(ACTION_SNIPPETS)
                style = random.choice(
                    [
                        "behavior_insight_action",
                        "cause_reframe_action",
                        "mirror_contrast_action",
                        "micro_pattern_break",
                    ]
                )
                if style == "cause_reframe_action":
                    body = (
                        f"{behavior}\n"
                        f"it is not your personality; {insight}\n"
                        f"{curiosity}.\n"
                        f"{action}"
                    )
                elif style == "mirror_contrast_action":
                    body = (
                        f"outside you look calm. inside, {behavior}\n"
                        f"{insight}\n"
                        f"{action}\n"
                        f"{curiosity}."
                    )
                elif style == "micro_pattern_break":
                    body = (
                        f"{behavior}\n"
                        f"{curiosity}.\n"
                        f"small interruption, big shift: {action}\n"
                        f"{insight}"
                    )
                else:
                    body = (
                        f"{behavior}\n"
                        f"{insight}\n"
                        f"{action}\n"
                        f"{curiosity}."
                    )
                combo_key = f"comp|{style}|{trigger}|{identity}|{curiosity}|{closing}|{hook_tpl}|{behavior}|{insight}|{action}"
            else:
                body = body_tpl.format(curiosity=curiosity)
                combo_key = f"tpl|{trigger}|{identity}|{curiosity}|{closing}|{hook_tpl}|{body_idx}"
            hook = hook_tpl.format(trigger=trigger, identity=identity, curiosity=curiosity)
        if combo_key in self._used_combos:
            return None
        self._used_combos.add(combo_key)

        arm = str(experiment_arm or "").strip().lower()
        if arm in EXPERIMENT_ARM_TRANSFORMS:
            hook, body, closing = self._apply_experiment_arm(hook, body, closing, arm)

        # Lowercase everything
        hook    = hook.strip().lower()
        body    = body.strip().lower()
        closing = closing.strip().lower()

        self._body_usage[body_idx] += 1
        self._recent_body_idxs.append(body_idx)
        candidate = self._trim_script_to_bounds(Script(hook=hook, body=body, closing=closing))
        if arm:
            setattr(candidate, "_experiment_arm", arm)
        return candidate

    def _compose_scratch_script(self, trigger: str, identity: str, curiosity: str) -> str:
        opener = random.choice(SCRATCH_OPENERS)
        context = random.choice(SCRATCH_CONTEXT).format(identity=identity, trigger=trigger)
        insight = random.choice(SCRATCH_INSIGHTS)
        action = random.choice(SCRATCH_ACTIONS)
        ending = random.choice(SCRATCH_ENDINGS)

        middle = [context, insight, f"{curiosity}.", action]
        random.shuffle(middle)
        # Keep a clear action close to the end for retention + practicality.
        middle = [m for m in middle if m != action] + [action]
        lines = [opener] + middle + [ending]
        return " ".join(str(x).strip() for x in lines if str(x).strip())

    def _split_scratch_script(self, full_raw: str) -> tuple[str, str, str]:
        parts = [p.strip() for p in re.split(r"[.!?]+\s*", str(full_raw).strip()) if p.strip()]
        if len(parts) < 3:
            words = re.findall(r"\w+", full_raw)
            if len(words) < 12:
                return (
                    "this pattern is quieter than it looks.",
                    "your nervous system may be protecting you with old rules.\npause and name one feeling before you reply.",
                    "that one pause is where the loop starts breaking.",
                )
            third = max(4, len(words) // 3)
            hook = " ".join(words[:third]) + "."
            body = " ".join(words[third : 2 * third]) + "."
            closing = " ".join(words[2 * third :]) + "."
            return hook, body, closing

        hook = parts[0] + "."
        closing = parts[-1] + "."
        body = ". ".join(parts[1:-1]).strip()
        if body and not body.endswith("."):
            body = body + "."
        # Add line breaks for caption rhythm.
        body = body.replace(". ", ".\n")
        return hook, body, closing

    def _apply_experiment_arm(self, hook: str, body: str, closing: str, arm: str) -> tuple[str, str, str]:
        """
        Apply controlled language shifts so each experiment arm has a distinct style.
        """
        t = EXPERIMENT_ARM_TRANSFORMS.get(str(arm).strip().lower())
        if not t:
            return hook, body, closing

        hook_prefixes = t.get("hook_prefixes", []) or []
        body_suffixes = t.get("body_suffixes", []) or []
        hook_prefix = str(random.choice(hook_prefixes)).strip() if hook_prefixes else ""
        body_suffix = str(random.choice(body_suffixes)).strip() if body_suffixes else ""

        if hook_prefix and hook_prefix not in hook:
            hook = f"{hook_prefix} {hook}"
        if body_suffix and body_suffix not in body:
            body = f"{body}\n{body_suffix}"
        return hook, body, closing

 
    def _build_candidate_from_ollama(
        self,
        retry_index: int = 0,
        previous_viral: float = 0.0,
        experiment_arm: Optional[str] = None,
    ) -> Optional[Script]:
        """Two-step Ollama flow: idea -> script -> critique -> one rewrite.""" 
        normalize = lambda t: re.sub(r"\s+", " ", str(t or "")).strip().lower() 
 
        idea_prompt = ( 
            "return json only. schema: {\"angle\":\"...\",\"struggle\":\"...\",\"reason\":\"...\",\"action_step\":\"...\"}. "  
            "therapist advice niche, lowercase, practical, non-cliche, no diagnosis language. "  
            f"retry={retry_index + 1}, previous_viral={previous_viral:.1f}" 
        )
        idea = self._extract_json_block(self._request_ollama_text(idea_prompt) or "") 
        if not isinstance(idea, dict): 
            logger.warning("Ollama idea step missing valid JSON block.") 
            return None 
        angle = normalize(idea.get("angle", "")) 
        struggle = normalize(idea.get("struggle", "")) 
        reason = normalize(idea.get("reason", "")) 
        action = normalize(idea.get("action_step", "")) 
        if not angle or not struggle or not reason or not action: 
            return None
 
        script_prompt = ( 
            "return json only. schema: {\"hook\":\"...\",\"body\":\"...\",\"closing\":\"...\"}. "  
            "hook/body/closing must each be single-line strings with no newline characters. "  
            "total words 45-78, practical therapist advice, high emotional pull. "  
            f"angle={angle}; struggle={struggle}; reason={reason}; action_step={action}"  
        ) 
        obj = self._extract_json_block(self._request_ollama_text(script_prompt) or "") 
        if not isinstance(obj, dict): 
            logger.warning("Ollama script step missing valid JSON block.") 
            return None 
        hook = normalize(obj.get("hook", "")) 
        body = normalize(obj.get("body", "")) 
        closing = normalize(obj.get("closing", "")) 
        if not hook or not body or not closing: 
            return None 
        script = Script(hook=hook, body=body, closing=closing)
 
        critique_prompt = ( 
            "return json only. schema: {\"pass\": true/false, \"rewrite_instruction\": \"...\"}. "  
            "fail if weak hook, weak advice step, cliche phrasing, or low emotional pull. "  
            f"hook={script.hook}; body={script.body}; closing={script.closing}"  
        ) 
        critique = self._extract_json_block(self._request_ollama_text(critique_prompt) or "") 
        if isinstance(critique, dict) and not bool(critique.get("pass", False)): 
            inst = normalize(critique.get("rewrite_instruction", "")) 
            if inst: 
                rewrite_prompt = ( 
                    "rewrite and return json only with hook/body/closing single-line strings. "  
                    f"instruction={inst}; old_hook={script.hook}; old_body={script.body}; old_closing={script.closing}"  
                ) 
                rev = self._extract_json_block(self._request_ollama_text(rewrite_prompt) or "") 
                if isinstance(rev, dict): 
                    rh = normalize(rev.get("hook", "")); rb = normalize(rev.get("body", "")); rc = normalize(rev.get("closing", "")) 
                    if rh and rb and rc: 
                        script = Script(hook=rh, body=rb, closing=rc) 
        script = self._reinforce_scoring_signals(script)
        arm = str(experiment_arm or "").strip().lower()
        if arm in EXPERIMENT_ARM_TRANSFORMS:
            hook, body, closing = self._apply_experiment_arm(script.hook, script.body, script.closing, arm)
            script = self._trim_script_to_bounds(Script(hook=hook, body=body, closing=closing))
            setattr(script, "_experiment_arm", arm)
        return script
    def _request_ollama_text(self, prompt: str) -> Optional[str]:
        """Try Ollama generate API, then chat API as fallback."""
        base = OLLAMA_BASE_URL.rstrip("/")
        try:
            payload = {
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "format": "json",
                "options": {
                    "temperature": OLLAMA_TEMPERATURE,
                },
            }
            req = urllib.request.Request(
                url=f"{base}/api/generate",
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=OLLAMA_TIMEOUT_SEC) as resp:
                raw = resp.read().decode("utf-8", errors="ignore")
            parsed = json.loads(raw)
            return str(parsed.get("response", "")).strip()
        except urllib.error.HTTPError as exc:
            if exc.code != 404:
                logger.warning("Ollama generate API error: %s", exc)
            # fallback to /api/chat
            try:
                payload = {
                    "model": OLLAMA_MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    "stream": False,
                    "format": "json",
                    "options": {"temperature": OLLAMA_TEMPERATURE},
                }
                req = urllib.request.Request(
                    url=f"{base}/api/chat",
                    data=json.dumps(payload).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=OLLAMA_TIMEOUT_SEC) as resp:
                    raw = resp.read().decode("utf-8", errors="ignore")
                parsed = json.loads(raw)
                msg = parsed.get("message", {})
                return str(msg.get("content", "")).strip()
            except Exception as fallback_exc:
                logger.warning("Ollama chat API fallback failed: %s", fallback_exc)
        except urllib.error.URLError as exc:
            logger.warning("Could not reach Ollama at %s: %s", OLLAMA_BASE_URL, exc)
        except json.JSONDecodeError as exc:
            logger.warning("Invalid JSON from Ollama: %s", exc)
        except Exception as exc:
            logger.warning("Ollama generation error: %s", exc)
        return None

    def _build_ollama_prompt(self, retry_index: int = 0, previous_viral: float = 0.0) -> str:
        """Prompt for fully original, relatable, high-retention therapist-advice scripts."""
        retry_note = (
            f"this is retry {retry_index + 1}. previous viral score: {previous_viral:.1f}. raise emotional pull and practical clarity."
            if previous_viral > 0
            else "target a script that feels impossible to skip in the first 2 seconds."
        )
        return (
            "you write shorts scripts for the niche: therapist advice.\n"
            "generate one fresh therapist-advice script that feels personal, practical, and impossible to skip.\n\n"
            "return only valid raw json. no markdown. no code fences. no commentary.\n"
            "json schema:\n"
            "{\n"
            '  "hook": "...",\n'
            '  "body": "...",\n'
            '  "closing": "..."\n'
            "}\n\n"
            "must-follow rules:\n"
            "- lowercase only\n"
            "- total words across hook+body+closing: 45 to 78\n"
            "- no emojis, no hashtags, no bullet points\n"
            "- no medical or diagnostic claims\n"
            "- no generic lines, no cliches, no lecture tone\n"
            "- do not explain theory for too long\n"
            "- script must include one concrete therapist-style action step\n\n"
            "format and pacing:\n"
            "- hook: 1 line, 6 to 12 words, instantly emotional\n"
            "- body: single line only, compact spoken-language rhythm\n"
            "- closing: 1 line that loops emotionally and encourages rewatch\n"
            "- hook, body, and closing must each be single-line strings with no newline characters\n"
            "- keep punctuation attached to words and avoid orphan tail words\n\n"
            "content objective:\n"
            "- name a specific inner struggle the viewer already feels\n"
            "- reveal why it happens in simple human language\n"
            "- give one small therapist-style action they can do today\n"
            "- end with a lingering line\n\n"
            "quality bar:\n"
            "- should feel like: 'this is exactly me'\n"
            "- should make viewer pause instead of swipe\n"
            f"- {retry_note}\n\n"
            "now output only json."
        )

    def _reinforce_scoring_signals(self, script: Script) -> Script:
        """
        Nudge Ollama output to satisfy scorer signals so retries converge quickly.
        Keeps text in script length bounds.
        """
        hook = script.hook.strip().lower()
        body = script.body.strip().lower()
        closing = script.closing.strip().lower()
        full = f"{hook}\n{body}\n{closing}"

        # Ensure identity phrase appears (identity_score).
        if not any(phrase in full for phrase in IDENTITY_PHRASES):
            hook = f"most {random.choice(IDENTITY_PHRASES)} do this without realizing it."

        # Ensure hook has explicit emotional pull.
        hook_tokens = set(re.findall(r"\w+", hook))
        emo_set = set(EMOTIONAL_TRIGGERS)
        if not (hook_tokens & emo_set):
            emo = random.choice(EMOTIONAL_TRIGGERS)
            hook = f"if you feel {emo} and cannot explain it —"

        # Ensure curiosity signals.
        curiosity_anchor = "this behavior has a hidden cost, and most people never realize why."
        if "hidden" not in body and "realize" not in body and "deeper" not in body:
            body = f"{body}\n{curiosity_anchor}"

        # Ensure strong loop closing signals.
        loop_words = {"again", "always", "continues", "returns", "repeats", "cycle", "loop", "over", "back", "pattern"}
        closing_tokens = set(re.findall(r"\w+", closing))
        if len(loop_words & closing_tokens) < 2:
            closing = f"{closing.rstrip('.')} the cycle repeats again."

        # Rotate CTA-style closing variants for freshness and practical follow-through.
        if random.random() < 0.55:
            cta = random.choice(CTA_CLOSINGS)
            if cta not in closing:
                closing = f"{closing}\n{cta}"

        # Ensure enough emotional words for emotional score.
        emo_hits = sum(1 for w in re.findall(r"\w+", f"{hook} {body} {closing}") if w in set(EMOTIONAL_TRIGGERS))
        if emo_hits < 5:
            sample = ", ".join(random.sample(EMOTIONAL_TRIGGERS, k=5))
            body = f"{body}\nyou feel {sample}."

        # Ensure therapist-advice direction exists (not only emotional description).
        advice_markers = {
            "pause", "name", "breathe", "write", "ask", "set",
            "notice", "choose", "say", "protect", "ground",
        }
        tokens = set(re.findall(r"\w+", f"{hook} {body} {closing}"))
        if not (advice_markers & tokens):
            body = (
                f"{body}\n"
                "pause for ten seconds, name the feeling, then choose one clear boundary."
            )

        candidate = Script(hook=hook, body=body, closing=closing)
        if candidate.word_count <= SCRIPT_MAX_WORDS and candidate.word_count >= SCRIPT_MIN_WORDS:
            return candidate

        # Trim body if too long.
        if candidate.word_count > SCRIPT_MAX_WORDS:
            words = re.findall(r"\w+|[^\w\s]", body)
            # Keep rough target in range.
            target_body_words = max(20, SCRIPT_MAX_WORDS - len(re.findall(r"\w+", f"{hook} {closing}")) - 2)
            trimmed = []
            count = 0
            for token in words:
                trimmed.append(token)
                if re.match(r"\w+", token):
                    count += 1
                if count >= target_body_words:
                    break
            body = "".join(
                (t if re.match(r"[^\w\s]", t) else f"{t} ") for t in trimmed
            ).strip()
            candidate = Script(hook=hook, body=body, closing=closing)

        # Re-check advice marker after trimming to avoid losing action step.
        advice_markers = {
            "pause", "name", "breathe", "write", "ask", "set",
            "notice", "choose", "say", "protect", "ground",
        }
        if not (set(re.findall(r"\w+", candidate.full_text.lower())) & advice_markers):
            body = f"{candidate.body}\npause, name the feeling, then set one boundary."
            candidate = Script(hook=candidate.hook, body=body, closing=candidate.closing)

        if candidate.word_count < SCRIPT_MIN_WORDS:
            body = f"{candidate.body}\nthis pattern stays quiet until it feels normal."
            candidate = Script(hook=candidate.hook, body=body, closing=candidate.closing)
        return candidate

    def _trim_script_to_bounds(self, script: Script) -> Script:
        """
        Keep rewritten/variant scripts inside configured word bounds.
        """
        if SCRIPT_MIN_WORDS <= script.word_count <= SCRIPT_MAX_WORDS:
            return script

        hook = script.hook.strip()
        body = script.body.strip()
        closing = script.closing.strip()

        if script.word_count > SCRIPT_MAX_WORDS:
            words = body.split()
            while words and len(re.findall(r"\w+", f"{hook} {' '.join(words)} {closing}")) > SCRIPT_MAX_WORDS:
                words.pop()
            body = " ".join(words).strip()
            if not body:
                body = "pause, name the feeling, then choose one boundary."

        candidate = Script(hook=hook, body=body, closing=closing)
        if candidate.word_count < SCRIPT_MIN_WORDS:
            pad = " this pattern stays quiet until you name it."
            while candidate.word_count < SCRIPT_MIN_WORDS and len(re.findall(r"\w+", pad)) > 0:
                body = f"{body}{pad}"
                candidate = Script(hook=hook, body=body.strip(), closing=closing)
                pad = " pause and choose one gentle boundary."
        return candidate

    @staticmethod
    def _extract_json_block(text: str) -> Optional[dict]:
        """Extract first JSON object from model text."""
        # Remove fenced code wrappers if present.
        cleaned = text.strip()
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned)
        try:
            obj = json.loads(cleaned)
            return obj if isinstance(obj, dict) else None
        except Exception:
            pass

        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start == -1 or end == -1 or end <= start:
            return None
        snippet = cleaned[start:end + 1]
        try:
            obj = json.loads(snippet)
            return obj if isinstance(obj, dict) else None
        except Exception:
            pass

        # Fallback parser for label-style outputs:
        # hook: ...
        # body: ...
        # closing: ...
        hook_m = re.search(r"(?im)^\s*hook\s*:\s*(.+)$", cleaned)
        body_m = re.search(r"(?ims)^\s*body\s*:\s*(.+?)(?=^\s*closing\s*:|\Z)", cleaned)
        close_m = re.search(r"(?im)^\s*closing\s*:\s*(.+)$", cleaned)
        if hook_m and body_m and close_m:
            return {
                "hook": hook_m.group(1).strip(),
                "body": body_m.group(1).strip(),
                "closing": close_m.group(1).strip(),
            }
        return None

    def _choose_body_index(self) -> int:
        """Pick a body template with soft balancing and no immediate repeats."""
        candidate_idxs = [
            i for i in range(len(BODY_TEMPLATES))
            if i not in self._recent_body_idxs
        ]
        if not candidate_idxs:
            candidate_idxs = list(range(len(BODY_TEMPLATES)))

        min_use = min(self._body_usage[i] for i in candidate_idxs)
        least_used = [i for i in candidate_idxs if self._body_usage[i] == min_use]
        return random.choice(least_used)

    def _append_model_output_log(self, script: Script, viral_score: float) -> None:
        """
        Append accepted model output rows to CSV:
        created_at, hook, body, closing, viral_score
        """
        path = MODEL_OUTPUT_LOG_CSV
        header = ["created_at", "hook", "body", "closing", "viral_score"]
        row = [
            datetime.now().isoformat(timespec="seconds"),
            script.hook,
            script.body,
            script.closing,
            f"{viral_score:.1f}",
        ]
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            write_header = (not path.exists()) or path.stat().st_size == 0
            with path.open("a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                if write_header:
                    writer.writerow(header)
                writer.writerow(row)
            logger.info("Model output logged to %s", path)
        except Exception as exc:
            logger.warning("Failed to append model output log %s: %s", path, exc)

 
    def _load_model_output_hashes(self) -> set[str]: 
        if self._model_output_hashes_cache is not None: 
            return self._model_output_hashes_cache 
 
        hashes: set[str] = set() 
        path = MODEL_OUTPUT_LOG_CSV 
        if not path.exists(): 
            self._model_output_hashes_cache = hashes 
            return hashes 
 
        try: 
            with path.open("r", newline="", encoding="utf-8-sig") as f: 
                reader = csv.DictReader(f) 
                for row in reader: 
                    combined = " ".join(part.strip() for part in (row.get("hook") or "", row.get("body") or "", row.get("closing") or "") if str(part).strip()) 
                    if not combined: 
                        continue 
                    hashes.add(hashlib.sha256(re.sub(r"\s+", " ", combined.strip().lower()).encode("utf-8")).hexdigest()) 
        except Exception as exc: 
            logger.warning("Could not read model output log %%s: %%s", path, exc) 
 
        self._model_output_hashes_cache = hashes 
        return hashes 
 
    def _is_used_in_model_output_csv(self, script: Script) -> bool: 
        combined = " ".join(part.strip() for part in (script.hook, script.body, script.closing) if str(part).strip()) 
        if not combined: 
            return False 
        candidate_hash = hashlib.sha256(re.sub(r"\s+", " ", combined.strip().lower()).encode("utf-8")).hexdigest() 
        return candidate_hash in self._load_model_output_hashes() 
    def _extract_theme(self, script: Script) -> str:
        text = script.full_text.lower()
        best_theme = "general_therapist_advice"
        best_score = 0
        for theme, keys in THEME_KEYWORDS.items():
            score = sum(1 for k in keys if k in text)
            if score > best_score:
                best_score = score
                best_theme = theme
        return best_theme

    def _is_theme_on_cooldown(self, theme: str) -> bool:
        rows = execute_query(
            "SELECT last_used_script_id FROM theme_cooldown WHERE theme = ?",
            (theme,),
        )
        if not rows:
            return False
        last_id = rows[0]["last_used_script_id"] or 0
        max_row = execute_query("SELECT COALESCE(MAX(id), 0) AS max_id FROM scripts")
        max_id = int(max_row[0]["max_id"]) if max_row else 0
        return (max_id - int(last_id)) < int(THEME_COOLDOWN_RECENT_SCRIPTS)

    def _update_theme_cooldown(self, theme: str, script_id: int) -> None:
        execute_write(
            """
            INSERT INTO theme_cooldown(theme, last_used_script_id, last_used_at)
            VALUES(?, ?, datetime('now'))
            ON CONFLICT(theme) DO UPDATE SET
                last_used_script_id = excluded.last_used_script_id,
                last_used_at = datetime('now')
            """,
            (theme, script_id),
        )

    def _compute_novelty(self, candidate: Script) -> float:
        rows = execute_query(
            """
            SELECT hook, body, closing
            FROM scripts
            ORDER BY datetime(created_at) DESC, id DESC
            LIMIT ?
            """,
            (int(NOVELTY_WINDOW),),
        )
        if not rows:
            return 1.0
        cand_text = f"{candidate.hook}\n{candidate.body}\n{candidate.closing}"
        cand_tokens = self._normalized_tokens(cand_text)
        cand_phrases = self._phrase_set(cand_text)
        max_sim = 0.0
        for row in rows:
            prev_text = f"{row['hook']}\n{row['body']}\n{row['closing']}"
            prev_tokens = self._normalized_tokens(prev_text)
            prev_phrases = self._phrase_set(prev_text)
            sim = max(self._jaccard(cand_tokens, prev_tokens), self._jaccard(cand_phrases, prev_phrases))
            if sim > max_sim:
                max_sim = sim
        return max(0.0, 1.0 - max_sim)

    def _save_hook_variants(self, script_id: int, script: Script, count: int = 3) -> None:
        hooks = self._generate_hook_variants(script, count=count)
        if not hooks:
            return
        for idx, hook in enumerate(hooks, start=1):
            try:
                execute_write(
                    "INSERT INTO hook_variants(script_id, hook_text, variant_ix) VALUES (?, ?, ?)",
                    (script_id, hook, idx),
                )
            except Exception as exc:
                logger.warning("Could not save hook variant %d: %s", idx, exc)

    def _generate_hook_variants(self, script: Script, count: int = 3) -> list[str]:
        prompt = (
            "create hook variants for this short. return only json like {\"hooks\":[\"...\",\"...\"]}.\n"
            f"count: {count}\n"
            "rules: lowercase, each hook 6-12 words, unique angle, emotionally magnetic.\n"
            f"body:\n{script.body}\nclosing:\n{script.closing}\n"
        )
        text = self._request_ollama_text(prompt)
        obj = self._extract_json_block(text or "")
        if not obj:
            return []
        hooks = obj.get("hooks", [])
        if not isinstance(hooks, list):
            return []
        cleaned = []
        for h in hooks:
            s = str(h).strip().lower()
            if s:
                cleaned.append(s)
        return cleaned[:count]

    def _save_language_variants(self, script_id: int, script: Script) -> None:
        for lang in SCRIPT_VARIANT_LANGUAGES:
            lang = str(lang).strip().lower()
            if not lang:
                continue
            if lang == "en":
                hook, body, closing = script.hook, script.body, script.closing
            else:
                translated = self._translate_script(script, lang)
                if translated is None:
                    continue
                hook, body, closing = translated
            try:
                execute_write(
                    """
                    INSERT INTO script_variants(script_id, language, hook, body, closing)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (script_id, lang, hook, body, closing),
                )
            except Exception as exc:
                logger.warning("Could not save %s variant: %s", lang, exc)

    def _translate_script(self, script: Script, target_lang: str) -> Optional[tuple[str, str, str]]:
        prompt = (
            f"translate this shorts script to language code '{target_lang}'.\n"
            "return only json: {\"hook\":\"...\",\"body\":\"...\",\"closing\":\"...\"}\n"
            "keep emotional tone and line breaks. no extra text.\n"
            f"hook: {script.hook}\nbody: {script.body}\nclosing: {script.closing}\n"
        )
        text = self._request_ollama_text(prompt)
        obj = self._extract_json_block(text or "")
        if not obj:
            return None
        hook = str(obj.get("hook", "")).strip()
        body = str(obj.get("body", "")).strip()
        closing = str(obj.get("closing", "")).strip()
        if not hook or not body or not closing:
            return None
        return hook, body, closing

    # ------------------------------------------------------------------
    def _validate(self, script: Script) -> Optional[str]:
        """Return a reason string if invalid, else None."""
        if script.word_count < SCRIPT_MIN_WORDS:
            return f"too short ({script.word_count} words)"
        if script.word_count > SCRIPT_MAX_WORDS:
            return f"too long ({script.word_count} words)"
        return None

    # ------------------------------------------------------------------
    @staticmethod
    def _is_duplicate(content_hash: str) -> bool:
        rows = execute_query(
            "SELECT id FROM scripts WHERE content_hash = ?",
            (content_hash,)
        )
        return len(rows) > 0

    def _similarity_policy(self, attempt: int) -> tuple[float, float, int]:
        """
        Adapt repeat filtering by attempt so generation does not dead-end.
        Early attempts are strict; later attempts widen acceptable variation.
        """
        max_attempt_idx = max(1, self.MAX_ATTEMPTS - 1)
        progress = min(1.0, max(0.0, attempt / max_attempt_idx))

        meaning_threshold = min(
            self.MAX_MEANING_THRESHOLD,
            self.MEANING_SIMILARITY_THRESHOLD + (0.17 * progress),
        )
        phrase_threshold = min(
            self.MAX_PHRASE_THRESHOLD,
            self.PHRASE_SIMILARITY_THRESHOLD + (0.26 * progress),
        )
        recent_window = max(
            self.MIN_SIMILARITY_WINDOW,
            int(round(self.RECENT_SIMILARITY_WINDOW * (1.0 - (0.7 * progress)))),
        )
        return meaning_threshold, phrase_threshold, recent_window

    def _is_semantic_repeat(self, candidate: Script, attempt: int = 0) -> bool:
        """
        Reject scripts that are too similar in meaning to recent saved scripts.
        Uses token-set and phrase-overlap heuristics for fast local checks.
        """
        meaning_threshold, phrase_threshold, recent_window = self._similarity_policy(attempt)
        recent_rows = execute_query(
            """
            SELECT hook, body, closing
            FROM scripts
            ORDER BY datetime(created_at) DESC, id DESC
            LIMIT ?
            """,
            (recent_window,),
        )
        if not recent_rows:
            return False

        cand_text = f"{candidate.hook}\n{candidate.body}\n{candidate.closing}"
        cand_tokens = self._normalized_tokens(cand_text)
        cand_phrases = self._phrase_set(cand_text)

        for row in recent_rows:
            prev_text = f"{row['hook']}\n{row['body']}\n{row['closing']}"
            prev_tokens = self._normalized_tokens(prev_text)
            prev_phrases = self._phrase_set(prev_text)

            meaning_sim = self._jaccard(cand_tokens, prev_tokens)
            phrase_sim = self._jaccard(cand_phrases, prev_phrases)

            # Treat strong meaning overlap as immediate repeat.
            if meaning_sim >= (meaning_threshold + 0.08):
                logger.info(
                    "Rejected semantic repeat (meaning_sim=%.2f >= %.2f, attempt=%d, window=%d)",
                    meaning_sim, (meaning_threshold + 0.08), attempt + 1, recent_window
                )
                return True
            # Treat strong phrase overlap as immediate repeat.
            if phrase_sim >= (phrase_threshold + 0.10):
                logger.info(
                    "Rejected phrase repeat (phrase_sim=%.2f >= %.2f, attempt=%d, window=%d)",
                    phrase_sim, (phrase_threshold + 0.10), attempt + 1, recent_window
                )
                return True
            # Medium overlap requires both meaning + phrase overlap to reject.
            if meaning_sim >= meaning_threshold and phrase_sim >= phrase_threshold:
                logger.info(
                    (
                        "Rejected blended repeat "
                        "(meaning_sim=%.2f >= %.2f, phrase_sim=%.2f >= %.2f, attempt=%d, window=%d)"
                    ),
                    meaning_sim, meaning_threshold, phrase_sim, phrase_threshold, attempt + 1, recent_window
                )
                return True
        return False

    def _is_structural_repeat(self, candidate: Script) -> bool:
        """
        Reject near-identical script structure to force visible variation across videos.
        """
        rows = execute_query(
            """
            SELECT hook, body, closing
            FROM scripts
            ORDER BY datetime(created_at) DESC, id DESC
            LIMIT ?
            """,
            (int(self.STRUCTURE_REPEAT_WINDOW),),
        )
        if not rows:
            return False

        cand_hook_stem = self._hook_stem(candidate.hook, self.HOOK_STEM_WORDS)
        cand_closing_tail = self._closing_tail(candidate.closing, self.CLOSING_TAIL_WORDS)
        cand_body_shape = self._body_shape(candidate.body)

        for row in rows:
            prev_hook = str(row["hook"] or "")
            prev_body = str(row["body"] or "")
            prev_closing = str(row["closing"] or "")
            prev_hook_stem = self._hook_stem(prev_hook, self.HOOK_STEM_WORDS)
            prev_closing_tail = self._closing_tail(prev_closing, self.CLOSING_TAIL_WORDS)
            prev_body_shape = self._body_shape(prev_body)
            hook_sim = self._jaccard(set(cand_hook_stem), set(prev_hook_stem))
            close_sim = self._jaccard(set(cand_closing_tail), set(prev_closing_tail))

            if cand_hook_stem and cand_hook_stem == prev_hook_stem:
                logger.info("Rejected structural repeat (same hook stem: %s)", " ".join(cand_hook_stem))
                return True
            # Repeated closing tail alone is allowed; reject only if overall structure is also close.
            if cand_closing_tail and cand_closing_tail == prev_closing_tail:
                if cand_body_shape == prev_body_shape and hook_sim >= 0.4:
                    logger.info("Rejected structural repeat (same closing tail + body shape + similar hook)")
                    return True
            if cand_body_shape == prev_body_shape and hook_sim >= 0.6 and close_sim >= 0.6:
                logger.info("Rejected structural repeat (shape + opening/closing overlap)")
                return True
        return False

    @staticmethod
    def _hook_stem(text: str, n: int) -> tuple[str, ...]:
        raw = str(text).lower()
        if ":" in raw:
            raw = raw.split(":", 1)[1]
        toks = [t for t in re.findall(r"[a-z']+", raw) if t not in _STOPWORDS]
        return tuple(toks[: max(1, int(n))])

    @staticmethod
    def _closing_tail(text: str, n: int) -> tuple[str, ...]:
        toks = [t for t in re.findall(r"[a-z']+", str(text).lower()) if t not in _STOPWORDS]
        if not toks:
            return tuple()
        k = max(1, int(n))
        return tuple(toks[-k:])

    @staticmethod
    def _body_shape(text: str) -> str:
        lines = [ln.strip() for ln in str(text).split("\n") if ln.strip()]
        line_count = len(lines)
        first_words = [ln.split()[0] for ln in lines if ln.split()]
        fw = "-".join(first_words[:3]) if first_words else "none"
        total_words = len(re.findall(r"\w+", str(text)))
        if total_words < 28:
            band = "short"
        elif total_words <= 52:
            band = "medium"
        else:
            band = "long"
        return f"{line_count}|{fw}|{band}"

    @staticmethod
    def _normalized_tokens(text: str) -> set[str]:
        tokens = re.findall(r"[a-z']+", text.lower())
        out = set()
        for t in tokens:
            if t in _STOPWORDS:
                continue
            # Light stemming to collapse near variants.
            t = re.sub(r"(ing|ed|ly|es|s)$", "", t)
            if len(t) >= 3:
                out.add(t)
        return out

    @staticmethod
    def _phrase_set(text: str) -> set[str]:
        tokens = [t for t in re.findall(r"[a-z']+", text.lower()) if t not in _STOPWORDS]
        if len(tokens) < 2:
            return set(tokens)
        return {f"{tokens[i]} {tokens[i+1]}" for i in range(len(tokens) - 1)}

    @staticmethod
    def _jaccard(a: set[str], b: set[str]) -> float:
        if not a or not b:
            return 0.0
        inter = len(a & b)
        union = len(a | b)
        return inter / union if union else 0.0
