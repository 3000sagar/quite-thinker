"""
core/metadata_engine.py – Generate YouTube-safe titles, descriptions, and tags.
"""

import random
import re
import logging
from dataclasses import dataclass
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from core.script_engine import Script

logger = logging.getLogger(__name__)

# ─── Title templates (≤60 chars, lowercase, curiosity-driven) ───────────────

_TITLE_TEMPLATES = [
    # Strong hooks as titles (proven viral patterns - use hook directly)
    "if you feel {trigger} and cannot explain it",
    "the moment you feel {trigger},",
    "most {identity} do this without realizing it",
    "there is a name for what {identity} quietly carry",
    "your quiet habit is saying something deeper",
    "the pattern that keeps you feeling {trigger}",
    "why you feel most alone in a room full of people",
    "why {identity} go quiet under stress",
    "the real reason you can't stop overthinking",
    "what your silence may be protecting",
    "why you shrink yourself around others",
    "the cost of hiding your emotions quietly",
    "this is what anxiety looks like from inside",
    "why you apologize for existing",
    "the loop your mind keeps returning to",
    "what nobody tells {identity} about feeling {trigger}",
    # Varied formats for algorithmic diversity
    "therapist take: why you feel {trigger}",
    "what it means when you always feel {trigger}",
    "why {identity} feel everything more deeply",
    "the hidden reason you keep feeling {trigger}",
]

_DESCRIPTION_TEMPLATES = [
    "Follow for more gentle therapist guidance on {topic}.",
    "If this resonated, save it and share with someone who needs it.",
    "A calm breakdown of {topic} in everyday language.",
    "Quiet, practical insight for people dealing with {topic}.",
    "Supportive perspective on {topic} without judgment.",
    "Follow for daily therapist-style insights on emotional patterns.",
]

_TOPIC_FILLERS = [
    "emotional suppression",
    "the overthinking loop",
    "identity and self-perception",
    "silent nervous system responses",
    "emotional masking in daily life",
    "hypervigilance and anxiety",
]

_DYNAMIC_TAG_POOL = [
    "#therapistadvice", "#mentalwellness", "#emotionalhealing",
    "#overthinkers", "#anxietysupport", "#selfawareness",
    "#innerwork", "#nervoussystem", "#quietmind",
    "#attachmentpatterns", "#traumainformed", "#mindfulness",
    "#boundaries", "#selfregulation", "#emotionalgrowth",
    "#introvertlife", "#highlysensitiveperson", "#healingjourney",
    "#humanbehavior", "#quietpatterns",
]

_FIXED_TAGS = ["#shorts", "#therapistadvice", "#mentalhealth"]


@dataclass
class Metadata:
    title:       str
    description: str
    tags:        list[str]


class MetadataEngine:
    """Generate YouTube metadata from a Script."""

    MAX_TITLE_LENGTH = 60

    def generate(self, script: Script) -> Metadata:
        title = self._build_title(script)
        desc  = self._build_description(script)
        tags  = self._build_tags()

        logger.info("Metadata generated — title: '%s' (%d chars)", title, len(title))
        return Metadata(title=title, description=desc, tags=tags)

    # ── Internal ─────────────────────────────────────────────────────────

    def _build_title(self, script: Script) -> str:
        # Extract dominant trigger word from hook
        trigger  = self._extract_word_from(script.hook, _TRIGGER_WORDS)
        identity = self._extract_word_from(script.hook + script.body, _IDENTITY_WORDS)

        for tpl in random.sample(_TITLE_TEMPLATES, len(_TITLE_TEMPLATES)):
            candidate = tpl.format(
                trigger=trigger or "everything",
                identity=identity or "some people"
            ).strip()
            if len(candidate) <= self.MAX_TITLE_LENGTH:
                return candidate
        # Fallback: truncate
        return "therapist advice for what you feel quietly"[:self.MAX_TITLE_LENGTH]

    def _build_description(self, script: Script) -> str:
        topic = random.choice(_TOPIC_FILLERS)
        tpl   = random.choice(_DESCRIPTION_TEMPLATES)
        return tpl.format(topic=topic).strip()

    def _build_tags(self) -> list[str]:
        dynamic = random.sample(_DYNAMIC_TAG_POOL, 3)
        return _FIXED_TAGS + dynamic

    @staticmethod
    def _extract_word_from(text: str, word_pool: list[str]) -> str | None:
        text_lower = text.lower()
        for word in word_pool:
            if word in text_lower:
                return word
        return None


# Shared pools for extraction (imported from script_engine for consistency)
_TRIGGER_WORDS = [
    "anxious", "exhausted", "overwhelmed", "lonely", "disconnected",
    "invisible", "misunderstood", "stuck", "numb", "afraid",
    "unworthy", "ashamed", "restless", "empty", "lost",
    "tense", "guilty", "desperate", "defeated", "hollow",
]
_IDENTITY_WORDS = [
    "overthinkers", "introverts", "perfectionists",
    "highly sensitive people", "quiet observers",
    "people who always put others first",
]
