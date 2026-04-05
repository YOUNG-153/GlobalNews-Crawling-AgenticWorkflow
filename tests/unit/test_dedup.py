"""Unit tests for src/crawling/dedup.py.

Covers:
- SimHash: 64-bit fingerprint consistency, near-duplicate detection,
  CJK text tokenization, very short / empty body handling
- Title similarity: Jaccard + Levenshtein thresholds, site-name stripping,
  prefix matching, CJK titles, normalization of prefixes ([속보], [Breaking])
- DedupEngine cascade: Level 1 URL exact match, Level 2 title match,
  Level 3 SimHash near-duplicate
- DedupEngine persistence: cross-invocation URL and fingerprint memory
- DedupEngine batch: batch dedup with in-batch duplicates
- DedupEngine stats: count tracking
- DedupResult: unique() factory
- Thread safety: concurrent is_duplicate() calls
"""

import sys
import os
import threading
import uuid

import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.crawling.dedup import (
    DedupEngine,
    DedupResult,
    compute_simhash,
    hamming_distance,
    simhash_similarity,
    titles_are_similar,
    jaccard_similarity,
    _normalize_title,
    _levenshtein_distance,
    SIMHASH_BITS,
    SIMHASH_THRESHOLD,
    TITLE_JACCARD_THRESHOLD,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _engine() -> DedupEngine:
    """Return a fresh in-memory DedupEngine for each test."""
    return DedupEngine(in_memory=True)


def _article_id() -> str:
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# SimHash tests
# ---------------------------------------------------------------------------

class TestComputeSimhash:
    """SimHash produces correct 64-bit fingerprints."""

    def test_returns_64bit_int(self):
        fp = compute_simhash("Hello world, this is a test article about global news.")
        assert isinstance(fp, int)
        assert 0 <= fp < (1 << 64)

    def test_identical_text_same_fingerprint(self):
        text = "The quick brown fox jumps over the lazy dog near the river bank."
        assert compute_simhash(text) == compute_simhash(text)

    def test_empty_text_returns_zero(self):
        assert compute_simhash("") == 0

    def test_short_text_returns_zero(self):
        # Text shorter than MIN_BODY_LEN_FOR_SIMHASH (50) should return 0
        assert compute_simhash("Short") == 0

    def test_completely_different_texts_large_hamming(self):
        fp_a = compute_simhash(
            "The president signed a major economic reform bill yesterday, "
            "according to government officials who spoke on condition of anonymity. "
            "The legislation will take effect next quarter."
        )
        fp_b = compute_simhash(
            "Scientists discovered a new species of deep-sea fish in the Pacific Ocean. "
            "The organism, which lives at depths of over 5,000 meters, was found during "
            "a recent oceanographic expedition funded by marine research institutes."
        )
        dist = hamming_distance(fp_a, fp_b)
        # Very different content should have a large Hamming distance (> 10 bits)
        assert dist > SIMHASH_THRESHOLD

    def test_near_duplicate_text_small_hamming(self):
        """Text with only a few word changes should have Hamming distance ≤ SIMHASH_THRESHOLD."""
        # Longer texts (more tokens) produce more stable SimHash fingerprints.
        # Only 1 word differs: "forecasts" → "outlooks" in the last sentence.
        base = (
            "Breaking news: The central bank announced interest rate decisions today. "
            "Officials confirmed the benchmark rate will remain unchanged at 3.5 percent "
            "amid concerns about inflation and economic growth forecasts for the year. "
            "The policy committee voted unanimously to maintain the current monetary stance. "
            "Bond markets rallied on the announcement while currency traders remained cautious. "
            "Financial analysts broadly expected this outcome based on recent economic data."
        )
        near_dup = (
            "Breaking news: The central bank announced interest rate decisions today. "
            "Officials confirmed the benchmark rate will remain unchanged at 3.5 percent "
            "amid concerns about inflation and economic growth outlooks for the year. "
            "The policy committee voted unanimously to maintain the current monetary stance. "
            "Bond markets rallied on the announcement while currency traders remained cautious. "
            "Financial analysts broadly expected this outcome based on recent economic data."
        )
        fp_a = compute_simhash(base)
        fp_b = compute_simhash(near_dup)
        dist = hamming_distance(fp_a, fp_b)
        assert dist <= SIMHASH_THRESHOLD, f"Expected dist <= {SIMHASH_THRESHOLD}, got {dist}"

    def test_cjk_text_computes_fingerprint(self):
        """CJK (Korean) text should produce a non-zero fingerprint."""
        korean_text = (
            "서울 중앙지방법원은 오늘 주요 기업 회장에 대한 판결을 내렸다. "
            "재판부는 징역 3년에 집행유예 5년을 선고하면서 기업의 사회적 책임을 강조했다. "
            "이번 판결은 재계에 큰 파장을 일으킬 것으로 전망된다."
        )
        fp = compute_simhash(korean_text)
        assert fp != 0
        assert 0 <= fp < (1 << 64)

    def test_korean_duplicate_small_hamming(self):
        """Near-duplicate Korean articles should have Hamming distance ≤ SIMHASH_THRESHOLD."""
        base = (
            "정부는 오늘 새로운 부동산 정책을 발표했다. 수도권 지역을 중심으로 "
            "주택 공급을 확대하고 투기 수요를 억제하는 방안이 포함됐다. "
            "전문가들은 이번 정책이 주택 시장에 미칠 영향을 분석하고 있다."
        )
        near_dup = (
            "정부는 오늘 새로운 부동산 정책을 발표했다. 수도권 지역을 중심으로 "
            "주택 공급을 늘리고 투기 수요를 억제하는 방안이 포함됐다. "
            "전문가들은 이번 정책이 주택 시장에 미칠 영향을 분석하고 있다."
        )
        fp_a = compute_simhash(base)
        fp_b = compute_simhash(near_dup)
        dist = hamming_distance(fp_a, fp_b)
        assert dist <= SIMHASH_THRESHOLD, f"Korean near-dup Hamming distance: {dist}"


class TestHammingDistance:
    def test_identical_hashes_distance_zero(self):
        assert hamming_distance(0xDEADBEEF, 0xDEADBEEF) == 0

    def test_one_bit_difference(self):
        a = 0b1100
        b = 0b1101
        assert hamming_distance(a, b) == 1

    def test_all_bits_different(self):
        assert hamming_distance(0, (1 << 64) - 1) == 64


class TestSimhashSimilarity:
    def test_identical_is_1(self):
        fp = compute_simhash("Some content " * 20)
        assert simhash_similarity(fp, fp) == 1.0

    def test_completely_different_low_similarity(self):
        a = 0
        b = (1 << 64) - 1  # all bits different
        assert simhash_similarity(a, b) == 0.0

    def test_one_bit_diff_high_similarity(self):
        a = 0b1
        b = 0b0
        sim = simhash_similarity(a, b)
        # 1/64 bits different -> similarity = 63/64
        assert abs(sim - (63 / 64)) < 0.01


# ---------------------------------------------------------------------------
# Title similarity tests
# ---------------------------------------------------------------------------

class TestNormalizeTitle:
    def test_strips_whitespace(self):
        assert _normalize_title("  Hello World  ") == "hello world"

    def test_strips_site_suffix_pipe(self):
        result = _normalize_title("Article Title | BBC News")
        assert "bbc" not in result
        assert "article title" in result

    def test_strips_site_suffix_dash(self):
        result = _normalize_title("Article Title - The Guardian")
        assert "guardian" not in result

    def test_strips_korean_breaking_prefix(self):
        result = _normalize_title("[속보] 정부 긴급 발표")
        assert "속보" not in result

    def test_strips_english_breaking_prefix(self):
        result = _normalize_title("[Breaking] Major Event Happens")
        assert "breaking" not in result.lower()

    def test_lowercase(self):
        assert _normalize_title("HELLO WORLD") == "hello world"

    def test_collapses_spaces(self):
        assert _normalize_title("hello   world") == "hello world"


class TestLevenshteinDistance:
    def test_identical_strings(self):
        assert _levenshtein_distance("hello", "hello") == 0

    def test_empty_vs_nonempty(self):
        assert _levenshtein_distance("", "abc") == 3

    def test_one_insertion(self):
        assert _levenshtein_distance("cat", "cats") == 1

    def test_one_substitution(self):
        assert _levenshtein_distance("cat", "bat") == 1

    def test_one_deletion(self):
        assert _levenshtein_distance("cats", "cat") == 1

    def test_completely_different(self):
        assert _levenshtein_distance("abc", "xyz") == 3


class TestJaccardSimilarity:
    def test_identical_sets(self):
        s = {"a", "b", "c"}
        assert jaccard_similarity(s, s) == 1.0

    def test_disjoint_sets(self):
        assert jaccard_similarity({"a"}, {"b"}) == 0.0

    def test_partial_overlap(self):
        a = {"a", "b", "c"}
        b = {"b", "c", "d"}
        # intersection = {b, c}, union = {a, b, c, d}
        assert abs(jaccard_similarity(a, b) - 0.5) < 0.01

    def test_both_empty(self):
        assert jaccard_similarity(set(), set()) == 1.0

    def test_one_empty(self):
        assert jaccard_similarity(set(), {"a"}) == 0.0


class TestTitlesAreSimilar:
    def test_identical_titles_similar(self):
        is_sim, conf = titles_are_similar("Economy Grows 3%", "Economy Grows 3%")
        assert is_sim
        assert conf == 1.0

    def test_syndicated_titles_similar(self):
        """Same story republished with site-name suffix should match."""
        a = "Treasury announces new bond program | Reuters"
        b = "Treasury announces new bond program - Bloomberg"
        is_sim, conf = titles_are_similar(a, b)
        assert is_sim

    def test_clearly_different_titles_not_similar(self):
        a = "Soccer team wins championship"
        b = "Scientists discover new exoplanet"
        is_sim, conf = titles_are_similar(a, b)
        assert not is_sim

    def test_one_word_changed(self):
        """Titles differing by one word in a longer title should be similar."""
        a = "Government announces major economic reform package"
        b = "Government announces major economic stimulus package"
        is_sim, conf = titles_are_similar(a, b)
        assert is_sim

    def test_prefix_match(self):
        """Truncated title should be detected as similar to longer version.

        The shorter title must be >= 70% the length of the longer title
        and be a prefix of it.
        """
        # ratio: len("Fed keeps rates") / len("Fed keeps rates on hold") ~ 0.65 -- below threshold
        # Use a case where the shorter is clearly >= 70% of the longer
        full = "Federal Reserve keeps rates on hold"
        truncated = "Federal Reserve keeps rates"
        # ratio = 26/35 = 0.74 > 0.70, and truncated IS a prefix of full
        is_sim, conf = titles_are_similar(full, truncated)
        assert is_sim

    def test_korean_identical_titles(self):
        a = "정부 긴급 경제 대책 발표"
        b = "정부 긴급 경제 대책 발표"
        is_sim, conf = titles_are_similar(a, b)
        assert is_sim
        assert conf == 1.0

    def test_korean_near_identical_titles(self):
        """Korean titles differing by one character should be flagged."""
        a = "서울 지하철 9호선 연장 공사 시작"
        b = "서울 지하철 9호선 연장 공사 착수"
        is_sim, conf = titles_are_similar(a, b)
        assert is_sim

    def test_empty_title_not_similar(self):
        is_sim, conf = titles_are_similar("", "Some title")
        assert not is_sim

    def test_breaking_prefix_stripped_before_comparison(self):
        """[속보] and [Breaking] prefixes should not prevent a match."""
        a = "[속보] 금리 동결 결정"
        b = "금리 동결 결정"
        is_sim, conf = titles_are_similar(a, b)
        assert is_sim


# ---------------------------------------------------------------------------
# DedupEngine tests — Level 1: URL
# ---------------------------------------------------------------------------

class TestDedupEngineURLLevel:
    def test_same_url_detected_as_duplicate(self):
        with _engine() as engine:
            aid = _article_id()
            engine.is_duplicate(
                url="https://example.com/article/123",
                title="Test Article",
                body="Some body content " * 10,
                source_id="example",
                article_id=aid,
            )
            result = engine.is_duplicate(
                url="https://example.com/article/123",
                title="Different Title",
                body="Completely different content " * 10,
                source_id="example",
                article_id=_article_id(),
            )
        assert result.is_duplicate
        assert result.level == 1
        assert result.confidence == 1.0

    def test_url_with_tracking_params_same_as_clean(self):
        with _engine() as engine:
            aid = _article_id()
            engine.is_duplicate(
                url="https://example.com/article/123",
                title="Test Article",
                body="Some body content " * 10,
                source_id="example",
                article_id=aid,
            )
            result = engine.is_duplicate(
                url="https://www.example.com/article/123?utm_source=twitter&fbclid=abc",
                title="Test Article",
                body="Some body content " * 10,
                source_id="example",
                article_id=_article_id(),
            )
        assert result.is_duplicate
        assert result.level == 1

    def test_different_urls_not_duplicate_at_url_level(self):
        """Two distinct URLs should not produce a Level 1 (URL) duplicate verdict."""
        with _engine() as engine:
            engine.is_duplicate(
                url="https://example.com/article/123",
                title="Article One",
                body="Article one content " * 10,
                source_id="example",
                article_id=_article_id(),
            )
            result = engine.is_duplicate(
                url="https://example.com/article/456",
                title="Article Two",
                body="Article two content " * 10,
                source_id="example",
                article_id=_article_id(),
            )
        # Different URL: should NOT be a Level-1 duplicate
        assert result.level != 1


# ---------------------------------------------------------------------------
# DedupEngine tests — Level 2: Title
# ---------------------------------------------------------------------------

class TestDedupEngineTitleLevel:
    def test_same_story_different_url_detected_by_title(self):
        """Cross-outlet syndication: same title, different URL."""
        with _engine() as engine:
            engine.is_duplicate(
                url="https://reuters.com/article/fed-rate-123",
                title="Federal Reserve holds rates steady amid inflation concerns",
                body="The Federal Reserve decided to hold interest rates steady " * 5,
                source_id="reuters",
                article_id=_article_id(),
            )
            result = engine.is_duplicate(
                url="https://bloomberg.com/news/fed-rate-456",
                title="Federal Reserve holds rates steady amid inflation concerns",
                body="Different publication body text about the fed rate decision " * 5,
                source_id="bloomberg",
                article_id=_article_id(),
            )
        assert result.is_duplicate
        assert result.level == 2

    def test_site_name_suffix_stripped_before_comparison(self):
        with _engine() as engine:
            engine.is_duplicate(
                url="https://reuters.com/article/energy-123",
                title="Oil prices surge on supply cuts | Reuters",
                body="Oil prices climbed significantly today following OPEC announcements " * 4,
                source_id="reuters",
                article_id=_article_id(),
            )
            result = engine.is_duplicate(
                url="https://apnews.com/article/energy-456",
                title="Oil prices surge on supply cuts - AP News",
                body="Completely different body text about oil market movements " * 4,
                source_id="apnews",
                article_id=_article_id(),
            )
        assert result.is_duplicate
        assert result.level == 2


# ---------------------------------------------------------------------------
# DedupEngine tests — Level 3: SimHash
# ---------------------------------------------------------------------------

class TestDedupEngineSimHashLevel:
    def test_near_duplicate_body_detected(self):
        """Article with slightly reworded body should be flagged at level 3."""
        # Bodies differ by only 1 word: "news" → "announcement" in sentence 5.
        # Very high overlap (>95% token identity) ensures SimHash detects near-dup.
        base_body = (
            "The central bank announced its decision to maintain interest rates at the "
            "current level of 5.25 percent, citing ongoing concerns about core inflation "
            "remaining above the 2 percent target. The decision was unanimous among the "
            "twelve voting members of the monetary policy committee, who met for two days. "
            "Markets reacted positively to the news with bond yields declining slightly. "
            "Economists predict the central bank will hold rates steady through Q3. "
            "The currency strengthened by half a percentage point on the statement."
        )
        dup_body = (
            "The central bank announced its decision to maintain interest rates at the "
            "current level of 5.25 percent, citing ongoing concerns about core inflation "
            "remaining above the 2 percent target. The decision was unanimous among the "
            "twelve voting members of the monetary policy committee, who met for two days. "
            "Markets reacted positively to the announcement with bond yields declining slightly. "
            "Economists predict the central bank will hold rates steady through Q3. "
            "The currency strengthened by half a percentage point on the statement."
        )
        with _engine() as engine:
            engine.is_duplicate(
                url="https://reuters.com/article/rates-001",
                title="Central Bank Holds Rates at 5.25%",
                body=base_body,
                source_id="reuters",
                article_id=_article_id(),
            )
            result = engine.is_duplicate(
                url="https://bloomberg.com/article/rates-002",
                title="Completely Different Title About Technology",
                body=dup_body,
                source_id="bloomberg",
                article_id=_article_id(),
            )
        assert result.is_duplicate
        assert result.level == 3

    def test_completely_different_body_not_duplicate(self):
        """Completely different article body should not be flagged."""
        with _engine() as engine:
            engine.is_duplicate(
                url="https://example.com/article/sports-001",
                title="Team Wins Championship",
                body=(
                    "The home team secured their championship victory with a final score "
                    "of 3-1 in last night's decisive match. The winning goal was scored in "
                    "the 87th minute by the team captain, drawing roars from the 60,000 fans. "
                    "This marks the club's first title in over a decade of competition."
                ),
                source_id="sports",
                article_id=_article_id(),
            )
            result = engine.is_duplicate(
                url="https://example.com/article/science-002",
                title="New Exoplanet Discovered",
                body=(
                    "Astronomers have detected a new Earth-like exoplanet orbiting within "
                    "the habitable zone of a nearby star system. The discovery was made using "
                    "advanced spectroscopic analysis combined with data from the space telescope. "
                    "Researchers believe the planet could potentially support liquid water."
                ),
                source_id="science",
                article_id=_article_id(),
            )
        assert not result.is_duplicate

    def test_empty_body_not_flagged_as_duplicate(self):
        """Empty body should not produce SimHash false positive."""
        with _engine() as engine:
            # Register an article with empty body
            engine.is_duplicate(
                url="https://example.com/article/paywall-001",
                title="Paywalled Article One",
                body="",  # empty body (paywall)
                source_id="premium",
                article_id=_article_id(),
            )
            result = engine.is_duplicate(
                url="https://example.com/article/paywall-002",
                title="Paywalled Article Two",
                body="",
                source_id="premium",
                article_id=_article_id(),
            )
        # Should NOT flag as SimHash duplicate (both have 0 fingerprint)
        assert result.level != 3 or not result.is_duplicate


# ---------------------------------------------------------------------------
# DedupEngine: full cascade
# ---------------------------------------------------------------------------

class TestDedupEngineCascade:
    def test_unique_article_not_duplicate(self):
        with _engine() as engine:
            result = engine.is_duplicate(
                url="https://example.com/article/unique-001",
                title="Unique Article About Technology Innovation",
                body="A groundbreaking new technology has been developed by researchers " * 5,
                source_id="tech",
                article_id=_article_id(),
            )
        assert not result.is_duplicate
        assert result.level == 0
        assert result.reason == "unique"

    def test_url_match_short_circuits_simhash(self):
        """URL match at level 1 should not escalate to SimHash."""
        with _engine() as engine:
            engine.is_duplicate(
                url="https://example.com/article/001",
                title="Test Article",
                body="Content " * 20,
                source_id="test",
                article_id=_article_id(),
            )
            result = engine.is_duplicate(
                url="https://www.example.com/article/001?utm_source=twitter",
                title="Test Article",
                body="Content " * 20,
                source_id="test",
                article_id=_article_id(),
            )
        assert result.level == 1  # Short-circuited at URL level

    def test_dedup_result_unique_factory(self):
        result = DedupResult.unique()
        assert not result.is_duplicate
        assert result.reason == "unique"
        assert result.match_id is None
        assert result.level == 0
        assert result.confidence == 0.0


# ---------------------------------------------------------------------------
# DedupEngine: statistics
# ---------------------------------------------------------------------------

class TestDedupEngineStats:
    def test_stats_increments_on_registration(self):
        with _engine() as engine:
            initial = engine.stats()
            assert initial["total_urls"] == 0
            assert initial["total_fingerprints"] == 0

            engine.is_duplicate(
                url="https://example.com/article/001",
                title="Article One",
                body="Long enough content for SimHash computation here. " * 5,
                source_id="test",
                article_id=_article_id(),
            )
            after = engine.stats()
            assert after["total_urls"] == 1
            assert after["total_fingerprints"] == 1

    def test_duplicate_does_not_double_count(self):
        with _engine() as engine:
            aid = _article_id()
            engine.is_duplicate(
                url="https://example.com/article/001",
                title="Article One",
                body="Long enough content for SimHash computation here. " * 5,
                source_id="test",
                article_id=aid,
            )
            engine.is_duplicate(
                url="https://example.com/article/001",
                title="Article One",
                body="Long enough content for SimHash computation here. " * 5,
                source_id="test",
                article_id=_article_id(),
            )
            stats = engine.stats()
            assert stats["total_urls"] == 1  # Not doubled


# ---------------------------------------------------------------------------
# DedupEngine: batch processing
# ---------------------------------------------------------------------------

class TestDedupEngineBatch:
    def test_batch_in_order_first_unique_rest_duplicate(self):
        """Within a batch, the first occurrence is unique; subsequent are duplicates."""
        articles = [
            {
                "url": "https://example.com/article/001",
                "title": "Batch Article One",
                "body": "Content for batch article one. " * 5,
                "source_id": "test",
                "article_id": _article_id(),
            },
            {
                "url": "https://example.com/article/001",  # same URL
                "title": "Batch Article One",
                "body": "Content for batch article one. " * 5,
                "source_id": "test",
                "article_id": _article_id(),
            },
        ]
        with _engine() as engine:
            results = engine.is_duplicate_batch(articles)
        assert len(results) == 2
        assert not results[0].is_duplicate
        assert results[1].is_duplicate

    def test_batch_all_unique(self):
        # Use highly distinct bodies to avoid accidental SimHash collision
        bodies = [
            "The stock market surged today as investors cheered strong earnings reports "
            "from major technology companies. Analysts say the rally could continue if "
            "upcoming economic data meets expectations for sustained growth.",
            "Scientists at CERN have detected unusual particle interactions during the "
            "latest high-energy collider experiment. The findings could reshape our "
            "understanding of quantum field theory and dark matter candidates.",
            "Heavy rainfall in Southeast Asia has triggered devastating floods across "
            "multiple provinces, displacing thousands of families from their homes. "
            "Emergency relief teams have been mobilized to provide immediate assistance.",
            "A groundbreaking clinical trial has demonstrated remarkable success in "
            "treating late-stage pancreatic cancer using a novel immunotherapy approach. "
            "Researchers report significant tumor reduction in over sixty percent of patients.",
            "The international space station crew successfully completed a seven-hour "
            "spacewalk to repair a critical cooling system component. Mission control "
            "confirmed all objectives were achieved during the extravehicular activity.",
        ]
        titles = [
            "Stock Market Surges on Tech Earnings",
            "CERN Detects Unusual Particle Interactions",
            "Devastating Floods Hit Southeast Asia",
            "Clinical Trial Shows Cancer Breakthrough",
            "Space Station Crew Completes Spacewalk",
        ]
        articles = [
            {
                "url": f"https://example.com/article/{i}",
                "title": titles[i],
                "body": bodies[i],
                "source_id": "test",
                "article_id": _article_id(),
            }
            for i in range(5)
        ]
        with _engine() as engine:
            results = engine.is_duplicate_batch(articles)
        assert all(not r.is_duplicate for r in results)


# ---------------------------------------------------------------------------
# Thread safety
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Purge expired tests
# ---------------------------------------------------------------------------

class TestDedupEnginePurgeExpired:
    """Tests for DedupEngine.purge_expired() TTL purge."""

    def test_purge_safety_guard_rejects_short_ttl(self):
        """TTL < 7 must raise ValueError to prevent accidental full purge."""
        with _engine() as engine:
            with pytest.raises(ValueError, match="Minimum is 7"):
                engine.purge_expired(ttl_days=3)

    def test_purge_empty_db_returns_zero(self):
        """Purge on fresh DB should return zero counts without error."""
        with _engine() as engine:
            result = engine.purge_expired(ttl_days=90)
            assert result == {"urls_purged": 0, "hashes_purged": 0}

    def test_purge_removes_old_entries(self):
        """Entries older than TTL are deleted; recent entries are kept."""
        from datetime import datetime, timedelta, timezone

        with _engine() as engine:
            # Insert an article (registers in both tables with current timestamp)
            engine.is_duplicate(
                url="https://example.com/recent",
                title="Recent Article Title",
                body="Recent article body text with enough words for SimHash. " * 5,
                source_id="test",
                article_id=_article_id(),
            )
            before = engine.stats()
            assert before["total_urls"] == 1

            # Purge with 90-day TTL — nothing should be deleted (just inserted)
            result = engine.purge_expired(ttl_days=90)
            assert result["urls_purged"] == 0
            assert result["hashes_purged"] == 0

            # Manually backdate the entry to 100 days ago
            old_ts = (datetime.now(timezone.utc) - timedelta(days=100)).isoformat()
            engine._conn.execute(
                "UPDATE seen_urls SET first_seen = ?", (old_ts,)
            )
            engine._conn.execute(
                "UPDATE content_hashes SET seen_at = ?", (old_ts,)
            )
            engine._conn.commit()

            # Now purge should remove it
            result = engine.purge_expired(ttl_days=90)
            assert result["urls_purged"] == 1
            assert result["hashes_purged"] >= 1

            after = engine.stats()
            assert after["total_urls"] == 0


# ---------------------------------------------------------------------------

class TestDedupEngineThreadSafety:
    def test_concurrent_is_duplicate_no_exception(self):
        """Multiple threads calling is_duplicate() simultaneously must not raise."""
        errors: list[Exception] = []

        def _worker(engine: DedupEngine, idx: int) -> None:
            try:
                engine.is_duplicate(
                    url=f"https://example.com/article/{idx}",
                    title=f"Concurrent Article {idx}",
                    body=f"Content for concurrent article number {idx}. " * 5,
                    source_id="test",
                    article_id=str(uuid.uuid4()),
                )
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        with _engine() as engine:
            threads = [
                threading.Thread(target=_worker, args=(engine, i))
                for i in range(20)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

        assert not errors, f"Thread safety errors: {errors}"
