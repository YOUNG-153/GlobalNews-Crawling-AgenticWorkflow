# GlobalNews Agentic Workflow — Auto-Build System

Build a fully operational news crawling + big data analysis system that automatically collects articles from 44 news sites worldwide, analyzes them with 56 techniques, and classifies signals into a 5-Layer hierarchy (Fad → Short → Mid → Long → Singularity).

## Overview

- **Input**: PRD (`coding-resource/PRD.md`) + 44 news site URLs + user requirements
- **Output**: Fully operational Python-based news crawling + analysis system with cron automation
- **Frequency**: One-time build (the built system then runs daily via cron)
- **Autopilot**: disabled
- **pACS**: enabled

**Absolute Objective**: Build a news crawling service **automatically** via an AI agentic workflow automation system. Every step, every agent, every line of code serves this singular purpose: a system that crawls 44 news sites, extracts title/date/body/source URL without exception (these mandatory fields enable users to perform diverse Q&A queries on the crawled data for additional insights), collects **ALL news published within 24 hours with zero omissions**, and **never gives up until the mission is complete — 무한 반복**. The 4-level retry system (NetworkGuard 5 × Standard+TotalWar 2 × Crawler 3 × Pipeline 3 = 90 automated attempts) provides near-infinite persistence; after exhausting all 90 automated attempts, Tier 6 escalates to Claude Code interactive analysis for manual breakthrough — the pipeline does not terminate, it escalates. Self-modifies its own Python code in real-time when blocked.

**Dual Workflow Context**: This document defines **Workflow A** — the 20-step agentic build system. Upon completion (status: complete), the built system operates as a daily crawling+analysis pipeline. A separate **Workflow B** (Insight Analytics Pipeline, `src/insights/`, 7 modules M1-M7) was subsequently added as a fully decoupled consumer of Workflow A's Stage 1-4 outputs. Workflow B has its own runtime SOT (`data/insights/insight_state.json`), separate configuration (`data/config/insights.yaml`), and independent execution (`main.py --mode insight`). See `research/bigdata-insight-workflow-design.md` for Workflow B architecture.

**Hard Constraints** (from PRD):
- **C1 — Claude API = $0**: Claude API is NEVER called. ALL analysis (NLP, time series, network, signal classification) runs on local Python libraries only. Claude Code subscription covers orchestration only.
- **C2 — Conductor Pattern**: Claude Code generates Python scripts → executes via Bash → reads results → decides next step. It never processes data directly.
- **C3 — MacBook M2 Pro 16GB**: Entire pipeline runs on a single machine. No cloud GPU.
- **C4 — Output = Parquet/SQLite**: No dashboards, no reports, no visualization. Structured data only.
- **C5 — Legal Crawling**: robots.txt respected, rate limiting enforced, no personal data collected.

**Target Sites (44)**:

| # | Group | Sites |
|---|-------|-------|
| A | Korean Major Dailies (5) | chosun.com, joongang.co.kr, donga.com, hani.co.kr, yna.co.kr |
| B | Korean Economy (4) | mk.co.kr, hankyung.com, fnnews.com, mt.co.kr |
| C | Korean Niche (3) | nocutnews.co.kr, kmib.co.kr, ohmynews.com |
| D | Korean IT/Science (7) | 38north.org, bloter.net, etnews.com, sciencetimes.co.kr, zdnet.co.kr, irobotnews.com, techneedle.com |
| E | US/English Major (12) | marketwatch.com, voakorea.com, huffingtonpost.com, nytimes.com, ft.com, wsj.com, latimes.com, buzzfeed.com, nationalpost.com, edition.cnn.com, bloomberg.com, afmedios.com |
| F | Asia-Pacific (6) | people.com.cn, globaltimes.cn, scmp.com, taiwannews.com, yomiuri.co.jp, thehindu.com |
| G | Europe/Middle East (7) | thesun.co.uk, bild.de, lemonde.fr/en/, themoscowtimes.com, arabnews.com, aljazeera.com, israelhayom.com |

---

## Inherited DNA (Parent Genome)

> This workflow inherits the complete genome of AgenticWorkflow.
> Purpose varies by domain; the genome is identical. See `soul.md §0`.

**Constitutional Principles** (adapted to this workflow's domain):

1. **Quality Absolutism** — The ONLY metric is whether the built system successfully crawls all 44 sites and produces accurate analysis. Speed, token cost, and effort are irrelevant. A crawling failure on even one site is a quality failure.
2. **Single-File SOT** — `.claude/state.yaml` holds all shared state. Only Orchestrator/Team Lead writes. All agents read-only.
3. **Code Change Protocol** — Every Python script modification follows Intent → Ripple Analysis → Change Plan. Coding Anchor Points (CAP-1~4): think before coding, simplicity first, goal-driven execution, surgical changes.

**Inherited Patterns**:

| DNA Component | Inherited Form |
|--------------|---------------|
| 3-Phase Structure | Research → Planning → Implementation |
| SOT Pattern | `.claude/state.yaml` — single writer (Orchestrator/Team Lead) |
| 4-Layer QA | L0 Anti-Skip → L1 Verification → L1.5 pACS → L2 Adversarial Review |
| P1 Hallucination Prevention | Deterministic validation scripts (`validate_*.py`) |
| P2 Expert Delegation | Specialized sub-agents: crawling, anti-block, NLP, time series, integration |
| Safety Hooks | `block_destructive_commands.py` — dangerous command blocking |
| Adversarial Review | `@reviewer` — final code quality + security review |
| Decision Log | `autopilot-logs/` — transparent decision tracking |
| Context Preservation | Snapshot + Knowledge Archive + RLM restoration |

**Domain-Specific Gene Expression**:
- **P1 (Data Refinement)** genes are STRONGLY expressed — crawled HTML → Python extraction → clean text → AI analysis. Every data handoff has explicit pre/post-processing.
- **P2 (Expert Delegation)** genes are STRONGLY expressed — crawling specialists, NLP specialists, time series specialists each operate in isolated expert contexts.
- **CCP (Code Change Protocol)** genes are STRONGLY expressed — this workflow PRODUCES code. Every script must follow CCP rigorously.
- **Never Give Up (D2)** gene is uniquely expressed — 3-tier retry system, 6-tier escalation, self-modifying code. This gene has no equivalent in the parent genome; it is a domain-specific mutation.

---

## Research

### 1. Target Site Reconnaissance & Classification
- **Pre-processing**: `python3 scripts/extract_site_urls.py` — Parse the 44 URLs from `sources.yaml`, output structured site list with domain, expected language, and group classification
- **Agent**: `@site-recon` (sonnet)
- **Verification**:
  - [ ] All 44 sites visited and analyzed (zero omissions)
  - [ ] Each site entry contains: RSS availability (Y/N + URL), sitemap availability (Y/N + URL), dynamic loading detection (Y/N), paywall detection (Y/N), bot-blocking level (none/low/medium/high/extreme), primary language, section count estimate
  - [ ] Sites classified into crawling difficulty tiers: Easy (RSS+sitemap), Medium (DOM navigation), Hard (dynamic+anti-bot), Extreme (paywall+fingerprint)
  - [ ] Output format compatible with Step 5 architecture design (structured YAML/JSON)
- **Task**: Visit each of the 44 target news sites. For each site: (1) Check robots.txt and extract Crawl-delay, Disallow paths. (2) Probe for sitemap.xml and RSS feeds. (3) Analyze homepage structure — static HTML vs JavaScript-rendered, infinite scroll, load-more buttons. (4) Test basic HTTP request with standard UA to detect immediate blocking. (5) Classify difficulty tier. (6) Record mandatory field extraction feasibility: title (`<h1>`, `og:title`), published_at (`article:published_time`), body (article content area), source URL. Reference: PRD §5.1.1 Dynamic-First, §4.3 3-Tier URL Discovery, §4.4 Legal Framework.
- **Output**: `research/site-reconnaissance.md`
- **Review**: `@fact-checker`
- **Translation**: `@translator` → `research/site-reconnaissance.ko.md`
- **Post-processing**: `python3 scripts/generate_sources_yaml_draft.py` — Convert reconnaissance data into draft `sources.yaml` with per-site configurations

### 2. (team) Technology Stack Validation
- **Team**: `tech-validation-team`
- **Checkpoint Pattern**: standard
- **Tasks**:
  - `@dep-validator` (sonnet): Install and verify ALL Python packages from PRD §8.1 (crawling: playwright, patchright, trafilatura, fundus, newspaper4k, httpx, feedparser, beautifulsoup4, lxml, simhash, datasketch, undetected-chromedriver, apify-fingerprint-suite; NLP: kiwipiepy, spacy, sentence-transformers, transformers, keybert, bertopic, model2vec, setfit, gensim, fasttext, langdetect; timeseries: prophet, ruptures, statsmodels, tigramite, pywt, scipy, lifelines; network: networkx, igraph, hdbscan, scikit-learn, python-louvain; storage: pyarrow, duckdb, sqlite-vec, pandas, polars, pyyaml). Record install success/failure, version, and any compatibility issues. Run `playwright install chromium`. Test basic imports.
    - Output: `research/dependency-validation.md`
  - `@nlp-benchmarker` (sonnet): Download and benchmark Korean NLP models on M2 Pro: (1) Kiwi morphological analysis — speed + accuracy on 100 news sentences. (2) KoBERT sentiment (news domain F1=94%) — benchmark on sample data. (3) KcELECTRA sentiment (web/informal text F1=90.6%) — benchmark as fallback. (4) KLUE-RoBERTa NER — accuracy on sample. (5) snowflake-arctic-embed-l-v2.0-ko — embedding generation speed. (6) BERTopic + Model2Vec — topic modeling on 500 sample texts. Record peak memory for each model. Reference: PRD §5.2.5 Korean NLP Stack, §8.3 Memory Budget (target: peak ≤ 7GB).
    - Output: `research/nlp-benchmark.md`
  - `@memory-profiler` (sonnet): Profile memory usage of concurrent model loading scenarios on M2 Pro 16GB. Test: (1) Playwright browser + Trafilatura. (2) SBERT + BERTopic + KoBERT sequential loading. (3) Full 8-stage pipeline simulation with gc.collect() between stages. Determine if 16GB is sufficient for full pipeline or if sequential execution with explicit unloading is required. Reference: PRD §8.3 Memory Budget.
    - Output: `research/memory-profile.md`
- **Join**: All teammates complete → Team Lead merges into unified tech validation report
- **SOT Write**: Team Lead only — `state.yaml` update
- **Verification**:
  - [ ] All packages from PRD §8.1 tested (zero omissions — count ≥ 40 packages)
  - [ ] Korean NLP models benchmarked with quantitative metrics (F1, speed, memory)
  - [ ] Memory profiling completed with peak measurements for each scenario
  - [ ] Clear GO/NO-GO recommendation for each package and model
  - [ ] Incompatible packages identified with alternative recommendations
- **Output**: `research/tech-validation.md` (merged)
- **Translation**: none (technical data)

### 3. Crawling Feasibility Analysis
- **Pre-processing**: `python3 scripts/merge_recon_and_deps.py` — Combine Step 1 site reconnaissance with Step 2 tech validation into unified feasibility input
- **Agent**: `@crawl-analyst` (opus)
- **Verification**:
  - [ ] All 44 sites have assigned primary crawling strategy (RSS/Sitemap/DOM/Playwright) with fallback chain
  - [ ] Per-site rate limiting policy defined (respecting robots.txt Crawl-delay)
  - [ ] High-risk sites (difficulty: Hard/Extreme) have explicit 6-Tier escalation plans
  - [ ] Legal compliance checklist completed for each site (robots.txt respected, rate limit defined)
  - [ ] 4-level retry parameters defined: NetworkGuard(5) × Standard+TotalWar mode(2) × Crawler round(3) × Pipeline restart(3) = theoretical max 90 attempts per article
  - [ ] User-Agent rotation strategy designed (pool size ≥ 50 UAs, rotation policy)
  - [ ] Total estimated daily crawl time < 2 hours for all 44 sites (source: Step 1 data)
- **Task**: For each of the 44 sites, design a concrete crawling approach: (1) Primary method (RSS/Sitemap/DOM/Playwright) based on Step 1 reconnaissance. (2) Fallback chain when primary fails. (3) Rate limiting parameters (base delay, max concurrent requests). (4) Anti-block strategy tier assignment based on blocking level. (5) Mandatory field extraction method (title/date/body/URL) per site. (6) Estimate article volume per day. (7) Identify sites requiring undetected-chromedriver (paywall/extreme anti-bot). (8) Design User-Agent rotation pool. (9) Define 4-level retry architecture: NetworkGuard level (5 retries per HTTP request with exponential backoff), Standard+TotalWar mode switch (×2 — first pass Standard mode, second pass TotalWar with undetected-chromedriver), Crawler level (3 rounds per site with increasing delays between rounds), Pipeline level (3 full restarts preserving already-collected URLs via dedup). Total theoretical max: 5 × 2 × 3 × 3 = 90 attempts per article. Reference: PRD §5.1.2 Anti-Block, §4.3 3-Tier URL Discovery, user requirements for retry and UA rotation.
- **Output**: `research/crawling-feasibility.md`
- **Review**: `@fact-checker`
- **Translation**: `@translator` → `research/crawling-feasibility.ko.md`

### 4. (human) Research Review & Prioritization
- **Action**: Review all research outputs. Confirm: (1) Site difficulty classifications are accurate. (2) Technology stack is viable on M2 Pro 16GB. (3) Per-site crawling strategies are appropriate. (4) Rate limiting policies are conservative enough to avoid DDoS accusations but aggressive enough to collect all articles within 24h.
- **Command**: `/review-research`
- **Autopilot Default**: Accept all research findings; prioritize Hard/Extreme sites for early testing

---

## Planning

### 5. System Architecture Blueprint
- **Pre-processing**: `python3 scripts/filter_prd_architecture.py` — Extract PRD §6 (Architecture), §7 (Output Specs), §8 (Tech Stack) sections for focused architect input
- **Agent**: `@system-architect` (opus)
- **Verification**:
  - [ ] Staged Monolith 4-layer architecture defined: Orchestration → Crawling → Analysis → Storage (source: PRD §6.1)
  - [ ] Complete directory structure specified matching PRD §7.3 (data/raw, data/processed, data/features, data/analysis, data/output, data/models, data/logs, data/config)
  - [ ] Module interface contracts defined for each layer boundary (input format, output format, error handling)
  - [ ] Parquet schemas match PRD §7.1 exactly (articles.parquet, analysis.parquet, signals.parquet — all columns)
  - [ ] SQLite schemas match PRD §7.2 exactly (articles_fts, article_embeddings, signals_index, topics_index, crawl_status)
  - [ ] `sources.yaml` schema defined with per-site configuration fields
  - [ ] `pipeline.yaml` schema defined with stage-level configuration
  - [ ] Conductor Pattern (Generate→Execute→Read→Decide) explicitly mapped to each module
  - [ ] Memory management strategy documented (sequential model loading, gc.collect between stages)
  - [ ] Data flow diagram matches PRD §6.3: sources.yaml → raw(JSONL) → processed(Parquet) → features → analysis → timeseries → cross_analysis → signals → output
- **Task**: Design the complete system architecture for the GlobalNews crawling + analysis system. This is the technical blueprint that all Implementation steps will follow. (1) Define 4-layer architecture with clear module boundaries. (2) Specify all file/directory structures. (3) Define Parquet schemas (articles, analysis, signals, topics) per PRD §7.1. (4) Define SQLite schemas (FTS5, vec, indexes) per PRD §7.2. (5) Design `sources.yaml` format with fields: url, name, language, rss_url, sitemap_url, sections[], difficulty_tier, rate_limit_seconds, anti_block_tier, enabled. (6) Design `pipeline.yaml` with stage configurations. (7) Define Python module structure: `src/crawling/`, `src/analysis/`, `src/storage/`, `src/utils/`, `src/config/`. (8) Design shared utilities: logging, error handling, config loading. (9) Define inter-module data contracts. (10) Memory management plan for 16GB constraint. Reference: PRD §6 entire section, §7 entire section.
- **Output**: `planning/architecture-blueprint.md`
- **Review**: `@reviewer`
- **Translation**: `@translator` → `planning/architecture-blueprint.ko.md`
- **Post-processing**: `python3 scripts/validate_schema_completeness.py` — Verify all PRD §7.1 and §7.2 columns are present in the architecture document

### 6. (team) Per-Site Crawling Strategy Design
- **Team**: `crawl-strategy-team`
- **Checkpoint Pattern**: standard
- **Pre-processing**: `python3 scripts/split_sites_by_group.py` — Divide 44 sites into 4 balanced groups for parallel strategy design, including Step 1 reconnaissance data per site
- **Tasks**:
  - `@crawl-strategist-kr` (opus): Design detailed crawling strategies for Korean sites (19 sites: Groups A+B+C+D). For each site: URL discovery method, section navigation approach, article extraction selectors (CSS/XPath for title, date, body), anti-block tier, rate limit, expected daily article count. Special attention to: chosun.com (infinite scroll), hankyung.com (paywall), dynamic loading sites.
    - Output: `planning/crawl-strategy-korean.md`
  - `@crawl-strategist-en` (opus): Design detailed crawling strategies for English-language Western sites (12 sites: Group E). For each site: URL discovery method, Fundus/Trafilatura compatibility, paywall handling (nytimes, ft, wsj, bloomberg require undetected-chromedriver), section navigation. Special attention to paywalled sites.
    - Output: `planning/crawl-strategy-english.md`
  - `@crawl-strategist-asia` (opus): Design detailed crawling strategies for Asia-Pacific sites (6 sites: Group F). For each site: language detection, character encoding handling, non-Latin URL patterns, section navigation. Special attention to: people.com.cn (Chinese encoding), yomiuri.co.jp (Japanese encoding).
    - Output: `planning/crawl-strategy-asia.md`
  - `@crawl-strategist-global` (opus): Design detailed crawling strategies for Europe/Middle East sites (7 sites: Group G). For each site: multi-language handling, RTL text support (Arabic/Hebrew sites), geo-blocking detection, section navigation. Special attention to: arabnews.com, aljazeera.com (Arabic+English), bild.de (German), lemonde.fr/en/ (French/English).
    - Output: `planning/crawl-strategy-global.md`
- **Join**: Team Lead merges all 4 strategy documents into unified per-site strategy matrix
- **SOT Write**: Team Lead only
- **Verification**:
  - [ ] All 44 sites have complete strategy entries (zero omissions)
  - [ ] Each entry specifies: primary method, fallback chain, CSS/XPath selectors for title+date+body+URL, rate limit, anti-block tier
  - [ ] Paywall sites explicitly marked with undetected-chromedriver requirement
  - [ ] User-Agent rotation pool defined (≥ 50 diverse UAs from real browsers)
  - [ ] Estimated total daily crawl time documented per group
  - [ ] Output format compatible with Step 10 crawler implementation (structured, parseable)
- **Output**: `planning/crawling-strategies.md` (merged)
- **Translation**: none (technical reference data — primarily tables and selectors)
- **Post-processing**: `python3 scripts/validate_44_site_coverage.py` — Verify all 44 URLs appear in the merged strategy document

### 7. Analysis Pipeline Detailed Design
- **Pre-processing**: `python3 scripts/filter_prd_analysis.py` — Extract PRD §5.2 (Analysis Engine) with all 56 techniques, §5.2.2 (8-Stage Pipeline), §5.2.3 (5-Layer Signal), §5.2.5 (Korean NLP Stack) for focused input
- **Agent**: `@pipeline-designer` (opus)
- **Verification**:
  - [ ] All 8 stages defined with explicit input/output formats (Parquet column names matching PRD §7.1)
  - [ ] All 56 analysis techniques mapped to specific stages with Python library assignments
  - [ ] 5-Layer signal classification rules defined with quantitative thresholds (source: PRD §5.2.3)
  - [ ] Dual-Pass analysis strategy specified: Pass 1 (title) → Pass 2 (body) per PRD §5.2.3
  - [ ] Singularity composite score formula defined with 7 indicators and initial weights (source: PRD §5.2.4, Appendix E)
  - [ ] Korean NLP stack confirmed: Kiwi (morphology), KoBERT (sentiment), KLUE-RoBERTa (NER), snowflake-arctic-embed (embedding), BERTopic+Model2Vec (topics)
  - [ ] Memory management plan per stage (model load → process → unload → gc.collect)
  - [ ] Each stage has estimated processing time per 1,000 articles (target: ≤ 30 min total per PRD §9.2)
  - [ ] Stage dependencies are strictly linear (no circular references)
  - [ ] Pipeline configuration mapped to `pipeline.yaml` schema from Step 5
- **Task**: Design the complete 8-stage analysis pipeline in implementation-ready detail. For each stage: (1) Input format (Parquet columns). (2) Processing logic with specific Python library calls. (3) Output format (Parquet columns). (4) Memory requirements. (5) Estimated processing time. (6) Error handling strategy. Stages: Stage 1 (Preprocessing: Kiwi+spaCy), Stage 2 (Feature Extraction: SBERT+TF-IDF+NER+KeyBERT), Stage 3 (Per-Article Analysis: sentiment+emotion+STEEPS), Stage 4 (Aggregation: BERTopic+HDBSCAN+NMF/LDA+Louvain), Stage 5 (Time Series: STL+PELT+Kleinberg+Prophet+Wavelet), Stage 6 (Cross-Analysis: Granger+PCMCI+co-occurrence+cross-lingual), Stage 7 (Signal Classification: 5-Layer rules+Novelty+BERTrend+singularity score), Stage 8 (Data Output: Parquet ZSTD+SQLite FTS5+sqlite-vec). Also design the Dual-Pass analysis flow and 5-Layer classification rules with thresholds. Reference: PRD §5.2 entire section.
- **Output**: `planning/analysis-pipeline-design.md`
- **Review**: `@reviewer`
- **Translation**: `@translator` → `planning/analysis-pipeline-design.ko.md`

### 8. (human) Architecture & Strategy Approval
- **Action**: Review and approve: (1) System architecture blueprint (Step 5). (2) Per-site crawling strategies (Step 6). (3) Analysis pipeline design (Step 7). Confirm all designs are implementation-ready.
- **Command**: `/review-architecture`
- **Autopilot Default**: Approve all designs; flag any sites where strategy seems insufficient

---

## Implementation

### 9. Project Infrastructure Scaffolding
- **Agent**: `@infra-builder` (opus)
- **Verification**:
  - [ ] Directory structure created matching Step 5 architecture blueprint exactly
  - [ ] `data/config/sources.yaml` populated with all 44 sites and their configurations from Step 6
  - [ ] `data/config/pipeline.yaml` created with all 8 stage configurations from Step 7
  - [ ] Python virtual environment created with all packages from Step 2 installed
  - [ ] `requirements.txt` generated with pinned versions for all packages
  - [ ] `src/` module structure created: `crawling/`, `analysis/`, `storage/`, `utils/`, `config/`
  - [ ] Shared utilities implemented: `src/utils/logging_config.py` (structured JSON logging), `src/utils/config_loader.py` (YAML loading), `src/utils/error_handler.py` (retry decorators, Circuit Breaker)
  - [ ] `src/config/constants.py` with all shared constants (retry counts, timeouts, file paths)
  - [ ] All `__init__.py` files present for proper Python package structure
  - [ ] `main.py` entry point created (skeleton with argument parsing for crawl/analyze/full modes)
  - [ ] Basic smoke test: `python3 main.py --help` executes without error
- **Task**: Create the complete project infrastructure for the GlobalNews system. (1) Create all directories per architecture blueprint. (2) Generate `sources.yaml` with all 44 sites — each entry: url, name, language, group, rss_urls[], sitemap_url, sections[], difficulty_tier, rate_limit_seconds, anti_block_tier, enabled, selectors (title_css, date_css, body_css, article_link_css). (3) Generate `pipeline.yaml` with 8-stage configuration. (4) Create venv and install all dependencies. (5) Generate pinned `requirements.txt`. (6) Create Python package structure with `__init__.py` files. (7) Implement shared utilities (logging, config, error handling, constants). (8) Create `main.py` entry point. Use Step 5 architecture blueprint as the authoritative source for all structural decisions. Use Step 6 strategies for `sources.yaml` content.
- **Output**: Project infrastructure (code files)
- **Translation**: none (code)
- **Post-processing**: `python3 -c "import src; print('Package structure OK')"` — Verify Python package imports work

### 10. (team) Crawling Core Engine Implementation
- **Team**: `crawl-engine-team`
- **Checkpoint Pattern**: dense
- **Pre-processing**: `python3 scripts/extract_architecture_crawling.py` — Filter Step 5 architecture blueprint to crawling layer specs + Step 6 strategy summary
- **Tasks**:
  - `@crawler-core-dev` (opus): Implement the core crawling engine: `src/crawling/crawler.py`. (1) NetworkGuard class with 5-retry exponential backoff per HTTP request (base=2s, max=30s, jitter). (2) 3-Tier URL Discovery: Tier 1 (RSS/Sitemap parser — feedparser + lxml), Tier 2 (DOM navigation — beautifulsoup4 + httpx), Tier 3 (Playwright/Patchright dynamic rendering). (3) Article extraction chain: Fundus → Trafilatura → Newspaper4k fallback. (4) Mandatory field extraction with validation: title, published_at, body, url — raise error if any missing (these fields are mandatory because users perform diverse Q&A queries on the crawled data for additional insights; incomplete records break downstream querying). (5) JSONL output writer with atomic file operations. (6) Per-site config loading from `sources.yaml`. (7) Rate limiter respecting per-site `rate_limit_seconds`. (8) Crawl state persistence (resume from last successful URL on restart). Reference: PRD §5.1.1, Step 5 architecture, Step 6 strategies.
    - **Checkpoints** (dense):
      - CP-1: NetworkGuard + HTTP client architecture → report to Team Lead
      - CP-2: 3-Tier URL Discovery + Article extraction chain → report
      - CP-3: Complete crawler with JSONL output + tests → final report + pACS
    - Output: `src/crawling/crawler.py`, `src/crawling/network_guard.py`, `src/crawling/url_discovery.py`, `src/crawling/article_extractor.py`
  - `@anti-block-dev` (opus): Implement the adaptive anti-block system: `src/crawling/anti_block.py`. (1) BlockDetector class — diagnose 7 block types (IP block, UA filter, rate limit, CAPTCHA, JS challenge, fingerprint, geo-block) from HTTP responses. (2) 6-Tier escalation engine: Tier 1 (delay adjustment 5→10→15s + UA rotation), Tier 2 (session/cookie cycling + Referer + header diversification), Tier 3 (Playwright/Patchright headless rendering), Tier 4 (Patchright + fingerprint-suite), Tier 5 (proxy rotation — configurable provider), Tier 6 (failure logging for Claude Code interactive analysis). (3) Circuit Breaker pattern: Closed→Open(5 consecutive failures, 30min wait)→Half-Open(single test). (4) **"Total War" mode**: `pip3 install undetected-chromedriver` then deploy undetected-chromedriver for paywall/extreme sites (nytimes, ft, wsj, bloomberg, hankyung) — this is the named escalation beyond standard Playwright. (5) Self-modifying strategy: when a tier fails, automatically escalate and log the failure pattern for future avoidance. (6) Strategy persistence: save successful strategies per-site to `data/config/site_profiles.json` for reuse. Reference: PRD §5.1.2, user requirement for self-modifying code.
    - **Checkpoints** (dense):
      - CP-1: BlockDetector 7-type diagnosis → report
      - CP-2: 6-Tier escalation + Circuit Breaker → report
      - CP-3: Complete anti-block with undetected-chromedriver + strategy persistence → final report + pACS
    - Output: `src/crawling/anti_block.py`, `src/crawling/block_detector.py`, `src/crawling/circuit_breaker.py`, `src/crawling/stealth_browser.py`
  - `@dedup-dev` (sonnet): Implement the deduplication engine: `src/crawling/dedup.py`. (1) URL normalizer — remove query params (utm_*, ref, etc.), normalize protocol/trailing slashes. (2) Content hash — SimHash for body text (95%+ similarity = duplicate). (3) Title similarity — Jaccard coefficient + Levenshtein edit distance for cross-outlet same-story detection. (4) Dedup database — SQLite-backed seen-URLs + content-hashes for persistence across runs. (5) Filter interface: `is_duplicate(url, title, body) → bool`. When duplicate detected, skip all extraction (title, date, body, URL not collected per user requirement). Reference: PRD §5.1.3, user requirement for dedup filtering.
    - Output: `src/crawling/dedup.py`, `src/crawling/url_normalizer.py`
  - `@ua-rotation-dev` (sonnet): Implement User-Agent rotation and session management: `src/crawling/ua_manager.py`. (1) UA pool with ≥ 50 real browser User-Agents (Chrome, Firefox, Safari, Edge — desktop + mobile variants, latest versions). (2) Weighted rotation — distribute UAs to simulate natural browser distribution. (3) Session manager — cookie jar per UA, session lifecycle (create, use N requests, retire). (4) Referer chain builder — generate realistic referer headers (Google Search → site homepage → section → article). (5) Header diversification — Accept-Language, Accept-Encoding, Connection headers matching UA browser type. (6) Integration interface for NetworkGuard: `get_request_headers(site_url) → dict`. Reference: User requirement for UA rotation to avoid bot classification.
    - Output: `src/crawling/ua_manager.py`, `src/crawling/session_manager.py`
- **Join**: All teammates complete → Team Lead verifies module interfaces are compatible → integration test
- **SOT Write**: Team Lead only
- **Verification**:
  - [ ] All 4 modules implement the interfaces defined in Step 5 architecture
  - [ ] NetworkGuard correctly retries 5 times with exponential backoff (unit test)
  - [ ] 3-Tier URL Discovery works: RSS parsing, DOM navigation, Playwright rendering (integration test on 3 sample sites)
  - [ ] Article extraction chain produces title, published_at, body, url for all test cases (≥ 5 sites sampled)
  - [ ] BlockDetector correctly identifies at least 4 of 7 block types in simulated scenarios
  - [ ] 6-Tier escalation progresses correctly from Tier 1 to Tier 6 (unit test)
  - [ ] Circuit Breaker state machine transitions correctly (Closed→Open→Half-Open)
  - [ ] Dedup correctly filters URL duplicates, content-similar articles, and cross-outlet same stories (unit test with 10 test cases)
  - [ ] UA rotation pool contains ≥ 50 UAs and distributes realistically (unit test)
  - [ ] All modules have proper error handling (no unhandled exceptions)
- **Output**: Crawling core engine (code files in `src/crawling/`)
- **Translation**: none (code)

### 11. (team) Site-Specific Crawler Adapters (44 sites)
- **Team**: `site-adapters-team`
- **Checkpoint Pattern**: dense
- **Pre-processing**: `python3 scripts/distribute_sites_to_teams.py` — Split 44 sites into 4 balanced groups with per-site strategy data from Step 6
- **Tasks**:
  - `@adapter-dev-kr-major` (opus): Implement crawling adapters for 11 Korean sites: chosun.com, joongang.co.kr, donga.com, hani.co.kr, yna.co.kr, mk.co.kr, hankyung.com, fnnews.com, mt.co.kr, nocutnews.co.kr, kmib.co.kr. Each adapter: site-specific CSS/XPath selectors for title/date/body, section URL patterns, pagination handling, special anti-block config. Use Step 6 Korean strategy document.
    - **Checkpoints**: CP-1 (3 sites done+tested), CP-2 (7 sites), CP-3 (all 11+full test)
    - Output: `src/crawling/adapters/kr_major/` (11 adapter files)
  - `@adapter-dev-kr-tech` (opus): Implement crawling adapters for 8 Korean IT/niche sites: ohmynews.com, 38north.org, bloter.net, etnews.com, sciencetimes.co.kr, zdnet.co.kr, irobotnews.com, techneedle.com. These sites often have simpler structures. Use Step 6 Korean strategy document.
    - **Checkpoints**: CP-1 (3 sites), CP-2 (6 sites), CP-3 (all 8+test)
    - Output: `src/crawling/adapters/kr_tech/` (8 adapter files)
  - `@adapter-dev-english` (opus): Implement crawling adapters for 12 English-language Western sites: marketwatch.com, voakorea.com, huffingtonpost.com, nytimes.com, ft.com, wsj.com, latimes.com, buzzfeed.com, nationalpost.com, edition.cnn.com, bloomberg.com, afmedios.com. Special attention to paywall sites (nytimes, ft, wsj, bloomberg) requiring undetected-chromedriver. Use Step 6 English strategy document.
    - **Checkpoints**: CP-1 (4 sites incl. 1 paywall), CP-2 (8 sites incl. all paywalls), CP-3 (all 12+test)
    - Output: `src/crawling/adapters/english/` (12 adapter files)
  - `@adapter-dev-multilingual` (opus): Implement crawling adapters for 13 multilingual sites: people.com.cn, globaltimes.cn, scmp.com, taiwannews.com, yomiuri.co.jp, thehindu.com, thesun.co.uk, bild.de, lemonde.fr/en/, themoscowtimes.com, arabnews.com, aljazeera.com, israelhayom.com. Handle character encoding (UTF-8, GB2312, Shift_JIS), RTL text (Arabic, Hebrew), multi-language sections. Use Step 6 Asia + Global strategy documents.
    - **Checkpoints**: CP-1 (4 sites incl. 1 CJK + 1 RTL), CP-2 (9 sites), CP-3 (all 13+test)
    - Output: `src/crawling/adapters/multilingual/` (13 adapter files)
- **Join**: Team Lead verifies all 44 adapters conform to the common interface, tests cross-adapter compatibility
- **SOT Write**: Team Lead only
- **Verification**:
  - [ ] All 44 sites have dedicated adapter files (zero omissions — verified by file count)
  - [ ] Each adapter successfully extracts title, published_at, body, source_url from at least 3 test articles (132 total test cases)
  - [ ] Paywall sites (nytimes, ft, wsj, bloomberg, hankyung) use undetected-chromedriver path
  - [ ] Non-Latin character encoding correctly handled (Chinese, Japanese, Korean, Arabic, Hebrew, German, French)
  - [ ] All adapters implement the common `SiteAdapter` interface from Step 10 core engine
  - [ ] Adapter registry (`src/crawling/adapters/__init__.py`) maps all 44 domains to their adapters
- **Output**: Site-specific adapters (code files in `src/crawling/adapters/`)
- **Translation**: none (code)
- **Post-processing**: `python3 scripts/verify_adapter_coverage.py` — Cross-check 44 URLs in `sources.yaml` against adapter registry

### 12. Crawling Pipeline Integration & 3-Tier Retry System
- **Pre-processing**: Verify Steps 10 and 11 outputs exist and pass basic import test
- **Agent**: `@integration-engineer` (opus)
- **Verification**:
  - [ ] `src/crawling/pipeline.py` orchestrates: load sources.yaml → iterate sites → adapter selection → URL discovery → article extraction → dedup → JSONL output
  - [ ] 4-level retry system implemented: NetworkGuard(5) × Standard+TotalWar escalation(2) × Crawler round(3) × Pipeline restart(3) = 90 automated attempts — near-infinite persistence ("임무 완수까지 무한 반복에 가까운 집요함"). After 90 automated retries exhausted, Tier 6 escalates to Claude Code interactive analysis (never silently terminates)
  - [ ] All crawled articles written to single consolidated JSONL file: `data/raw/YYYY-MM-DD/all_articles.jsonl` (user requirement: single file for downstream task reuse — the next workflow stage consumes this file directly)
  - [ ] Each JSONL record contains mandatory fields: title, published_at, body, source_url, source_name, language, crawled_at, content_hash (title/date/body/url are mandatory because users perform diverse Q&A queries on crawled data for additional insights)
  - [ ] URL dedup prevents re-collection on pipeline restart (already-collected URLs skipped)
  - [ ] Pipeline produces structured crawl report: per-site success/fail counts, total articles, blocked sites, time elapsed
  - [ ] End-to-end test: pipeline crawls ≥ 3 articles from at least 5 diverse sites (1 Korean, 1 English, 1 paywall, 1 multilingual, 1 with anti-bot)
  - [ ] `python3 main.py crawl --date today` executes full pipeline without unhandled errors
- **Task**: Integrate all crawling components into a unified pipeline. (1) Create `src/crawling/pipeline.py` — the main orchestrator that: loads `sources.yaml`, iterates through enabled sites, selects appropriate adapter, runs 3-Tier URL discovery, extracts articles, applies dedup filter, writes to consolidated JSONL. (2) Implement 3-tier retry architecture: Level 1 (NetworkGuard — 5 retries per HTTP request), Level 2 (Crawler — 3 rounds per site with increasing delays), Level 3 (Pipeline — 3 full restarts preserving already-collected URLs via dedup). Between standard attempts and retries, escalate anti-block tier (Standard → TotalWar with undetected-chromedriver). Total: 5 × 2 × 3 × 3 = 90 automated attempts per article — near-infinite persistence ("임무 완수까지 무한 반복"). After 90 automated retries exhausted, Tier 6 escalates to Claude Code interactive analysis for manual breakthrough (the pipeline never silently gives up). "Total War" mode activates when standard retries fail: `pip3 install undetected-chromedriver` → deploy for paywall/extreme sites. (3) Consolidate all articles into single JSONL file per day (single file for downstream task reuse). (4) Generate crawl statistics report. (5) Implement graceful shutdown with checkpoint saving. Reference: User requirements for 4-level retry, single file output, mission-complete persistence, user Q&A on crawled data.
- **Output**: `src/crawling/pipeline.py`, `src/crawling/retry_manager.py`, `src/crawling/crawl_report.py`
- **Translation**: none (code)

### 13. (team) Analysis Pipeline Stages 1-4 (NLP Foundation)
- **Team**: `analysis-foundation-team`
- **Checkpoint Pattern**: dense
- **Pre-processing**: `python3 scripts/extract_pipeline_design_s1_s4.py` — Filter Step 7 analysis pipeline design to Stages 1-4 specs
- **Tasks**:
  - `@preprocessing-dev` (opus): Implement Stage 1 (Preprocessing): `src/analysis/stage1_preprocessing.py`. (1) Language detection (langdetect). (2) Korean text: Kiwi morphological analysis → noun/verb/adjective extraction → stopword removal. (3) English text: spaCy lemmatization → stopword removal. (4) Common: sentence splitting, normalization (Unicode NFKC), whitespace cleanup. (5) Input: `all_articles.jsonl`. Output: `data/processed/articles.parquet` matching PRD §7.1.1 schema.
    - **Checkpoints**: CP-1 (Korean pipeline), CP-2 (English + language detection), CP-3 (full stage + Parquet output)
    - Output: `src/analysis/stage1_preprocessing.py`
  - `@feature-extraction-dev` (opus): Implement Stage 2 (Feature Extraction): `src/analysis/stage2_features.py`. (1) SBERT embeddings (snowflake-arctic-embed-l-v2.0-ko for Korean, all-MiniLM-L6-v2 for English). (2) TF-IDF (word + bigram, per-language). (3) NER (KLUE-RoBERTa-large for Korean, spaCy for English) — extract person, organization, location. (4) KeyBERT keyword extraction (top 10 per article). (5) FastText word vectors (language-specific). Output: `data/features/embeddings.parquet`, `data/features/tfidf.parquet`, `data/features/ner.parquet`. Memory: load one model at a time, unload after processing.
    - **Checkpoints**: CP-1 (SBERT embeddings), CP-2 (TF-IDF + NER), CP-3 (full stage + memory profiling)
    - Output: `src/analysis/stage2_features.py`
  - `@article-analysis-dev` (opus): Implement Stage 3 (Per-Article Analysis): `src/analysis/stage3_article_analysis.py`. (1) Sentiment analysis — KoBERT for Korean news (F1=94%), KcELECTRA as fallback for informal/web text (F1=90.6%), local transformer for English. Output: sentiment_label, sentiment_score. (2) 8-dimension emotion (Plutchik model): joy, trust, fear, surprise, sadness, disgust, anger, anticipation. (3) Zero-shot STEEPS classification (facebook/bart-large-mnli local): Social/Technology/Economic/Environmental/Political/Security. (4) Importance score calculation (composite of: source authority, article length, entity count, burst proximity). Output: `data/analysis/article_analysis.parquet` matching PRD §7.1.2 schema.
    - **Checkpoints**: CP-1 (sentiment + emotion), CP-2 (STEEPS + importance), CP-3 (full stage + schema validation)
    - Output: `src/analysis/stage3_article_analysis.py`
  - `@aggregation-dev` (opus): Implement Stage 4 (Aggregation Analysis): `src/analysis/stage4_aggregation.py`. (1) BERTopic topic modeling with Model2Vec for CPU 500x speedup. (2) Dynamic Topic Modeling (DTM — time-bucketed topic evolution). (3) HDBSCAN density-based clustering. (4) NMF + LDA auxiliary topics (for comparison/validation). (5) Louvain community detection on entity co-occurrence graph. Output: `data/analysis/topics.parquet` with topic_id, topic_label, topic_probability per article.
    - **Checkpoints**: CP-1 (BERTopic + Model2Vec), CP-2 (DTM + HDBSCAN), CP-3 (full stage + NMF/LDA + Louvain)
    - Output: `src/analysis/stage4_aggregation.py`
- **Join**: Team Lead verifies Stage 1→2→3→4 data flow compatibility (Parquet schema chaining)
- **SOT Write**: Team Lead only
- **Verification**:
  - [ ] Stage 1 produces valid `articles.parquet` with all PRD §7.1.1 columns
  - [ ] Stage 2 produces embeddings (384-dim for Korean, 384-dim for English), TF-IDF matrices, NER entities
  - [ ] Stage 3 produces sentiment labels/scores, 8 emotion dimensions, STEEPS categories for ≥ 95% of articles
  - [ ] Stage 4 produces topic assignments with meaningful labels (not just cluster IDs)
  - [ ] Data flows correctly: Stage 1 output → Stage 2 input → Stage 3 input → Stage 4 input (no schema mismatches)
  - [ ] Peak memory per stage ≤ 4GB on M2 Pro (measured with memory profiling)
  - [ ] Processing speed ≤ 15 min per 1,000 articles for Stages 1-4 combined
- **Output**: Analysis Stages 1-4 (code files in `src/analysis/`)
- **Translation**: none (code)

### 14. (team) Analysis Pipeline Stages 5-8 (Signal Detection)
- **Team**: `analysis-signal-team`
- **Checkpoint Pattern**: dense
- **Pre-processing**: `python3 scripts/extract_pipeline_design_s5_s8.py` — Filter Step 7 design to Stages 5-8 specs
- **Tasks**:
  - `@timeseries-dev` (opus): Implement Stage 5 (Time Series Analysis): `src/analysis/stage5_timeseries.py`. (1) STL decomposition (trend/seasonal/residual — statsmodels). (2) Kleinberg burst detection (article frequency spikes). (3) PELT changepoint detection (ruptures — structural transition points in topic/sentiment distributions). (4) Prophet forecasting (7-day and 30-day article volume prediction). (5) Wavelet analysis (multi-period cycle detection — pywt). (6) Moving average crossover (short-term/long-term). (7) ARIMA modeling (statsmodels — autoregressive trend detection). Output: `data/analysis/timeseries.parquet` with burst_score, changepoint indicators, forecast values.
    - **Checkpoints**: CP-1 (STL + Kleinberg burst), CP-2 (PELT + Prophet), CP-3 (full stage + Wavelet + MA crossover)
    - Output: `src/analysis/stage5_timeseries.py`
  - `@cross-analysis-dev` (opus): Implement Stage 6 (Cross-Analysis): `src/analysis/stage6_cross_analysis.py`. (1) Granger causality test (topic-topic temporal precedence — statsmodels). (2) PCMCI causal inference (multivariate time series — tigramite). (3) Co-occurrence networks (entity-entity, topic-topic — networkx). (4) Cross-lingual topic alignment (Korean↔English via multilingual embeddings). (5) Frame analysis (same issue, different outlet framing — cosine similarity of per-source topic distributions). (6) GraphRAG-style knowledge graph construction (entity-relation triples → graph-based retrieval for narrative analysis). Output: `data/analysis/networks.parquet`, `data/analysis/cross_analysis.parquet`.
    - **Checkpoints**: CP-1 (Granger + co-occurrence), CP-2 (PCMCI + cross-lingual), CP-3 (full stage + frame analysis)
    - Output: `src/analysis/stage6_cross_analysis.py`
  - `@signal-classifier-dev` (opus): Implement Stage 7 (5-Layer Signal Classification): `src/analysis/stage7_signals.py`. (1) Rule-based 5-Layer classifier: L1 Fad (Kleinberg burst + Z-score, 1-14 days), L2 Short-term (BERTopic DTM + sentiment trajectory + MA crossover, 2 weeks-3 months), L3 Mid-term (PELT changepoints + network evolution + frame shifts, 3 months-1 year), L4 Long-term (embedding drift + Wavelet + STEEPS persistence, 1-5 years), L5 Singularity (composite score ≥ 0.65). (2) Novelty detection: LOF + Isolation Forest for OOD articles. (3) BERTrend-inspired weak→emerging signal transition detection. (4) Singularity composite score: S = w1×OOD + w2×Changepoint + w3×CrossDomain + w4×BERTrend + w5×Entropy + w6×Novelty + w7×Network (initial weights from PRD Appendix E). (5) Dual-Pass: title pass (fast signal scan) → body pass (evidence confirmation). Output: `data/output/signals.parquet` matching PRD §7.1.3 schema.
    - **Checkpoints**: CP-1 (L1-L3 classification rules), CP-2 (L4-L5 + novelty + singularity), CP-3 (full stage + dual-pass + schema validation)
    - Output: `src/analysis/stage7_signals.py`
  - `@storage-dev` (sonnet): Implement Stage 8 (Data Output): `src/analysis/stage8_output.py`. (1) Parquet writer with ZSTD compression for: analysis.parquet (unified), signals.parquet, topics.parquet. (2) SQLite builder: articles_fts (FTS5 full-text search), article_embeddings (sqlite-vec vector search), signals_index, topics_index, crawl_status. Schema exactly per PRD §7.2. (3) DuckDB query compatibility verification. (4) Data quality checks: null rate, duplicate rate, schema validation. Output: `data/output/` directory with all final files.
    - **Checkpoints**: CP-1 (Parquet writer + schema), CP-2 (SQLite FTS5 + vec), CP-3 (full output + quality checks)
    - Output: `src/analysis/stage8_output.py`, `src/storage/sqlite_builder.py`, `src/storage/parquet_writer.py`
- **Join**: Team Lead verifies Stage 5→6→7→8 data flow + output schema compliance with PRD §7
- **SOT Write**: Team Lead only
- **Verification**:
  - [ ] Stage 5 detects bursts and changepoints in test data (at least 1 burst, 1 changepoint in sample)
  - [ ] Stage 6 produces at least 1 Granger-significant topic pair and 1 co-occurrence network
  - [ ] Stage 7 classifies test signals into all 5 layers with confidence scores
  - [ ] Stage 7 singularity composite score formula matches PRD Appendix E exactly
  - [ ] Stage 8 output Parquet schemas match PRD §7.1 exactly (column names, types, all present)
  - [ ] Stage 8 SQLite schemas match PRD §7.2 exactly (all tables, indexes, virtual tables)
  - [ ] Full Stage 5-8 pipeline processes 1,000 test articles in ≤ 15 min (target: 30 min total for 8 stages)
  - [ ] Cross-lingual topic alignment produces meaningful Korean↔English topic pairs
- **Output**: Analysis Stages 5-8 (code files in `src/analysis/`, `src/storage/`)
- **Translation**: none (code)

### 15. Analysis Pipeline Integration & Memory Management
- **Agent**: `@integration-engineer` (opus)
- **Verification**:
  - [ ] `src/analysis/pipeline.py` orchestrates all 8 stages in strict sequence: 1→2→3→4→5→6→7→8
  - [ ] Memory management implemented: each stage loads models → processes → saves output → unloads models → gc.collect()
  - [ ] Peak memory during any single stage ≤ 5GB (measured)
  - [ ] Full 8-stage pipeline completes for 1,000 test articles without OOM
  - [ ] Intermediate Parquet files correctly chain between stages (no data loss)
  - [ ] Pipeline produces all final outputs: `data/output/analysis.parquet`, `data/output/signals.parquet`, `data/output/index.sqlite`, `data/output/topics.parquet`
  - [ ] `python3 main.py analyze --date today` executes full analysis pipeline
  - [ ] Error in one stage does not corrupt previous stage outputs (atomic stage execution)
- **Task**: Integrate all 8 analysis stages into a unified pipeline. (1) Create `src/analysis/pipeline.py` — sequential stage orchestrator with memory management. (2) Between each stage: save Parquet output → explicitly del model objects → gc.collect() → torch.cuda.empty_cache() (if applicable). (3) Implement stage-level error handling: if a stage fails, log error, save partial results, continue to next stage where possible. (4) Add pipeline progress logging (stage name, article count, time elapsed, memory usage). (5) Connect to crawling pipeline: `main.py full` runs crawl → analyze → output in sequence. (6) Verify all Parquet schemas match PRD §7.1 column definitions exactly.
- **Output**: `src/analysis/pipeline.py` (integrated pipeline orchestrator)
- **Translation**: none (code)

### 16. End-to-End Testing (44 Sites Full Crawl + Analysis)
- **Agent**: `@test-engineer` (opus)
- **Verification**:
  - [ ] Full crawl executed on all 44 sites — per-site success/failure logged
  - [ ] Crawling success rate ≥ 80% of sites (≥ 35 out of 44 sites return articles)
  - [ ] Total articles collected ≥ 500 (across all sites, 24h window)
  - [ ] Mandatory fields (title, published_at, body, source_url) present in ≥ 99% of collected articles
  - [ ] Dedup correctly filters duplicates (duplicate rate ≤ 1% in output)
  - [ ] Analysis pipeline completes without OOM on collected articles
  - [ ] All 5 signal layers represented in output (at minimum L1 and L2 detected)
  - [ ] SQLite FTS5 search returns results for test queries
  - [ ] sqlite-vec similarity search returns meaningful nearest neighbors
  - [ ] End-to-end time (crawl + analyze) ≤ 3 hours on M2 Pro
  - [ ] Failure report generated: which sites failed, at which tier, why — for future improvement
  - [ ] 3-tier retry system actually engages (at least 1 site escalated beyond Tier 1)
- **Task**: Execute a complete end-to-end test of the entire system. (1) Run full crawling pipeline on all 44 sites with real HTTP requests. (2) Verify mandatory field extraction (title, date, body, URL) across all successful crawls. (3) Run full 8-stage analysis pipeline on collected articles. (4) Verify output schemas (Parquet + SQLite). (5) Test search functionality (FTS5 + vector search). (6) Measure performance metrics (time, memory, success rates). (7) Generate comprehensive test report with per-site results. (8) Identify and document failed sites with root cause analysis. (9) For failed sites, attempt manual debugging and adapter fixes. (10) Re-run failed sites after fixes. This step IS the validation that the system works.
- **Output**: `testing/e2e-test-report.md`, `testing/per-site-results.json`
- **Review**: `@reviewer`
- **Translation**: `@translator` → `testing/e2e-test-report.ko.md`
- **Post-processing**: `python3 scripts/calculate_success_metrics.py` — Compute PRD §9.1 metrics (success rate, coverage, article count, accuracy, dedup rate, time)

### 17. Automation, Scheduling & Self-Recovery
- **Agent**: `@devops-engineer` (opus)
- **Verification**:
  - [ ] cron job configured for daily execution at 02:00 AM: full crawl + analysis pipeline
  - [ ] Weekly cron for site structure rescan (Sunday 01:00 AM)
  - [ ] `scripts/run_daily.sh` wrapper script with: venv activation, pipeline execution, log rotation, error notification
  - [ ] Self-recovery implemented: (a) Pipeline restart on crash with checkpoint resume. (b) Stale lock file detection and cleanup. (c) Disk space check before run. (d) Previous run cleanup (archive old raw data).
  - [ ] Structured logging to `data/logs/crawl.log` and `data/logs/analysis.log` (JSON format, rotated daily)
  - [ ] Error notification: failed crawl/analysis logged to `data/logs/errors.log` with severity levels
  - [ ] `crontab -l` shows correctly configured entries
  - [ ] `scripts/run_daily.sh` executes successfully in dry-run mode
  - [ ] Monthly data archiving script: compress and move data older than 30 days
- **Task**: Set up automated daily execution and self-recovery. (1) Create `scripts/run_daily.sh` — bash wrapper that activates venv, runs `python3 main.py full --date today`, handles errors, rotates logs. (2) Create `scripts/run_weekly_rescan.sh` — site structure rescan for navigation/section changes. (3) Configure crontab entries. (4) Implement self-recovery in Python: checkpoint-based resume, lock file management, pre-run health checks (disk space, network, dependencies). (5) Set up structured JSON logging with daily rotation. (6) Create monthly archiving script for old data. Reference: PRD §5.1.4 Scheduling, §6.2 Execution Model, §12.2 Reliability.
- **Output**: `scripts/run_daily.sh`, `scripts/run_weekly_rescan.sh`, `scripts/archive_old_data.sh`, `src/utils/self_recovery.py`
- **Translation**: none (code/scripts)

### 18. (human) Final System Review & Deployment Approval
- **Action**: Review and approve: (1) E2E test report (Step 16) — success rates, failed sites, performance. (2) Automation setup (Step 17) — cron configuration, self-recovery. (3) Overall system readiness for daily unattended operation. (4) Decide on any sites to temporarily disable in `sources.yaml`. (5) Approve deployment to production cron.
- **Command**: `/review-final`
- **Autopilot Default**: Approve if crawling success rate ≥ 80% and analysis pipeline completes without errors

### 19. Documentation & Operational Guides
- **Agent**: `@doc-writer` (opus)
- **Verification**:
  - [ ] `README.md` covers: project overview, quick start, directory structure, configuration, troubleshooting
  - [ ] `docs/operations-guide.md` covers: daily monitoring, adding new sites to `sources.yaml`, handling blocked sites, Tier 6 manual intervention, model retraining
  - [ ] `docs/architecture-guide.md` covers: system architecture, module interfaces, data flow, extension points
  - [ ] All code files have module-level docstrings explaining purpose and interfaces
  - [ ] `sources.yaml` has inline comments explaining each field
  - [ ] Troubleshooting section covers top 10 risks from PRD §11.4 with resolution steps
- **Task**: Create comprehensive documentation for the built system. (1) `README.md` — project overview, installation, quick start, daily operation. (2) `docs/operations-guide.md` — how to monitor, add sites, handle failures, run Tier 6 manual debugging. (3) `docs/architecture-guide.md` — system design, data flow, how to extend (add analysis techniques, add sites). (4) Add docstrings to all major Python modules. (5) Document `sources.yaml` configuration format with examples. Reference: PRD §12.4 Maintainability.
- **Output**: `README.md`, `docs/operations-guide.md`, `docs/architecture-guide.md`
- **Translation**: `@translator` → `README.ko.md`, `docs/operations-guide.ko.md`, `docs/architecture-guide.ko.md`

### 20. Final Code Review
- **Agent**: `@reviewer` (opus)
- **Verification**:
  - [ ] Code quality: no hardcoded credentials, proper error handling, consistent naming conventions
  - [ ] Security: no SQL injection vulnerabilities, input validation on external data, safe file operations
  - [ ] Performance: no obvious memory leaks, proper resource cleanup, efficient data processing
  - [ ] Correctness: Parquet schemas match PRD exactly, SQLite schemas match PRD exactly, all 44 sites in adapter registry
  - [ ] Reliability: 3-tier retry system correctly implemented, Circuit Breaker pattern correct, self-recovery logic sound
  - [ ] Completeness: all 56 analysis techniques accounted for (mapped to specific stages), all 44 site adapters present
  - [ ] Architecture: Conductor Pattern respected (Claude Code generates scripts, doesn't process data), Staged Monolith boundaries maintained
  - [ ] Legal: robots.txt compliance implemented, rate limiting respects Crawl-delay, User-Agent is transparent
- **Task**: Perform a comprehensive adversarial review of the entire codebase. Check for: (1) Security vulnerabilities (injection, credential exposure). (2) Correctness (schema compliance, data flow integrity). (3) Reliability (error handling, retry logic, recovery). (4) Performance (memory management, processing speed). (5) Completeness (all 44 sites, all 56 techniques, all PRD requirements). (6) Code quality (naming, structure, documentation). Generate a detailed review report with Critical/Warning/Suggestion classifications.
- **Output**: `review-logs/step-20-review.md`
- **Translation**: `@translator` → `review-logs/step-20-review.ko.md`

---

## Claude Code Configuration

### Sub-agents

```yaml
agents:
  # Research Phase
  site-recon:
    name: site-recon
    description: "Reconnaissance specialist for news site structure analysis"
    model: sonnet
    tools: Read, Write, Bash, Glob, Grep, WebFetch, WebSearch
    maxTurns: 50
    memory: project

  crawl-analyst:
    name: crawl-analyst
    description: "Crawling feasibility and strategy analysis expert"
    model: opus
    tools: Read, Write, Bash, Glob, Grep, WebFetch
    maxTurns: 40
    memory: project

  # Planning Phase
  system-architect:
    name: system-architect
    description: "System architecture design specialist"
    model: opus
    tools: Read, Write, Edit, Glob, Grep
    maxTurns: 50
    memory: project

  pipeline-designer:
    name: pipeline-designer
    description: "NLP and data analysis pipeline design specialist"
    model: opus
    tools: Read, Write, Edit, Glob, Grep, WebSearch
    maxTurns: 50
    memory: project

  # Implementation Phase — Crawling
  crawler-core-dev:
    name: crawler-core-dev
    description: "Core crawling engine developer — NetworkGuard, URL discovery, article extraction"
    model: opus
    tools: Read, Write, Edit, Bash, Glob, Grep
    maxTurns: 80
    memory: project

  anti-block-dev:
    name: anti-block-dev
    description: "Anti-blocking system developer — 7-type detection, 6-tier escalation, Circuit Breaker"
    model: opus
    tools: Read, Write, Edit, Bash, Glob, Grep
    maxTurns: 80
    memory: project

  dedup-dev:
    name: dedup-dev
    description: "Deduplication engine developer — URL normalization, SimHash, title similarity"
    model: sonnet
    tools: Read, Write, Edit, Bash, Glob, Grep
    maxTurns: 40
    memory: project

  ua-rotation-dev:
    name: ua-rotation-dev
    description: "User-Agent rotation and session management developer"
    model: sonnet
    tools: Read, Write, Edit, Bash, Glob, Grep
    maxTurns: 40
    memory: project

  # Implementation Phase — Site Adapters
  adapter-dev-kr-major:
    name: adapter-dev-kr-major
    description: "Korean major news site adapter developer"
    model: opus
    tools: Read, Write, Edit, Bash, Glob, Grep, WebFetch
    maxTurns: 80
    memory: project

  adapter-dev-kr-tech:
    name: adapter-dev-kr-tech
    description: "Korean IT/tech news site adapter developer"
    model: opus
    tools: Read, Write, Edit, Bash, Glob, Grep, WebFetch
    maxTurns: 60
    memory: project

  adapter-dev-english:
    name: adapter-dev-english
    description: "English-language news site adapter developer"
    model: opus
    tools: Read, Write, Edit, Bash, Glob, Grep, WebFetch
    maxTurns: 80
    memory: project

  adapter-dev-multilingual:
    name: adapter-dev-multilingual
    description: "Multilingual news site adapter developer — CJK, RTL, European languages"
    model: opus
    tools: Read, Write, Edit, Bash, Glob, Grep, WebFetch
    maxTurns: 80
    memory: project

  # Implementation Phase — Analysis
  preprocessing-dev:
    name: preprocessing-dev
    description: "NLP preprocessing developer — Kiwi, spaCy, text normalization"
    model: opus
    tools: Read, Write, Edit, Bash, Glob, Grep
    maxTurns: 60
    memory: project

  feature-extraction-dev:
    name: feature-extraction-dev
    description: "Feature extraction developer — SBERT, TF-IDF, NER, KeyBERT"
    model: opus
    tools: Read, Write, Edit, Bash, Glob, Grep
    maxTurns: 60
    memory: project

  article-analysis-dev:
    name: article-analysis-dev
    description: "Per-article analysis developer — sentiment, emotion, STEEPS classification"
    model: opus
    tools: Read, Write, Edit, Bash, Glob, Grep
    maxTurns: 60
    memory: project

  aggregation-dev:
    name: aggregation-dev
    description: "Aggregation analysis developer — BERTopic, HDBSCAN, community detection"
    model: opus
    tools: Read, Write, Edit, Bash, Glob, Grep
    maxTurns: 60
    memory: project

  timeseries-dev:
    name: timeseries-dev
    description: "Time series analysis developer — STL, PELT, Kleinberg, Prophet, Wavelet"
    model: opus
    tools: Read, Write, Edit, Bash, Glob, Grep
    maxTurns: 60
    memory: project

  cross-analysis-dev:
    name: cross-analysis-dev
    description: "Cross-analysis developer — Granger, PCMCI, network analysis, frame analysis"
    model: opus
    tools: Read, Write, Edit, Bash, Glob, Grep
    maxTurns: 60
    memory: project

  signal-classifier-dev:
    name: signal-classifier-dev
    description: "5-Layer signal classification developer — L1-L5, novelty detection, singularity"
    model: opus
    tools: Read, Write, Edit, Bash, Glob, Grep
    maxTurns: 60
    memory: project

  storage-dev:
    name: storage-dev
    description: "Data storage developer — Parquet writer, SQLite builder, schema validation"
    model: sonnet
    tools: Read, Write, Edit, Bash, Glob, Grep
    maxTurns: 40
    memory: project

  # Integration & Testing
  integration-engineer:
    name: integration-engineer
    description: "Pipeline integration specialist — connects crawling and analysis modules"
    model: opus
    tools: Read, Write, Edit, Bash, Glob, Grep
    maxTurns: 60
    memory: project

  test-engineer:
    name: test-engineer
    description: "End-to-end testing specialist — full pipeline validation"
    model: opus
    tools: Read, Write, Edit, Bash, Glob, Grep, WebFetch
    maxTurns: 80
    memory: project

  devops-engineer:
    name: devops-engineer
    description: "Automation and scheduling specialist — cron, self-recovery, logging"
    model: opus
    tools: Read, Write, Edit, Bash, Glob, Grep
    maxTurns: 40
    memory: project

  # Documentation
  doc-writer:
    name: doc-writer
    description: "Technical documentation writer"
    model: opus
    tools: Read, Write, Edit, Glob, Grep
    maxTurns: 40
    memory: project

  # Validation (Tech Stack)
  dep-validator:
    name: dep-validator
    description: "Dependency installation and validation specialist"
    model: sonnet
    tools: Read, Write, Bash, Glob
    maxTurns: 30
    memory: project

  nlp-benchmarker:
    name: nlp-benchmarker
    description: "NLP model benchmarking specialist for Korean language"
    model: sonnet
    tools: Read, Write, Bash, Glob
    maxTurns: 30
    memory: project

  memory-profiler:
    name: memory-profiler
    description: "Memory profiling specialist for M2 Pro 16GB constraints"
    model: sonnet
    tools: Read, Write, Bash, Glob
    maxTurns: 30
    memory: project

  # Crawling Strategy (Planning)
  crawl-strategist-kr:
    name: crawl-strategist-kr
    description: "Korean news site crawling strategy designer"
    model: opus
    tools: Read, Write, Glob, Grep, WebFetch
    maxTurns: 40
    memory: project

  crawl-strategist-en:
    name: crawl-strategist-en
    description: "English news site crawling strategy designer"
    model: opus
    tools: Read, Write, Glob, Grep, WebFetch
    maxTurns: 40
    memory: project

  crawl-strategist-asia:
    name: crawl-strategist-asia
    description: "Asia-Pacific news site crawling strategy designer"
    model: opus
    tools: Read, Write, Glob, Grep, WebFetch
    maxTurns: 40
    memory: project

  crawl-strategist-global:
    name: crawl-strategist-global
    description: "Europe/Middle East news site crawling strategy designer"
    model: opus
    tools: Read, Write, Glob, Grep, WebFetch
    maxTurns: 40
    memory: project

  # Infrastructure
  infra-builder:
    name: infra-builder
    description: "Project infrastructure scaffolding — directories, configs, utilities"
    model: opus
    tools: Read, Write, Edit, Bash, Glob, Grep
    maxTurns: 60
    memory: project
```

### Agent Teams

```yaml
teams:
  tech-validation-team:     # Step 2
    members: [dep-validator, nlp-benchmarker, memory-profiler]
    checkpoint: standard

  crawl-strategy-team:      # Step 6
    members: [crawl-strategist-kr, crawl-strategist-en, crawl-strategist-asia, crawl-strategist-global]
    checkpoint: standard

  crawl-engine-team:        # Step 10
    members: [crawler-core-dev, anti-block-dev, dedup-dev, ua-rotation-dev]
    checkpoint: dense

  site-adapters-team:       # Step 11
    members: [adapter-dev-kr-major, adapter-dev-kr-tech, adapter-dev-english, adapter-dev-multilingual]
    checkpoint: dense

  analysis-foundation-team: # Step 13
    members: [preprocessing-dev, feature-extraction-dev, article-analysis-dev, aggregation-dev]
    checkpoint: dense

  analysis-signal-team:     # Step 14
    members: [timeseries-dev, cross-analysis-dev, signal-classifier-dev, storage-dev]
    checkpoint: dense
```

### SOT (State Management)

- **SOT File**: `.claude/state.yaml`
- **Write Permission**: Orchestrator (main session) or Team Lead (during team steps) — single writer
- **Agent Access**: Read-only — all agents read SOT but never write; they produce output files only
- **Quality Override**: Team members MAY read each other's output files directly when cross-referencing improves quality (e.g., adapter developers reading core engine interfaces). SOT single-write point is always preserved.

```yaml
# .claude/state.yaml — Initial SOT
workflow:
  name: "GlobalNews Auto-Build"
  current_step: 1
  status: "in_progress"

  parent_genome:
    source: "AgenticWorkflow"
    version: "2026-02-25"
    inherited_dna:
      - "absolute-criteria"
      - "sot-pattern"
      - "3-phase-structure"
      - "4-layer-qa"
      - "safety-hooks"
      - "adversarial-review"
      - "decision-log"
      - "context-preservation"
      - "cross-step-traceability"

  outputs: {}
    # step-1: "research/site-reconnaissance.md"
    # step-2: "research/tech-validation.md"
    # ...

  pending_human_action:
    step: null
    options: []

  # Autopilot disabled — manual approval at human steps
  autopilot:
    enabled: false
    decision_log_dir: "autopilot-logs/"
    auto_approved_steps: []

  pacs:
    current_step_score: null
    dimensions: { F: null, C: null, L: null }
    weak_dimension: null
    pre_mortem_flag: null
    history: {}

  verification:
    last_verified_step: 0
    retries: {}

  domain_knowledge:
    file: null
    entity_count: 0
    relation_count: 0
    constraint_count: 0
    built_at_step: null
    last_validated: null
```

### Context Injection Pattern

**Pattern B — Filtered Delegation** (PRD = ~30,000 tokens, 50-200KB range)

PRD를 에이전트에 전달할 때 전체를 넘기지 않고, Python 전처리 스크립트로 관련 섹션만 필터링하여 주입한다.

```yaml
context_injection:
  pattern: "B — Filtered Delegation"
  source: "coding-resource/PRD.md"
  source_size: "~30,000 tokens"
  filter_scripts:
    step-1:  "scripts/extract_site_urls.py"            # §4.3, §5.1.1 → site-recon
    step-3:  "scripts/merge_recon_and_deps.py"          # Step 1 + Step 2 → crawl-analyst
    step-5:  "scripts/filter_prd_architecture.py"       # §6, §7, §8 → system-architect
    step-6:  "scripts/split_sites_by_group.py"          # Step 1 data + group split → crawl-strategists
    step-7:  "scripts/filter_prd_analysis.py"           # §5.2 → pipeline-designer
    step-13: "scripts/extract_pipeline_design_s1_s4.py" # Step 7 Stages 1-4 → analysis-foundation-team
    step-14: "scripts/extract_pipeline_design_s5_s8.py" # Step 7 Stages 5-8 → analysis-signal-team
  rule: "Each Pre-processing script extracts ONLY the sections relevant to that agent's task. Never pass full PRD to a sub-agent."
```

### Parallel Translation Protocol

When a step has `Translation: @translator`, the Orchestrator applies the following protocol based on document size:

```yaml
translation_protocol:
  threshold: 350  # lines — documents above this trigger multi-agent translation

  small_document:  # ≤ 350 lines
    strategy: "single"
    process: "Spawn one @translator Task agent → write .ko.md"

  large_document:  # > 350 lines
    strategy: "multi"
    process:
      step_1_split: "python3 scripts/split_for_translation.py --source <file> --output-dir .tmp/translate-chunks --project-dir ."
      step_2_translate: "Spawn N @translator Task agents in PARALLEL — one per chunk"
      step_3_merge: "python3 scripts/merge_translations.py --manifest .tmp/translate-chunks/manifest.json --output <file>.ko.md --project-dir ."
      step_4_validate: "python3 .claude/hooks/scripts/validate_translation.py --step N --project-dir . --check-pacs --check-sequence"

    chunk_rules:
      - "Split on ## heading boundaries only — never within code blocks"
      - "Code blocks (```yaml, ```json, ```python) are NOT translated — pass through unchanged"
      - "Each chunk translator receives: chunk file + glossary + document title for context"
      - "Each chunk translator writes chunk-NN.ko.md in the same output-dir"
      - "Source language is auto-detected (character-class density heuristic)"

    naming_convention:
      source_chunk: ".tmp/translate-chunks/chunk-{NN}.md"
      translated_chunk: ".tmp/translate-chunks/chunk-{NN}.ko.md"
      manifest: ".tmp/translate-chunks/manifest.json"

    pacs:
      - "Each chunk translator does NOT score pACS independently"
      - "Team Lead scores Translation pACS (Ft/Ct/Nt) on the MERGED output"
      - "Single pacs-logs/step-N-translation-pacs.md for the merged result"
```

**Why multi-agent**: A single translator agent hits the 32K output token limit on documents > ~350 lines (~76KB). Splitting into N chunks keeps each agent well within limits while enabling parallel execution for speed.

### Hooks

```json
{
  "hooks": {
    "PreToolUse": [
      {
        "matcher": "Bash",
        "hooks": [{
          "type": "command",
          "command": "if test -f \"$CLAUDE_PROJECT_DIR/.claude/hooks/scripts/block_destructive_commands.py\"; then python3 \"$CLAUDE_PROJECT_DIR/.claude/hooks/scripts/block_destructive_commands.py\"; fi",
          "timeout": 10
        }]
      },
      {
        "matcher": "Edit|Write",
        "hooks": [{
          "type": "command",
          "command": "if test -f \"$CLAUDE_PROJECT_DIR/.claude/hooks/scripts/predictive_debug_guard.py\"; then python3 \"$CLAUDE_PROJECT_DIR/.claude/hooks/scripts/predictive_debug_guard.py\"; fi",
          "timeout": 10
        }]
      }
    ],
    "PostToolUse": [
      {
        "matcher": "Edit|Write|Bash|Task|NotebookEdit|TeamCreate|SendMessage|TaskCreate|TaskUpdate",
        "hooks": [{
          "type": "command",
          "command": "if test -f \"$CLAUDE_PROJECT_DIR/.claude/hooks/scripts/context_guard.py\"; then python3 \"$CLAUDE_PROJECT_DIR/.claude/hooks/scripts/context_guard.py\" --mode=post-tool; fi",
          "timeout": 15
        }]
      }
    ],
    "Stop": [
      {
        "hooks": [{
          "type": "command",
          "command": "if test -f \"$CLAUDE_PROJECT_DIR/.claude/hooks/scripts/context_guard.py\"; then python3 \"$CLAUDE_PROJECT_DIR/.claude/hooks/scripts/context_guard.py\" --mode=stop; fi",
          "timeout": 30
        }]
      }
    ],
    "PreCompact": [
      {
        "hooks": [{
          "type": "command",
          "command": "if test -f \"$CLAUDE_PROJECT_DIR/.claude/hooks/scripts/context_guard.py\"; then python3 \"$CLAUDE_PROJECT_DIR/.claude/hooks/scripts/context_guard.py\" --mode=pre-compact; fi",
          "timeout": 30
        }]
      }
    ],
    "SessionStart": [
      {
        "matcher": "clear|compact|resume",
        "hooks": [{
          "type": "command",
          "command": "if test -f \"$CLAUDE_PROJECT_DIR/.claude/hooks/scripts/context_guard.py\"; then python3 \"$CLAUDE_PROJECT_DIR/.claude/hooks/scripts/context_guard.py\" --mode=restore; fi",
          "timeout": 15
        }]
      }
    ],
    "SessionEnd": [
      {
        "matcher": "clear",
        "hooks": [{
          "type": "command",
          "command": "if test -f \"$CLAUDE_PROJECT_DIR/.claude/hooks/scripts/save_context.py\"; then python3 \"$CLAUDE_PROJECT_DIR/.claude/hooks/scripts/save_context.py\" --trigger sessionend; fi",
          "timeout": 30
        }]
      }
    ]
  }
}
```

> **Exit Code 규칙**: `0` = 통과, `2` = 차단 (stderr → Claude에 피드백), 기타 = 논블로킹 에러
> **SOT 비접근**: 모든 Hook은 SOT를 읽기 전용으로만 접근 (절대 기준 2 준수)

### Slash Commands

```markdown
# /review-research — Step 4 (human)
---
description: "Research phase 산출물 검토 (Steps 1-3)"
---
Research phase의 모든 산출물을 검토합니다.

다음 파일을 읽고 종합 검토 의견을 제시하세요:
1. `research/site-reconnaissance.md` — 44개 사이트 정찰 결과
2. `research/tech-validation.md` — 기술 스택 검증 결과
3. `research/crawling-feasibility.md` — 크롤링 가능성 분석

검토 기준:
- 44개 사이트 전수 커버리지 확인
- 난이도 분류 정확성
- M2 Pro 16GB 기술 스택 호환성
- 사이트별 크롤링 전략 적절성
$ARGUMENTS

# /review-architecture — Step 8 (human)
---
description: "Architecture & Strategy 승인 (Steps 5-7)"
---
Planning phase의 모든 설계 문서를 검토합니다.

다음 파일을 읽고 구현 준비 상태를 평가하세요:
1. `planning/architecture-blueprint.md` — 시스템 아키텍처 청사진
2. `planning/crawling-strategies.md` — 44개 사이트별 크롤링 전략
3. `planning/analysis-pipeline-design.md` — 8단계 분석 파이프라인 설계

검토 기준:
- PRD §6-8 아키텍처 사양 준수
- 44개 사이트별 전략 완전성 (selector, 속도제한, fallback)
- 56개 분석 기법 스테이지 매핑 완전성
- 16GB 메모리 제약 내 실행 가능성
$ARGUMENTS

# /review-final — Step 18 (human)
---
description: "Final system review & deployment approval (Steps 16-17)"
---
전체 시스템 최종 검토 및 배포 승인.

다음 파일을 읽고 운영 준비 상태를 평가하세요:
1. `testing/e2e-test-report.md` — E2E 테스트 결과
2. `testing/per-site-results.json` — 사이트별 성공/실패 상세
3. `scripts/run_daily.sh` — 일일 자동 실행 스크립트
4. `src/utils/self_recovery.py` — 자기 복구 로직

검토 기준:
- 크롤링 성공률 ≥ 80% (44개 중 35개 이상)
- 분석 파이프라인 OOM 없이 완료
- cron 설정 정확성
- 자기 복구 로직 건전성
$ARGUMENTS
```

### Runtime Directories

```yaml
runtime_directories:
  # Workflow outputs — Research phase
  research/:                     # Steps 1-3 research outputs

  # Workflow outputs — Planning phase
  planning/:                     # Steps 5-7 design documents

  # Workflow outputs — Implementation phase
  src/crawling/:                 # Steps 9-12 crawling engine code
  src/crawling/adapters/:        # Step 11 site-specific adapters
  src/analysis/:                 # Steps 13-15 analysis pipeline code
  src/storage/:                  # Step 14 storage layer code
  src/utils/:                    # Step 9 shared utilities
  src/config/:                   # Step 9 constants and config
  scripts/:                      # Pre/post-processing + automation scripts
  testing/:                      # Step 16 E2E test reports
  docs/:                         # Step 19 documentation

  # Data directories — runtime data (gitignored)
  data/raw/:                     # Raw crawled JSONL per day
  data/processed/:               # Parquet after Stage 1
  data/features/:                # Parquet after Stage 2
  data/analysis/:                # Parquet after Stages 3-6
  data/output/:                  # Final Parquet + SQLite
  data/models/:                  # Downloaded NLP models
  data/logs/:                    # Structured JSON logs
  data/config/:                  # sources.yaml + pipeline.yaml

  # Quality assurance — QA gate outputs
  verification-logs/:            # step-N-verify.md (L1 검증 결과)
  pacs-logs/:                    # step-N-pacs.md (L1.5 pACS 자체 신뢰 평가)
  review-logs/:                  # step-N-review.md (L2 Adversarial Review)
  diagnosis-logs/:               # step-N-{gate}-{timestamp}.md (Abductive Diagnosis)
  autopilot-logs/:               # step-N-decision.md (Autopilot 결정 로그)
  translations/:                 # glossary.yaml + *.ko.md (@translator)
```

> **gitignore**: `data/`, `verification-logs/`, `pacs-logs/`, `review-logs/`, `diagnosis-logs/`, `autopilot-logs/` → `.gitignore`에 추가 권장
> **초기화**: Step 9 (`@infra-builder`)가 `mkdir -p`로 전체 디렉터리 구조 생성

### Error Handling

```yaml
error_handling:
  on_agent_failure:
    action: retry_with_feedback
    max_attempts: 3
    escalation: human                  # 3회 초과 → 사용자에게 에스컬레이션

  on_validation_failure:               # Verification Gate FAIL
    action: retry_with_diagnosis       # Abductive Diagnosis → 재작업
    max_retries: 10                    # 최대 10회 (ULW: 15회)
    diagnosis_required: true           # 재시도 전 반드시 진단 수행
    escalation_after: budget_exhausted # 재시도 예산 소진 시 사용자 에스컬레이션

  on_pacs_red:                         # pACS < 50
    action: retry_with_diagnosis
    max_retries: 10                    # 별도 예산 (ULW: 15회)
    diagnosis_required: true

  on_review_fail:                      # Adversarial Review FAIL
    action: retry_with_diagnosis
    max_retries: 10                    # 별도 예산 (ULW: 15회)
    diagnosis_required: true
    blocks_translation: true           # Review FAIL 상태에서 Translation 금지

  on_hook_failure:
    action: log_and_continue           # Hook 실패는 워크플로우를 차단하지 않음

  on_context_overflow:
    action: save_and_recover           # Context Preservation System 자동 적용

  on_teammate_failure:                 # Agent Team 사용 시
    attempt_1: retry_same_agent        # SendMessage로 피드백 → 같은 Teammate 재작업
    attempt_2: replace_with_upgrade    # Teammate shutdown → 새 Teammate (상위 모델)
    attempt_3: human_escalation        # 사용자 판단 요청

  on_crawling_failure:                 # Domain-specific: 크롤링 실패
    action: escalate_anti_block_tier   # 6-Tier 자동 에스컬레이션
    max_tier: 6                        # Tier 6 = Claude Code 수동 분석
    circuit_breaker:
      threshold: 5                     # 5회 연속 실패 → Open
      cooldown_minutes: 30             # 30분 대기 후 Half-Open
    self_modify: true                  # 코드 자체 수정 허용 (user requirement)
```

### pACS Logs

```yaml
pacs_logging:
  log_directory: "pacs-logs/"
  log_format: "step-{N}-pacs.md"
  translation_log_format: "step-{N}-translation-pacs.md"
  dimensions: [F, C, L]                  # Factual Grounding, Completeness, Logical Coherence
  translation_dimensions: [Ft, Ct, Nt]   # Fidelity, Translation Completeness, Naturalness
  scoring: "min-score"                    # pACS = min(F, C, L)
  triggers:
    GREEN: "≥ 70 → auto-proceed"
    YELLOW: "50-69 → proceed with warning in Decision Log"
    RED: "< 50 → Abductive Diagnosis → rework → re-score"
  protocol: "AGENTS.md §5.4"
  p1_validation: "python3 .claude/hooks/scripts/validate_pacs.py --step N --check-l0 --project-dir ."
```

### Notation

| Notation | Meaning |
|----------|---------|
| `(human)` | Human review/approval required (Steps 4, 8, 18) |
| `(team)` | Agent Team parallel execution (Steps 2, 6, 10, 11, 13, 14) |
| `(hook)` | Automated verification/quality gate |
| `@agent-name` | Sub-agent invocation |
| `@translator` | Translation sub-agent — invoked in `Translation` field |
| `@reviewer` | Adversarial review sub-agent (Enhanced L2) |
| `@fact-checker` | Fact verification sub-agent |
| `/command-name` | Slash command execution |
| `Review: @reviewer \| @fact-checker \| none` | Per-step adversarial review assignment |
| `Translation: @translator \| none` | Per-step translation assignment (text outputs only). Documents > 350 lines use Parallel Translation Protocol (split → N agents → merge) |
| `Pre-processing` | Python script executed before agent task (data filtering/merging) |
| `Post-processing` | Python script executed after agent task (validation/transformation) |
| `Checkpoints (dense)` | Dense Checkpoint Pattern — CP-1/CP-2/CP-3 intermediate reports |