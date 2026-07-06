"""
job_hunter.py — AI Job Hunter Orchestrator
==========================================
Scrapes developer job listings from LinkedIn, Naukri, Indeed, and Glassdoor
(India), deduplicates them via SQLite, runs LLM analysis against the user
profile, and fires an instant Telegram notification for any job scoring
>= FIT_THRESHOLD.

Run manually:
    source venv/bin/activate
    python job_hunter.py

Scheduled via:
    - GitHub Actions (cloud, recommended) — see .github/workflows/job_hunter.yml
    - Local cron via setup_cron.sh (fallback)
"""

import os
import re
import json
import time
import sqlite3
import logging
import requests
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from jobspy import scrape_jobs

from ai import AIAnalyzerFactory

# ─────────────────────────────────────────────
# Bootstrap
# ─────────────────────────────────────────────
load_dotenv()   # loads .env file if present; env vars take precedence in CI

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent
DB_PATH = SCRIPT_DIR / "jobs.db"

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
FIT_THRESHOLD = 6           # Only notify for jobs scoring >= this out of 10
RESULTS_PER_SITE = 20       # Listings to pull per site per search term
HOURS_OLD = 24              # Look at jobs posted in the last 24 hours
                            # Clean daily boundary; dedup via jobs.db handles overlap.
                            # Overnight gap (23:30→09:30) has near-zero SWE activity in India.

# Sites to scrape — ordered by India SWE relevance
# naukri:    blocked (406 recaptcha required)
# glassdoor: blocked (403 + location format unsupported)
SCRAPE_SITES = ["linkedin", "indeed"]

# Search terms — cast a wider net across role titles
# Deduplication by URL ensures the same posting isn't analysed twice
SEARCH_TERMS = [
    "software engineer",
    "backend engineer",
    "full stack developer",
]

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

# ─────────────────────────────────────────────
# Pre-filter  (keyword-based fast-reject)
# Runs BEFORE any LLM call — costs zero tokens.
# If a job description contains any REJECT keyword, it's scored 0 and skipped.
# If it contains NONE of the REQUIRE_ANY keywords, it's also skipped.
# Saves ~25-40% of LLM calls on a typical run.
# ─────────────────────────────────────────────
REJECT_KEYWORDS = [
    # Wrong stack
    r"\.net\b", r"\bc#\b", r"asp\.net", r"\bdjango\b", r"\bflask\b",
    r"\bfastapi\b", r"\blaravel\b", r"\bphp\b",
    # Wrong domain
    r"data scientist", r"machine learning engineer", r"ml engineer",
    r"devops engineer", r"\bsre\b", r"site reliability",
    r"embedded engineer", r"firmware engineer",
    r"android developer", r"ios developer", r"mobile developer",
    # Too senior
    r"8\+\s*years?", r"10\+\s*years?", r"12\+\s*years?", r"15\+\s*years?",
]

REQUIRE_ANY_KEYWORDS = [
    "javascript", "typescript", "node", "react", "vue", "next",
    "ruby", "rails", "java", "spring",
    "backend", "full.?stack", "fullstack",
    "software engineer", "software developer",
    "rest api", "graphql", "microservice",
]


def quick_filter(title: str, description: str) -> tuple[bool, str]:
    """
    Fast keyword-based pre-screen before spending an LLM call.
    Returns (should_analyse, reason).
    """
    text = (title + " " + description).lower()

    for pattern in REJECT_KEYWORDS:
        if re.search(pattern, text):
            return False, f"pre-filter reject: '{pattern}'"

    if not any(re.search(kw, text) for kw in REQUIRE_ANY_KEYWORDS):
        return False, "pre-filter reject: no relevant tech keywords found"

    return True, "passed"


# ─────────────────────────────────────────────
# Rate limiter
# Gemini 3.1 Flash-Lite free tier: 15 RPM = 1 req per 4s minimum.
# Without this, rapid-fire calls hit 429 immediately and cascade to
# OpenRouter / Groq — burning through their quotas too.
# A 4s floor keeps us comfortably under the 15 RPM ceiling.
# ─────────────────────────────────────────────
_last_llm_call: float = 0.0
LLM_MIN_INTERVAL_SEC = 4.0   # seconds between LLM calls


def rate_limited_analyze(content: str, prompt: str):
    """Wrap analyze_with_fallback with a minimum inter-call delay."""
    global _last_llm_call
    elapsed = time.monotonic() - _last_llm_call
    if elapsed < LLM_MIN_INTERVAL_SEC:
        wait = LLM_MIN_INTERVAL_SEC - elapsed
        logger.debug(f"Rate limiter: sleeping {wait:.1f}s")
        time.sleep(wait)
    _last_llm_call = time.monotonic()
    return AIAnalyzerFactory.analyze_with_fallback(content, prompt)

# ─────────────────────────────────────────────
# User Profile  (built from resume-short.txt)
# ─────────────────────────────────────────────
USER_PROFILE = {
    "name": "Chaitanya Gupta",
    "yoe": "3+ years",
    "target_roles": [
        "Software Engineer",
        "Senior Software Engineer",
        "Full Stack Engineer",
        "Full Stack Developer",
        "Backend Engineer",
        "Backend Developer",
    ],
    "target_yoe_range": "roles requiring 1–5 years of experience (ideally 3–5 YOE)",

    # ── What to INCLUDE ──────────────────────────────────────────────────────
    "preferred_stacks": [
        "Ruby on Rails",
        "Java / Spring Boot",
        "JavaScript / TypeScript",
        "Node.js / Express.js",
        "React.js / Next.js",
        "Vue.js / Vue 3",
        "MERN stack",
        "GraphQL",
        "REST APIs",
        "PostgreSQL",
        "MongoDB",
        "Redis",
        "Kafka",
        "Microservices",
        "Docker",
        "Kubernetes",
        "AWS",
        "Azure",
        "LLM API integration",
    ],
    "open_to_learning": True,
    "note_on_stack": (
        "Open to roles using any modern backend or full-stack technology, "
        "not limited to the above. Fast learner — has picked up Vue 3, Kafka, "
        "Kubernetes, and LLM pipelines on the job."
    ),

    # ── What to EXCLUDE ─────────────────────────────────────────────────────
    "avoid": [
        "Python-only roles (e.g. Django, Flask, FastAPI full-stack)",
        ".NET / C# roles",
        "Data Science / ML Engineer roles",
        "DevOps / SRE-only roles",
        "Embedded / firmware roles",
        "More than 5 YOE explicitly required",
    ],

    # ── Experience highlights (enriched from resume + self-appraisal) ─────────
    "experience": """
Current: Senior Software Engineer at Veersa Technologies (Feb 2023 – Present)
Stack: Ruby on Rails, Vue 3 (TypeScript), PostgreSQL, Redis, Sidekiq, REST APIs
Domain: US healthcare EMR & Integrated Billing SaaS (KIPU Health)
Total professional experience: 3+ years (2 years FTE + internships from 2020)

Key achievements & impact:
- Led Billable Report modernization: migrated a high-traffic legacy DataTables/ERB
  billing report to Vue 3 with composable architecture, server-side pagination, filtering,
  sorting, and unlimited streaming exports (previously capped at 3,000 rows). New
  architecture became the foundation for all subsequent billing report conversions.
- Architected REGEN billing pipeline refactor: replaced 20+ fragmented Sidekiq workers
  with a unified patient-centric pipeline, cutting worker executions by 65–75%, eliminating
  race conditions and long-standing data inconsistencies. Presented at R&D All-Hands;
  called the most ambitious IB project by the Engineering Manager.
- Built multi-step Audit Wizard: transformed a fully manual audit process into a guided,
  automated workflow. Reduced 100-patient validation from 50–100+ minutes to under a
  minute, directly accelerating client onboardings.
- Designed Claim History modernization: proposed and built a service-based architecture
  with a unified diff-snapshot timeline supporting both legacy and new billing workflows.
  Appreciated for well-thought options and attention to detail.
- Delivered Insurance Change Management (ICM) ahead of a critical CMS deadline;
  praised by Product and implementation teams as working "like a charm".
- Owned patient payments integration via Module Federation — embedded Stripe-powered
  React components into Vue 3 EMR; resolved a Stripe Connect iframe bug in production.
  Recognized by the CPTO.
- Built reusable BAT Vue component library: persistent cross-page selections, auto-save
  user preferences, redesigned action bars, sidebar modals. Adopted across all billing
  reports, reducing future development effort and inconsistency.
- Implemented Mass REGEN workflow for KIPU staff: regenerate billing candidates across
  entire census with progress tracking, validation safeguards, and real-time status updates.
- Regularly supported production incidents: root-cause analysis, data corrections, rapid
  triaging of customer-impacting issues alongside Product and Engineering.
- Stepped up as dev lead during team absence: ran standups, managed deployments,
  coordinated with Product. Earned Achievers Award for exceptional contribution.

Soft skills demonstrated:
- End-to-end ownership across architecture, delivery, QA, and production support.
- Cross-functional collaboration: Product, QA, Implementation, DevOps, and Engineering.
- Proactively identifies scope beyond assigned tickets; proposes improvements.
- Documented RCA findings, reusable scripts, and implementation walkthroughs in
  Jira/Confluence for team knowledge sharing.
- Demonstrated architectural thinking: evaluated multiple approaches, discussed
  trade-offs, iterated on design before implementation.

AI & continuous learning:
- Built AI-integrated personal projects including DSA Pattern Lab (context-aware AI
  tutor, mock interview mode, multi-provider LLM factory) and an AI-powered market
  research application (web scraping + LLM analysis pipeline).
- Actively learning RAG, LLM workflows, prompt engineering, and AI-assisted development.
- Uses Cursor for day-to-day development; integrates AI tooling into workflow.

Earlier: Full Stack Developer Intern at MarketInc (2022), MyWays (2020)
  MERN stack, REST APIs, ERP dashboards, notification infrastructure, ML analytics APIs.

Personal projects (portfolio: https://chaitanyagupta.netlify.app/):
- Amazon Clone: Polyglot microservices (Node.js/MongoDB, Java/Spring Boot/PostgreSQL,
  Go/Redis, Next.js), Kafka, Kubernetes, Docker, Stripe. Published shared NPM package.
- DSA Pattern Lab: Vue 3, TypeScript, Python data pipeline, Gemini/Groq/Ollama LLM
  factory, AI tutor, spaced repetition, mock interviews.
- Alpha Blog: Ruby on Rails 6, React, PostgreSQL, RSpec, Action Cable (real-time chat).
- Crowdy dApp: React, Solidity, Web3 — blockchain-powered crowdfunding.
- DoctorzBook: React, Node, Express, MongoDB — real-time appointment booking.
""",

    # ── Location & logistics ─────────────────────────────────────────────────
    "location_preferences": [
        "Remote (preferred)",
        "Hybrid — Noida / Delhi NCR",
        "Hybrid — Bengaluru (open to relocation)",
    ],
    "target_salary_range": "20–30 LPA",

    # ── Dream companies (high priority if matching) ──────────────────────────
    "dream_companies": [
        "Razorpay", "CRED", "Groww", "Atlassian", "Freshworks",
        "BrowserStack", "Chargebee", "Stripe", "Postman", "Setu",
        "Zepto", "Urban Company", "Meesho", "Swiggy", "Zomato",
        "Microsoft", "Amazon", "Google", "Uber"
    ],
}


# ─────────────────────────────────────────────
# Database
# ─────────────────────────────────────────────
def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS processed_jobs (
            job_url     TEXT PRIMARY KEY,
            title       TEXT,
            company     TEXT,
            score       INTEGER,
            provider    TEXT,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    return conn


def is_processed(conn: sqlite3.Connection, url: str) -> bool:
    return bool(conn.execute(
        "SELECT 1 FROM processed_jobs WHERE job_url = ?", (url,)
    ).fetchone())


def save_job(conn: sqlite3.Connection, url: str, title: str,
             company: str, score: int, provider: str) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO processed_jobs "
        "(job_url, title, company, score, provider) VALUES (?,?,?,?,?)",
        (url, title, company, score, provider)
    )
    conn.commit()


# ─────────────────────────────────────────────
# Scraper
# ─────────────────────────────────────────────
def fetch_jobs() -> pd.DataFrame:
    """
    Scrape jobs from all configured sites and search terms sequentially.

    Strategy:
    - Each (site, search_term) pair is scraped one at a time.
    - Per-site failures are caught and logged without aborting the whole run.
      A flaky Naukri scrape won't kill LinkedIn results.
    - Results are deduplicated by job_url across all batches.
    - Final DataFrame is sorted newest-first so the freshest listings
      are processed (and Telegram'd) first.
    """
    logger.info(
        f"Scraping {len(SCRAPE_SITES)} sites × {len(SEARCH_TERMS)} terms "
        f"(last {HOURS_OLD}h, {RESULTS_PER_SITE} results/batch) …"
    )

    all_frames: list[pd.DataFrame] = []
    seen_urls: set[str] = set()   # cross-batch URL dedup before DB check

    for term in SEARCH_TERMS:
        for site in SCRAPE_SITES:
            try:
                logger.info(f"  └─ [{site}] '{term}' …")
                batch = scrape_jobs(
                    site_name=[site],
                    search_term=term,
                    location="India",
                    results_wanted=RESULTS_PER_SITE,
                    country_indeed="india",
                    hours_old=HOURS_OLD,
                    linkedin_fetch_description=(site == "linkedin"),
                )

                if batch is None or batch.empty:
                    logger.info(f"     → 0 results")
                    continue

                # Deduplicate within this run's accumulated results
                if "job_url" in batch.columns:
                    before = len(batch)
                    batch = batch[~batch["job_url"].isin(seen_urls)]
                    batch = batch.dropna(subset=["job_url"])
                    seen_urls.update(batch["job_url"].tolist())
                    dupes = before - len(batch)
                    logger.info(
                        f"     → {len(batch)} new"
                        + (f" ({dupes} cross-batch dupes skipped)" if dupes else "")
                    )

                all_frames.append(batch)

            except Exception as e:
                # Log the failure but continue with remaining sites/terms
                logger.warning(f"     ⚠️  [{site}] '{term}' failed: {e}")
                continue

    if not all_frames:
        logger.warning("No listings returned from any site/term combination.")
        return pd.DataFrame()

    combined = pd.concat(all_frames, ignore_index=True)

    # Sort newest-first — freshest jobs processed and notified first
    if "date_posted" in combined.columns:
        combined = combined.sort_values(
            "date_posted", ascending=False, na_position="last"
        )
        combined = combined.reset_index(drop=True)

    logger.info(
        f"Total: {len(combined)} unique listings across all sites — newest first"
    )
    return combined


# ─────────────────────────────────────────────
# LLM Analysis
# ─────────────────────────────────────────────
ANALYSIS_PROMPT = f"""
You are an expert technical recruiter evaluating job postings for a specific candidate.
Your job is to read the job description and assess how well it fits the candidate profile.

Candidate Profile:
- Name: {USER_PROFILE['name']}
- Experience: {USER_PROFILE['yoe']}
- Target roles: {', '.join(USER_PROFILE['target_roles'])}
- YOE range preference: {USER_PROFILE['target_yoe_range']}
- Preferred tech stacks: {', '.join(USER_PROFILE['preferred_stacks'])}
- Is open to learning new tech: Yes — adaptable, fast learner
- Stack note: {USER_PROFILE['note_on_stack']}
- Avoid these roles: {', '.join(USER_PROFILE['avoid'])}
- Location preferences: {', '.join(USER_PROFILE['location_preferences'])}
- Dream companies (higher weight if matched): {', '.join(USER_PROFILE['dream_companies'])}

Experience Summary:
{USER_PROFILE['experience']}

Scoring Rules:
- Score 8–10: Strong alignment on tech stack, YOE range, and role type. Apply immediately.
- Score 6–7: Decent match, some gaps but learnable. Worth considering.
- Score 1–5: Poor fit — wrong stack, too senior/junior, domain mismatch.
- Score 0: Explicitly in the avoid list (e.g. Python-only, .NET, DevOps-only, Data Science).

Return ONLY a valid JSON object — no markdown, no preamble:
{{
    "fit_score": <integer 0–10>,
    "synopsis": "<2–3 sentence overview of the role and what it requires>",
    "why_it_fits": "<concise paragraph on alignment or misalignment with the candidate>",
    "dream_company_match": <true if company is in dream list, else false>,
    "resume_suggestions": "<specific, actionable advice on what to emphasise or rearrange in the resume for this job — or 'No changes needed' if already well aligned>"
}}
"""


def analyze_job(title: str, company: str, description: str) -> tuple | None:
    if not description or len(description.strip()) < 50:
        logger.warning(f"Skipping '{title}' — description too short or missing")
        return None

    # Pre-filter: keyword check before spending any LLM tokens
    ok, reason = quick_filter(title, description)
    if not ok:
        logger.info(f"  ↳ {reason} — skipped LLM")
        return None

    job_content = f"Job Title: {title}\nCompany: {company}\n\nJob Description:\n{description}"

    response = rate_limited_analyze(
        content=job_content,
        prompt=ANALYSIS_PROMPT,
    )

    if not response.success:
        logger.error(f"LLM failed for '{title}': {response.error}")
        return None

    try:
        raw = response.content.strip()
        # Strip markdown code fences robustly
        # Handles: ```json\n{...}\n``` and ```\n{...}\n```
        if raw.startswith("```"):
            raw = re.sub(r"^```[a-zA-Z]*\n?", "", raw)  # strip opening fence + lang tag
            raw = re.sub(r"\n?```$", "", raw.strip())    # strip closing fence
            raw = raw.strip()
        return json.loads(raw), response.provider.value
    except json.JSONDecodeError as e:
        logger.error(f"JSON parse error for '{title}': {e}\nRaw: {response.content[:300]}")
        return None


# ─────────────────────────────────────────────
# Telegram Notifier
# ─────────────────────────────────────────────
def send_telegram(title: str, company: str, url: str,
                  analysis: dict, provider: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram credentials not set — skipping notification")
        return

    score = analysis["fit_score"]
    star = "🌟" if analysis.get("dream_company_match") else ""
    bar = "🟢" * min(score, 10) + "⬜" * (10 - min(score, 10))

    text = (
        f"{star}{'🔥' if score >= 8 else '👀'} *New Job Match — {score}/10*\n"
        f"{bar}\n\n"
        f"*{title}*\n"
        f"🏢 {company}\n\n"
        f"📝 *Synopsis*\n{analysis['synopsis']}\n\n"
        f"💡 *Why it fits*\n{analysis['why_it_fits']}\n\n"
        f"📄 *Resume tip*\n{analysis['resume_suggestions']}\n\n"
        f"🤖 _Analysed by: {provider}_\n\n"
        f"[Apply Now →]({url})"
    )

    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text,
                "parse_mode": "Markdown",
                "disable_web_page_preview": False,
            },
            timeout=10,
        )
        r.raise_for_status()
        logger.info(f"📬 Telegram sent for: {title} @ {company}")
    except Exception as e:
        logger.error(f"Telegram send failed: {e}")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
def main() -> None:
    logger.info("=" * 55)
    logger.info("AI Job Hunter — starting run")
    logger.info("=" * 55)

    conn = init_db()
    jobs_df = fetch_jobs()

    if jobs_df.empty:
        logger.info("No jobs returned this run. Exiting.")
        conn.close()
        return

    new_count = processed_count = notified_count = 0

    for _, row in jobs_df.iterrows():
        url     = str(row.get("job_url", "")).strip()
        title   = str(row.get("title", "Unknown")).strip()
        company = str(row.get("company", "Unknown")).strip()
        desc    = str(row.get("description", "")).strip()

        if not url:
            continue

        if is_processed(conn, url):
            processed_count += 1
            continue

        new_count += 1
        logger.info(f"Analysing: {title} @ {company}")

        result = analyze_job(title, company, desc)
        if result is None:
            continue

        analysis, provider = result
        score = analysis.get("fit_score", 0)

        save_job(conn, url, title, company, score, provider)

        if score >= FIT_THRESHOLD:
            notified_count += 1
            send_telegram(title, company, url, analysis, provider)
        else:
            logger.info(f"  ↳ Score {score}/10 — below threshold, skipped notify")

    conn.close()
    logger.info("─" * 55)
    logger.info(
        f"Run complete — "
        f"{new_count} new | {processed_count} already seen | "
        f"{notified_count} notifications sent"
    )
    logger.info("=" * 55)


if __name__ == "__main__":
    main()
