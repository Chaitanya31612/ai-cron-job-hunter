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
RESULTS_PER_SITE = 60       # Listings to pull per site per search term
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
    "full stack builder",
    "product engineer",
]

TELEGRAM_BOT_TOKEN     = os.getenv("TELEGRAM_BOT_TOKEN", "")
# High-priority channel (score 8+) — your "apply now" queue
TELEGRAM_CHAT_ID_HIGH  = os.getenv("TELEGRAM_CHAT_ID_HIGH", "")
# Secondary channel (score 6-7) — your "review later" queue
TELEGRAM_CHAT_ID_LOW   = os.getenv("TELEGRAM_CHAT_ID_LOW", "")

# ─────────────────────────────────────────────
# Pre-filter  (keyword-based fast-reject)
# Runs BEFORE any LLM call — costs zero tokens.
# Saves ~25-40% of LLM calls on a typical run.
# ─────────────────────────────────────────────
# Title-based hard rejects: indicators of seniority checked only in the title
TITLE_REJECT_KEYWORDS = [
    r"principal\b", r"architect\b", r"manager\b", r"director\b",
    r"staff\b", r"distinguished\b",
]

# General rejects: reject if found in title OR description (wrong stack, wrong domain, AI, YOE)
REJECT_KEYWORDS = [
    # Wrong stack
    r"\.net\b", r"\bc#\b", r"\bcsharp\b", r"\basp\.net\b", r"\bdjango\b", r"\bflask\b",
    r"\bfastapi\b", r"\blaravel\b", r"\bphp\b",
    # Wrong domain / mobile
    r"\bdevops\b", r"\bsre\b", r"site\s+reliability", r"infrastructure\b", r"platform\s+engineer",
    r"embedded", r"firmware",
    r"android\b", r"ios\b", r"mobile\b", r"flutter\b", r"react\s+native\b",
    # AI / ML / Data Science (AI Roles)
    r"\bai\b", r"\bgen.?ai\b", r"generative\s+ai", r"artificial\s+intelligence",
    r"\bllm\b", r"agent\s+systems?", r"\bml\b", r"machine\s+learning",
    r"deep\s+learning", r"\bnlp\b", r"computer\s+vision",
    r"data\s+scientist", r"data\s+science",
    # Too senior YOE
    r"8\+\s*years?", r"10\+\s*years?", r"12\+\s*years?", r"15\+\s*years?",
    r"8\+\s*yoe\b", r"10\+\s*yoe\b", r"12\+\s*yoe\b", r"15\+\s*yoe\b",
]

REQUIRE_ANY_KEYWORDS = [
    "javascript", "typescript", "node", "react", "vue", "next",
    "ruby", "rails", "java", "spring",
    "backend", "full.?stack", "fullstack",
    "software engineer", "software developer",
    "rest api", "graphql", "microservice",
]


VALUABLE_COMPANIES = {
    # Big Tech / Tier 1 Global
    "google", "microsoft", "amazon", "meta", "apple", "netflix", "uber", "stripe", "adobe", "salesforce", "oracle",
    "atlassian", "coinbase", "linkedin", "snowflake", "twilio", "zoom", "airbnb", "github", "gitlab", "spacex", "tesla",
    "paypal", "stripe", "adyen", "block", "square", "twitter", "x.com", "reddit", "pinterest", "snapchat", "snap", "figma",
    "canva", "bytedance", "tiktok", "grab", "gojek", "shopee", "sea group", "ibm",

    # Indian Tier 1 & 2 Tech Leaders / Unicorns / Media
    "flipkart", "swiggy", "zomato", "razorpay", "cred", "phonepe", "ola", "groww", "zerodha", "paytm", "nykaa", "meesho",
    "zepto", "urban company", "blinkit", "dream11", "inmobi", "postman", "browserstack", "freshworks", "chargebee", "jiostar",
    "jio", "reliance", "sharechat", "dailyhunt", "licious", "spinny", "cars24", "cult.fit", "upstox", "coindcx", "coinswitch",
    "slice", "jupiter", "fi money", "uni", "bharatpe", "onecard", "khatabook", "rupeek", "turtlemint", "digit", "acko",
    "cleartax", "media.net", "pocket aces", "unacademy", "byju's", "upgrad", "eruditus", "simplilearn", "physics wallah",
    "lenskart", "nykaa", "pharmeasy", "1mg", "tata 1mg", "curefit", "myntra", "ajio", "dunzo", "portcast", "shadowfax",
    "delhivery", "shiprocket", "elasticrun", "ninjacart", "dehaat", "waycool", "agrostar", "leadsquared", "darwinbox",
    "highradius", "amagi", "hasura", "innovaccer", "fractal", "mu sigma", "gupshup", "route mobile", "yellow.ai",

    # Tier 2 Global MNCs (R&D Centers in India)
    "cisco", "intel", "broadcom", "vmware", "intuit", "dell", "sap", "siemens", "hp", "ebay", "expedia", "booking.com",
    "yahoo", "qualcomm", "nvidia", "amd", "arm", "netapp", "juniper", "nutanix", "palo alto", "fortinet", "f5", "walmart",
    "target", "tesco", "goldman sachs", "morgan stanley", "jp morgan", "jpmorgan", "citi", "barclays", "hsbc",
    "standard chartered", "fidelity", "blackrock", "visa", "mastercard", "american express", "capital one", "bofa",
    "bank of america", "wells fargo", "credit suisse", "ubs", "deutsche bank", "bny mellon", "societe generale",
    "mercedes-benz", "bmw", "audi", "ford", "general motors", "toyota", "honda", "hyundai", "renault", "nissan",
    "philips", "honeywell", "bosch", "samsung", "sony", "lg", "panasonic", "hitachi", "toshiba", "ge", "general electric",
    "schneider electric", "abb", "alstom", "johnson & johnson", "novartis", "roche", "pfizer", "merck",
    "astrazeneca", "gsk", "sanofi", "bayer", "eli lilly", "abbvie", "bristol myers", "amgen", "gilead", "biogen",

    # Mid-Tier / Tech Consultancies with high bar
    "thoughtworks", "epam", "nagarro", "sapient", "publicis", "accenture", "capgemini", "cognizant", "infosys", "wipro",
    "tcs", "hcl", "tech mahindra", "l&t", "larsen", "mindtree", "persistent", "coforge", "mphasis", "virtusa", "ust",
    "hexaware", "zensar", "cybage", "tata elxsi", "ltimindtree"
}


def is_valuable_company(company: str) -> bool:
    c = company.lower().strip()
    
    # Short names that could accidentally match substrings of other words (e.g. "dell" in "deloitte", "jio" in "jiostar")
    short_companies = {
        "jio", "lg", "hp", "ge", "abb", "tcs", "l&t", "sap", "gsk", "ubs", 
        "bofa", "f5", "arm", "amd", "p&g", "citi", "dell", "ford", "hcl", "ibm"
    }
    
    # Tokenize words using boundaries
    tokens = re.findall(r"\b[a-z0-9&']+\b", c)
    
    for vc in VALUABLE_COMPANIES:
        if vc in short_companies:
            if vc in tokens:
                return True
        else:
            if vc in c:
                return True
    return False


def quick_filter(title: str, company: str, description: str) -> tuple[bool, str]:
    """
    Fast keyword-based pre-screen before spending an LLM call.
    Returns (should_analyse, reason).
    """
    title_lower = title.lower()
    desc_lower = description.lower()
    text = (title_lower + " " + desc_lower)

    # 1. General rejects (checked anywhere in title or description)
    for pattern in REJECT_KEYWORDS:
        if re.search(pattern, text):
            return False, f"pre-filter reject (general): '{pattern}'"

    # 2. Title-based rejects
    for pattern in TITLE_REJECT_KEYWORDS:
        if re.search(pattern, title_lower):
            return False, f"pre-filter reject (title): '{pattern}'"

    # 3. Bypass tech keyword check if the company is recognized as valuable/tier-1/2/3
    if is_valuable_company(company):
        return True, "passed (valuable company bypass)"

    # 4. Require at least one relevant tech keyword in title/description
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
LLM_MIN_INTERVAL_SEC = 5.0   # seconds between LLM calls


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

    "avoid": [
        "Python-only roles (e.g. Django, Flask, FastAPI full-stack)",
        ".NET / C# roles",
        "Data Science / ML / AI / GenAI / LLM / Agent roles",
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
        "Remote — India or US",
        "Hybrid — Noida / Delhi NCR",
        "Hybrid — Bengaluru (open to relocation)",
        "In-office — Noida / Delhi NCR",
        "In-office — Bengaluru (open to relocation)",
    ],
    "location_note": (
        "Do NOT penalise roles for being remote-US, hybrid, or in-office. "
        "The candidate is open to all of these. Only flag location as a concern "
        "if the role explicitly requires being physically present outside India "
        "(e.g. on-site US/UK only with no remote option)."
    ),
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


def normalize_url(url: str) -> str:
    """
    Normalizes a job URL to a canonical format to prevent duplicate processing
    due to dynamic query parameters.
    """
    url = url.strip()
    if not url:
        return ""
    
    # 1. LinkedIn Job ID extraction
    # Patterns: 
    # - https://www.linkedin.com/jobs/view/1234567890/
    # - https://in.linkedin.com/jobs/view/1234567890
    # - https://www.linkedin.com/jobs/view/1234567890?refId=...
    # - https://www.linkedin.com/jobs/collections/recommended/?currentJobId=1234567890
    linkedin_view_match = re.search(r"linkedin\.com/(?:jobs/)?view/(\d+)", url, re.IGNORECASE)
    if linkedin_view_match:
        return f"linkedin:{linkedin_view_match.group(1)}"
        
    linkedin_query_match = re.search(r"currentJobId=(\d+)", url, re.IGNORECASE)
    if linkedin_query_match:
        return f"linkedin:{linkedin_query_match.group(1)}"
        
    # 2. Indeed Job ID extraction
    # Patterns:
    # - https://in.indeed.com/viewjob?jk=f8f91f438e767fcd
    # - https://in.indeed.com/rc/clk?jk=f8f91f438e767fcd
    indeed_match = re.search(r"jk=([a-zA-Z0-9]+)", url, re.IGNORECASE)
    if indeed_match:
        return f"indeed:{indeed_match.group(1)}"
        
    # 3. Fallback: strip query parameters and trailing slashes, remove protocol
    clean_url = url.split("?")[0].rstrip("/")
    clean_url = re.sub(r"^https?://(www\.)?", "", clean_url, flags=re.IGNORECASE)
    return clean_url.lower()


def is_processed(conn: sqlite3.Connection, url: str) -> bool:
    norm_url = normalize_url(url)
    cursor = conn.execute(
        "SELECT 1 FROM processed_jobs WHERE job_url = ? OR job_url = ?",
        (url, norm_url)
    )
    return bool(cursor.fetchone())


def is_title_company_processed(conn: sqlite3.Connection, title: str, company: str) -> bool:
    """
    Checks if a job with the same (case-insensitive) title and company has been
    processed in the last 14 days to prevent duplicate notifications for reposted roles.
    """
    t = title.lower().strip()
    c = company.lower().strip()
    if not t or not c or t == "unknown" or c == "unknown":
        return False
        
    cursor = conn.execute(
        """
        SELECT 1 FROM processed_jobs 
        WHERE LOWER(title) = ? AND LOWER(company) = ? 
        AND processed_at >= datetime('now', '-14 days')
        LIMIT 1
        """,
        (t, c)
    )
    return bool(cursor.fetchone())


def save_job(conn: sqlite3.Connection, url: str, title: str,
             company: str, score: int, provider: str) -> None:
    norm_url = normalize_url(url)
    conn.execute(
        "INSERT OR IGNORE INTO processed_jobs "
        "(job_url, title, company, score, provider) VALUES (?,?,?,?,?)",
        (norm_url if norm_url else url, title, company, score, provider)
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
- Location note: {USER_PROFILE['location_note']}
- Dream companies (higher weight if matched): {', '.join(USER_PROFILE['dream_companies'])}

Experience Summary:
{USER_PROFILE['experience']}

Scoring Rules:
- Score 8–10: Strong alignment on tech stack, YOE range, and role type. Apply immediately.
- Score 6–7: Decent match, some gaps but learnable. Worth considering.
- Score 1–5: Poor fit — wrong stack, too senior/junior, domain mismatch.
- Score 0: Explicitly in the avoid list (e.g. Python-only, .NET, DevOps-only, Data Science).

Ruby on Rails (RoR) Bias (CRITICAL):
- Ruby on Rails is the candidate's primary and working experience for the past 3 years.
- There are fewer RoR opportunities, so they are extremely important to the candidate.
- You MUST apply a strong positive bias to any role that mentions or requires Ruby on Rails.
- If the role lists Ruby on Rails or Ruby as a key skill/technology, automatically score it 8 or higher (unless it is a senior role requiring >5 YOE, or is explicitly in the avoid list), and explain this core expertise match in "why_it_fits".

Additionally, provide a brief company overview and estimated salary range for this role.
For salary: use any known data about this company's pay bands for similar roles in India.
For company overview: mention company type, size (if known), domain, and reputation.
If you are uncertain, give a best-effort estimate and prefix with "~" (e.g. "~18–25 LPA").
These are supplementary hints — the candidate will do their own research if interested.

Return ONLY a valid JSON object — no markdown, no preamble:
{{
    "fit_score": <integer 0–10>,
    "synopsis": "<2–3 sentence overview of the role and what it requires>",
    "why_it_fits": "<concise paragraph on alignment or misalignment with the candidate>",
    "dream_company_match": <true if company is in dream list, else false>,
    "company_overview": "<1–2 sentences: company type, domain, size/stage, and reputation if known — or 'Not enough info' if unknown>",
    "salary_range": "<estimated salary range in LPA for this role at this company in India — prefix with ~ if uncertain, or 'Not available' if no data>",
    "resume_suggestions": "<specific, actionable advice on what to emphasise or rearrange in the resume for this job — or 'No changes needed' if already well aligned>"
}}
"""


def analyze_job(title: str, company: str, description: str) -> tuple:
    if not description or len(description.strip()) < 50:
        logger.warning(f"Skipping '{title}' — description too short or missing")
        return None, "too_short"

    # Pre-filter: keyword check before spending any LLM tokens
    ok, reason = quick_filter(title, company, description)
    if not ok:
        logger.info(f"  ↳ {reason} — skipped LLM")
        return None, f"pre-filter: {reason}"

    job_content = f"Job Title: {title}\nCompany: {company}\n\nJob Description:\n{description}"

    response = rate_limited_analyze(
        content=job_content,
        prompt=ANALYSIS_PROMPT,
    )

    if not response.success:
        logger.error(f"LLM failed for '{title}': {response.error}")
        return None, f"llm_failed: {response.error}"

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
        return None, "json_failed"


# ─────────────────────────────────────────────
# Telegram Notifier
# ─────────────────────────────────────────────
def _post_telegram(chat_id: str, text: str, title: str, company: str) -> None:
    """Low-level helper — posts a message to a specific chat_id."""
    r = requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        json={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": False,
        },
        timeout=10,
    )
    r.raise_for_status()
    logger.info(f"📬 Telegram sent for: {title} @ {company}")


def send_telegram(title: str, company: str, url: str,
                  analysis: dict, provider: str) -> None:
    """
    Routes the notification to the correct Telegram channel based on score:
      - score 8+  → TELEGRAM_CHAT_ID_HIGH  ("apply now" queue)
      - score 6-7 → TELEGRAM_CHAT_ID_LOW   ("review later" queue)
    Falls back gracefully if a channel ID is not configured.
    """
    if not TELEGRAM_BOT_TOKEN:
        logger.warning("TELEGRAM_BOT_TOKEN not set — skipping notification")
        return

    score = analysis["fit_score"]
    is_hot = score >= 8

    # Pick the right channel
    if is_hot:
        chat_id  = TELEGRAM_CHAT_ID_HIGH
        channel  = "high-priority"
    else:
        chat_id  = TELEGRAM_CHAT_ID_LOW
        channel  = "regular"

    if not chat_id:
        logger.warning(
            f"TELEGRAM_CHAT_ID_{('HIGH' if is_hot else 'LOW')} not set — "
            f"skipping notification for '{title}'"
        )
        return

    star = "🌟" if analysis.get("dream_company_match") else ""
    bar  = "🟢" * min(score, 10) + "⬜" * (10 - min(score, 10))

    company_overview = analysis.get("company_overview", "Not available")
    salary_range     = analysis.get("salary_range", "Not available")

    label = "🔥 *HOT MATCH*" if is_hot else "👀 *Good Match*"

    text = (
        f"{star}{label} — {score}/10\n"
        f"{bar}\n\n"
        f"*{title}*\n"
        f"🏢 {company}\n\n"
        f"📝 *Synopsis*\n{analysis['synopsis']}\n\n"
        f"💡 *Why it fits*\n{analysis['why_it_fits']}\n\n"
        f"🏦 *Company*\n{company_overview}\n\n"
        f"💰 *Est. Salary* (AI best-effort)\n{salary_range}\n\n"
        f"📄 *Resume tip*\n{analysis['resume_suggestions']}\n\n"
        f"🤖 _Analysed by: {provider}_\n\n"
        f"[Apply Now →]({url})"
    )

    try:
        _post_telegram(chat_id, text, title, company)
        logger.info(f"  ↳ Routed to [{channel}] channel (score {score})")
    except Exception as e:
        logger.error(f"Telegram send failed ({channel}): {e}")


def send_summary_telegram(
    scraped: int,
    already_seen: int,
    dupes: int,
    rejected: int,
    llm_success: int,
    llm_failed: int,
    high: int,
    regular: int,
    below: int
) -> None:
    """Sends a summary message to Telegram at the end of the processing run."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID_HIGH:
        logger.warning("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID_HIGH not set — skipping summary notification")
        return

    text = (
        f"📊 *AI Job Hunter Run Summary*\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"📥 *Scraped Listings:* {scraped}\n"
        f"🔄 *Already Seen:* {already_seen}\n"
        f"👯 *Duplicates Skipped:* {dupes}\n"
        f"🚫 *Pre-Filter Rejected:* {rejected}\n"
        f"🧠 *LLM Runs:* {llm_success} (failed: {llm_failed})\n\n"
        f"📈 *LLM Score Outcomes:*\n"
        f"  🔥 *High Priority (8+):* {high}\n"
        f"  👀 *Regular Priority (6-7):* {regular}\n"
        f"  💤 *Below Threshold (<6):* {below}\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"📬 *Total Notifications Sent:* {high + regular}"
    )

    try:
        _post_telegram(TELEGRAM_CHAT_ID_HIGH, text, "Summary Message", "N/A")
        logger.info("📬 Telegram run summary sent successfully")
    except Exception as e:
        logger.error(f"Telegram summary send failed: {e}")


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
        send_summary_telegram(0, 0, 0, 0, 0, 0, 0, 0, 0)
        return

    scraped_count = len(jobs_df)
    already_seen_count = 0
    skipped_dupe_count = 0
    pre_filter_reject_count = 0
    llm_success_count = 0
    llm_failed_count = 0
    high_priority_count = 0
    regular_priority_count = 0
    below_threshold_count = 0

    new_count = processed_count = notified_count = 0

    # In-run dedup by (title, company) — catches same job posted under
    # different URLs (e.g. same Scoutit role on LinkedIn vs Indeed, or
    # scraped by multiple search terms returning slightly different URLs).
    seen_title_company: set[str] = set()

    for _, row in jobs_df.iterrows():
        try:
            url     = str(row.get("job_url", "")).strip()
            title   = str(row.get("title", "Unknown")).strip()
            company = str(row.get("company", "Unknown")).strip()
            desc    = str(row.get("description", "")).strip()

            if not url:
                continue

            if is_processed(conn, url):
                already_seen_count += 1
                processed_count += 1
                continue

            # Secondary dedup: same title+company already seen this run
            tc_key = f"{title.lower()}||{company.lower()}"
            if tc_key in seen_title_company:
                skipped_dupe_count += 1
                logger.info(f"  ↳ Duplicate in-run ({title} @ {company}) — skipped | url: {url}")
                continue
            seen_title_company.add(tc_key)

            # Historical title+company check
            if is_title_company_processed(conn, title, company):
                skipped_dupe_count += 1
                logger.info(f"  ↳ Duplicate historical ({title} @ {company}) — skipped | url: {url}")
                continue

            new_count += 1
            logger.info(f"Analysing: {title} @ {company} | url: {url}")

            result = analyze_job(title, company, desc)
            analysis, provider_or_reason = result
            if analysis is None:
                if "pre-filter:" in provider_or_reason:
                    pre_filter_reject_count += 1
                elif provider_or_reason != "too_short":
                    llm_failed_count += 1
                continue

            llm_success_count += 1
            provider = provider_or_reason
            score = analysis.get("fit_score", 0)

            # Check if the role is a Ruby on Rails role and apply bias boost
            desc_lower = desc.lower()
            title_lower = title.lower()
            has_ruby = bool(re.search(r"\bruby\b", desc_lower) or re.search(r"\bruby\b", title_lower))
            has_rails = bool(re.search(r"\brails\b", desc_lower) or re.search(r"\brails\b", title_lower))
            has_ror = bool(re.search(r"\bror\b", desc_lower) or re.search(r"\bror\b", title_lower))
            has_ruby_on_rails = bool(re.search(r"\bruby\s+on\s+rails\b", desc_lower) or re.search(r"\bruby\s+on\s+rails\b", title_lower))
            is_ror = has_ruby or has_rails or has_ror or has_ruby_on_rails

            if is_ror and 0 < score < 8:
                logger.info(f"  ↳ Python-side RoR boost: raising score from {score} to 8")
                score = 8
                analysis["fit_score"] = 8
                analysis["why_it_fits"] = f"[RoR Boost Applied] {analysis.get('why_it_fits', '')}"

            save_job(conn, url, title, company, score, provider)

            if score >= FIT_THRESHOLD:
                notified_count += 1
                if score >= 8:
                    high_priority_count += 1
                else:
                    regular_priority_count += 1
                send_telegram(title, company, url, analysis, provider)
            else:
                below_threshold_count += 1
                logger.info(f"  ↳ Score {score}/10 — below threshold, skipped notify")
        except Exception as e:
            logger.error(f"❌ Error processing job '{title}' @ '{company}': {e}", exc_info=True)
            continue

    conn.close()
    logger.info("─" * 55)
    logger.info(
        f"Run complete — "
        f"{new_count} new | {processed_count} already seen | "
        f"{skipped_dupe_count} in-run dupes skipped | "
        f"{notified_count} notifications sent"
    )
    logger.info("=" * 55)

    send_summary_telegram(
        scraped=scraped_count,
        already_seen=already_seen_count,
        dupes=skipped_dupe_count,
        rejected=pre_filter_reject_count,
        llm_success=llm_success_count,
        llm_failed=llm_failed_count,
        high=high_priority_count,
        regular=regular_priority_count,
        below=below_threshold_count,
    )


if __name__ == "__main__":
    main()
