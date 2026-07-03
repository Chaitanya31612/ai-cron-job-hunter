# 🚨 AI Job Hunter (India Developer Roles)

An automated, budget-friendly Python-based job aggregator and recommendation system. It pulls developer job listings from major platforms in India, filters and scores them against your profile using Gemini/OpenRouter, and alerts you instantly on Telegram.

---

## 🛠️ Status & Plan Reference
For detailed implementation progress, rate limits, and fallback logic, please see the [job_hunter_analysis.md](file:///home/chaitanya/.gemini/antigravity-cli/brain/6beb4f42-07a8-4e56-a512-c7cd750ef027/job_hunter_analysis.md) plan.

---

## 📅 Run Schedule (India Standard Time - IST)
To stay fully inside free limits, the system operates only when recruiter posting activity is high, skipping nights entirely:
* **Weekdays (Mon-Fri):** Every **2 hours** between **9:30 AM and 11:30 PM IST** (8 runs/day).
* **Weekends (Sat-Sun):** **3 times** a day at **12:00 PM (Noon)**, **7:00 PM**, and **11:30 PM IST** (3 runs/day).
* **Nights:** Entirely skipped (no executions between 11:30 PM and 9:30 AM).

---

## 🧠 LLM Providers & Fallback
1. **Primary:** Google AI Studio (`gemini-3.1-flash-lite` - 500 RPD Free)
2. **Fallback 1:** OpenRouter (`meta-llama/llama-3.3-70b-instruct:free` - 50 RPD Free)
3. **Fallback 2:** OpenRouter (`openai/gpt-oss-120b:free` - 50 RPD Free)
4. **Fallback 3 (Last resort):** Groq (`meta-llama/llama-3.3-70b-versatile` - 100K daily tokens)

---

## 📋 Implementation Tasks

- [ ] **Task 1: Prerequisites (Gather Credentials)** — pending your keys
- [x] **Task 2: Scaffold (.gitignore, requirements.txt, venv with Python 3.12.8)** ✅
- [x] **Task 3: Enhance `ai/` Factory with Fallback & OpenRouter** ✅
- [x] **Task 4: Core orchestrator script (`job_hunter.py`)** ✅
- [x] **Task 5: Local Cron Setup (`setup_cron.sh`)** ✅
- [x] **Task 6: GitHub Actions Workflow (`.github/workflows/job_hunter.yml`)** ✅

---

## ☁️ GitHub Actions Setup (Path A — cloud, recommended)

```bash
# 1. Push the repo to GitHub (make it public for unlimited free minutes)
git init
git add .
git commit -m "feat: AI Job Hunter initial setup"
git remote add origin https://github.com/<your-username>/<repo-name>.git
git push -u origin main

# 2. Add secrets in GitHub UI:
#    Repository → Settings → Secrets and variables → Actions → New repository secret
#    Add each of:
#      GEMINI_API_KEY
#      OPENROUTER_API_KEY
#      GROQ_API_KEY
#      TELEGRAM_BOT_TOKEN
#      TELEGRAM_CHAT_ID

# 3. Test manually before the first scheduled run:
#    GitHub → Actions tab → "AI Job Hunter" → Run workflow → Run workflow
```

The workflow runs on the exact IST schedule and uses `actions/cache` to persist
`jobs.db` across runs for deduplication — no database commits to git needed.

> ⚠️ GitHub Actions cron schedules can run a few minutes late under load.


---

## 🚀 How to Run (Manual / Test)

```bash
# 1. Activate the local venv
cd ~/Desktop/Development/learning/ai-cron-job-notifications
source venv/bin/activate

# 2. Copy the credential template and fill in your keys
cp .env.example .env
nano .env   # add GEMINI_API_KEY, OPENROUTER_API_KEY, GROQ_API_KEY,
            # TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

# 3. Run the script
python job_hunter.py
# Watch terminal for live logs.
# Check Telegram for a notification if any job scores >= 7/10.
```

---

## ⏰ Local Cron Setup (Path B — your machine)

```bash
# 1. Make executable (only once)
chmod +x setup_cron.sh

# 2. Run setup — checks .env, creates run_local.sh, installs crontab
./setup_cron.sh

# 3. Verify installed entries
crontab -l

# 4. Manual test run
./run_local.sh
tail -f cron.log

# 5. Uninstall all job hunter cron entries
crontab -l | grep -v 'AI Job Hunter' | grep -v 'run_local.sh' | crontab -
```

> ⚠️ Your machine must be on and connected for local cron to fire.
