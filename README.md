# Executive Partner Brief (weekly)

Autonomous weekly partner-intel email brief using **free sources** (Google News RSS) and **free delivery** (Google Apps Script + GitHub Actions).

## What you get
- **Partner Intelligence** email with:
  - Top 3 highlights per partner
  - A table of items tagged by **Category** and **Region (US/EU)**
- Runs weekly on a schedule, plus manual runs on demand.

## Project layout
- `scripts/partners.json`: partners + keyword rules
- `scripts/build_brief.py`: fetch → dedupe/score/tag → render `out/executive_partner_brief.html`
- `scripts/send_brief.py`: POST the HTML to Apps Script to send email
- `.github/workflows/weekly-brief.yml`: weekly scheduler
- `apps_script/SendBrief.gs`: Apps Script web app that sends the email

## Quick start

### 1) Customize partners / keywords
Edit `scripts/partners.json`.

### 2) Generate the brief locally (optional)

```bash
python3 scripts/build_brief.py
open out/executive_partner_brief.html
```

### 3) Set up the free email sender (Google Apps Script)
1. Go to `script.google.com` and create a new Apps Script project.
2. Paste the contents of `apps_script/SendBrief.gs` into the editor.
3. Set a Script Property:
   - **Key**: `BRIEF_SHARED_SECRET`
   - **Value**: a long random string you choose
4. Deploy as a Web App:
   - Deploy → New deployment → **Web app**
   - **Execute as**: Me
   - **Who has access**: Anyone
5. Copy the Web App URL (you’ll add it to GitHub secrets).

### 4) Put this code in a GitHub repo

GitHub Actions needs a repo to run the weekly schedule.

1. Create a new GitHub repo (private or public).
2. Add these files and push.

### 5) Configure GitHub Actions secrets
In your repo: Settings → Secrets and variables → Actions → New repository secret:

- **`BRIEF_APPS_SCRIPT_URL`**: the Web App URL from Apps Script
- **`BRIEF_SHARED_SECRET`**: the same secret you set in Apps Script Script Properties
- **`BRIEF_EMAIL_TO`**: your email address (recipient)

### 6) Run it
- **Manual run**: GitHub → Actions → “Executive Partner Brief” → Run workflow
- **Weekly**: runs Mondays 08:00 UTC (edit `.github/workflows/weekly-brief.yml` if you want a different time)

## Troubleshooting
- **No email arrives**: check GitHub Actions logs for the “Send email via Apps Script” step.
- **Unauthorized**: confirm `BRIEF_SHARED_SECRET` matches in Apps Script properties and GitHub secret.
- **Empty results**: broaden queries in `scripts/partners.json` (add a few synonyms per vendor).

## Upgrade path (optional, paid APIs)
If you later decide to allow small API usage costs, you can switch to the v2 design:
- Make.com weekly trigger
- Perplexity Sonar search per partner (citations)
- GPT formatting into the exact “Partner Intelligence” table

