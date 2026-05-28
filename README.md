# Spike Blog Broken Link Checker

Crawls `spike.sh/blog` and `blog.spike.sh`, checks every link on every blog page, and sends a Slack alert when broken links are found.

## How it works

1. BFS-crawls all blog pages on `spike.sh/blog/*` and `blog.spike.sh`
2. Collects every `<a href>` (internal + external)
3. Issues `HEAD` requests (falls back to `GET`) for each unique link
4. Groups broken links (4xx, 5xx, timeouts) by status code
5. Posts a formatted Slack message via Incoming Webhook

Runs automatically every day at 9:00 AM UTC via GitHub Actions.

## Setup

### 1. Create a Slack Incoming Webhook

1. Go to [api.slack.com/apps](https://api.slack.com/apps) → **Create New App** → **From scratch**
2. Name it `Link Checker Bot`, pick your workspace
3. In the left sidebar → **Incoming Webhooks** → toggle **On**
4. Click **Add New Webhook to Workspace**, pick the `#alerts` channel (or any channel)
5. Copy the webhook URL (starts with `https://hooks.slack.com/services/…`)

### 2. Add the secret to GitHub

In your repo → **Settings → Secrets and variables → Actions → New repository secret**:
- Name: `SLACK_WEBHOOK_URL`
- Value: the webhook URL from step 1

### 3. Push to GitHub

```bash
git init
git add .
git commit -m "Add Spike blog link checker"
git remote add origin https://github.com/your-org/spike-link-checker.git
git push -u origin main
```

GitHub Actions will automatically run at 9 AM UTC daily.  
You can also trigger it manually: **Actions tab → Broken Link Checker → Run workflow**.

## Run locally

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Without Slack (prints JSON to stdout):
python link_checker.py

# With Slack:
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/... python link_checker.py
```

## Configuration

Edit the constants at the top of `link_checker.py`:

| Constant | Default | Description |
|----------|---------|-------------|
| `SEED_URLS` | blog + blog.spike.sh | Starting URLs for the crawl |
| `REQUEST_TIMEOUT` | 15s | Per-request timeout |
| `CRAWL_DELAY` | 0.3s | Polite delay between requests |
| `MAX_PAGES` | 500 | Safety cap on crawled pages |
| `MAX_LINKS` | 2000 | Safety cap on checked links |

## Slack alert format

The alert groups broken links by HTTP status code and shows which blog page each broken link was found on, so you know exactly where to fix it.
