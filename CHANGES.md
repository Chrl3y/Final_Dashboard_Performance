# Change Log — Nova Dashboard

---

## [2026-03-15] — AI Insights Tab

**Merge commit:** `849bdbd`
**Feature commit:** `d26c015`
**Branch:** `claude/plan-ai-dashboard-features-S60Op`
**Merged into:** `main`

---

### Summary

Adds a live **"✦ Insights"** tab to the dashboard header. When clicked it
replaces the dashboard view with an AI-generated management briefing powered
by **Claude claude-sonnet-4-6** (Anthropic). The AI receives live portfolio data
(KPIs, branch performance, officer tables, arrears aging, collection vs target)
and returns a structured 5-section analysis.

---

### Files Changed

| File | Type | What changed |
|------|------|-------------|
| `server.js` | Modified | Added `ANTHROPIC_API_KEY` env var + `/api/nova/ai-insight` POST endpoint |
| `Nova-dashboard-live.html` | Modified | CSS, HTML panel, JS functions for Insights feature |
| `.env.example` | Created | New file — environment variable template |

---

### Detailed Changes

#### `server.js`
- **Line 22** — Added `ANTHROPIC_API_KEY` to the env destructure block
- **Lines 706–984** — New `POST /api/nova/ai-insight` endpoint added before
  the 404 handler. It runs 14 parallel SQL queries (KPIs, branch performance,
  officer performance, arrears aging, collection target), builds a structured
  prompt, and calls the Anthropic API. Returns `{ success, insight, period, generatedAt }`.

#### `Nova-dashboard-live.html`
- **CSS block** — Added `.insights-tab-btn`, `#insightsPanel`, `.context-area`,
  `.generate-btn`, `.insight-section` + 5 variant classes, `.insight-error`,
  and `.insight-results` styles.
- **Header controls** — Added `<button class="insights-tab-btn">✦ Insights</button>`
  between the Refresh and Print buttons.
- **HTML body** — Added `#insightsPanel` div (after the signature, before `#kpiGrid`)
  containing: context textarea, Generate button, error element, and 5 result cards.
- **JavaScript block** — Added `toggleInsights()`, `parseInsightSections()`,
  `renderMarkdown()`, and `generateInsights()` functions before the `/* ══ EVENTS ══ */`
  comment.

#### `.env.example` (new file)
- Documents all required environment variables for the server, including the
  new `ANTHROPIC_API_KEY` field.

---

### How to Revert

#### Option A — Revert via git (recommended, preserves full history)

```bash
# 1. Revert the merge commit (undoes the entire feature in one step)
git revert -m 1 849bdbd --no-edit

# 2. Push the revert
git push origin main
```

This creates a new "revert" commit on main. The feature's commits remain in
history and can be re-applied at any time.

#### Option B — Hard reset (destructive, erases commits)

```bash
# ⚠ Only use this if no one else has pulled main since the merge
git reset --hard 05f873f   # resets main to the pre-feature state
git push origin main --force
```

#### Option C — Revert individual files without touching git history

```bash
# Restore server.js and the HTML to their state before the feature
git show 05f873f:server.js > server.js
git show 05f873f:Nova-dashboard-live.html > Nova-dashboard-live.html
rm .env.example
git add server.js Nova-dashboard-live.html .env.example
git commit -m "revert: remove AI insights feature manually"
git push origin main
```

#### To re-apply the feature after a revert

```bash
# If you used Option A (git revert), re-apply by reverting the revert:
git revert <revert-commit-hash> --no-edit
git push origin main

# Or cherry-pick the original feature commit:
git cherry-pick d26c015
git push origin main
```

---

### Dependency Requirements

The feature uses `axios` (already present in `server.js`) — no new npm packages needed.

### Environment Variable Required

```
ANTHROPIC_API_KEY=sk-ant-api03-...
```

Add to your `.env` file. Without it the endpoint returns HTTP 503 gracefully
and the dashboard continues to function normally.

---

## [2026-03-15] — Initial Commit

**Commit:** `05f873f`

Initial Nova dashboard files: `server.js` and `Nova-dashboard-live.html`.
