require('dotenv').config();
const express  = require('express');
const axios    = require('axios');
const jwt      = require('jsonwebtoken');
const cors     = require('cors');
const https    = require('https');
const path     = require('path');

const app = express();
app.use(express.json());
app.use(cors());
app.use(express.static('.'));

app.get('/', (_req, res) => res.sendFile(path.join(__dirname, 'Nova-dashboard-live.html')));
app.get('/mcp', (_req, res) => res.sendFile(path.join(__dirname, 'mcp-ui.html')));

const {
  METABASE_SITE_URL,
  METABASE_URL,
  METABASE_USERNAME,
  METABASE_PASSWORD,
  METABASE_SECRET,
  ANTHROPIC_API_KEY,
  PORT          = 3000,
  CACHE_TTL,
  CACHE_TTL_SECONDS,
  NOVA_DB_ID    = 2
} = process.env;

if (!METABASE_URL) console.warn('[WARN] METABASE_URL not set');
const cacheTtlSeconds = parseInt(CACHE_TTL || CACHE_TTL_SECONDS || '3600', 10);
if (!Number.isFinite(cacheTtlSeconds)) console.warn('[WARN] CACHE_TTL not a number, defaulting to 3600');

// ─── SSL agent (self-signed cert on Metabase host) ───────────────────────────
const httpsAgent = new https.Agent({ rejectUnauthorized: false });

// ─── Session cache ────────────────────────────────────────────────────────────
let _session = null, _sessionAt = 0;

async function getSession() {
  const now = Date.now() / 1000;
  if (_session && now - _sessionAt < cacheTtlSeconds) return _session;
  if (!METABASE_USERNAME || !METABASE_PASSWORD)
    throw new Error('METABASE_USERNAME / METABASE_PASSWORD not set');
  const r = await axios.post(`${METABASE_URL}/api/session`,
    { username: METABASE_USERNAME, password: METABASE_PASSWORD },
    { timeout: 10000, httpsAgent });
  _session   = r.data.id;
  _sessionAt = now;
  console.log('[Metabase] Session renewed');
  return _session;
}

// ─── SQL runner ───────────────────────────────────────────────────────────────
async function runSQL(sql) {
  const session = await getSession();
  const r = await axios.post(`${METABASE_URL}/api/dataset`, {
    database: parseInt(NOVA_DB_ID),
    type: 'native',
    native: { query: sql }
  }, { headers: { 'X-Metabase-Session': session }, timeout: 60000, httpsAgent });
  const { rows, cols } = r.data.data;
  return rows.map(row => Object.fromEntries(cols.map((c, i) => [c.name, row[i]])));
}

// ─── Value helpers ────────────────────────────────────────────────────────────
const num  = (rows, f, fb = 0) => { if (!rows?.[0]) return fb; const v = rows[0][f]; return v == null ? fb : parseFloat(v) || fb; };
const int  = (rows, f)         => { if (!rows?.[0]) return 0;  return parseInt(rows[0][f]) || 0; };
const pct  = (cur, prev)       => prev ? Math.round((cur - prev) / prev * 1000) / 10 : null;

// ─── AI helper ───────────────────────────────────────────────────────────────
// Models: sonnet for quality (briefings, SQL), haiku for speed (summaries, short explanations)
const AI_MODEL_STRONG = 'claude-sonnet-4-6';
const AI_MODEL_FAST   = 'claude-haiku-4-5-20251001';

async function callClaude(systemPrompt, userMessage, maxTokens = 2048, model = AI_MODEL_FAST) {
  if (!ANTHROPIC_API_KEY) throw new Error('ANTHROPIC_API_KEY not set in .env');
  const r = await axios.post('https://api.anthropic.com/v1/messages', {
    model,
    max_tokens: maxTokens,
    system: systemPrompt,
    messages: [{ role: 'user', content: userMessage }]
  }, {
    headers: {
      'x-api-key': ANTHROPIC_API_KEY,
      'anthropic-version': '2023-06-01',
      'content-type': 'application/json'
    },
    timeout: 120000
  });
  return r.data.content[0].text;
}

// ─── Period WHERE helpers ─────────────────────────────────────────────────────
// RULE: Collections → DATE(mlt.created_date)  |  Disbursements → ml.disbursedon_date
//       Expected payments → rs.duedate
function periodWhere(col, p) {
  switch (p) {
    case 'today':    return `DATE(${col}) = CURDATE()`;
    case 'week':     return `YEARWEEK(${col}, 1) = YEARWEEK(CURDATE(), 1)`;
    case 'month':    return `YEAR(${col}) = YEAR(CURDATE()) AND MONTH(${col}) = MONTH(CURDATE())`;
    case 'quarter':  return `YEAR(${col}) = YEAR(CURDATE()) AND QUARTER(${col}) = QUARTER(CURDATE())`;
    case 'year':     return `YEAR(${col}) = YEAR(CURDATE())`;
    case 'uptodate': return `DATE(${col}) <= CURDATE()`;
    // Rolling periods — last N days/months, always ends today
    case 'rolling7':   return `DATE(${col}) BETWEEN DATE_SUB(CURDATE(), INTERVAL 6 DAY) AND CURDATE()`;
    case 'rolling30':  return `DATE(${col}) BETWEEN DATE_SUB(CURDATE(), INTERVAL 29 DAY) AND CURDATE()`;
    case 'rolling90':  return `DATE(${col}) BETWEEN DATE_SUB(CURDATE(), INTERVAL 89 DAY) AND CURDATE()`;
    case 'rolling12m': return `DATE(${col}) BETWEEN DATE_SUB(CURDATE(), INTERVAL 12 MONTH) AND CURDATE()`;
    default:         return `YEAR(${col}) = YEAR(CURDATE()) AND MONTH(${col}) = MONTH(CURDATE())`;
  }
}
function prevWhere(col, p) {
  switch (p) {
    case 'today':    return `DATE(${col}) = DATE_SUB(CURDATE(), INTERVAL 1 DAY)`;
    case 'week':     return `YEARWEEK(${col}, 1) = YEARWEEK(DATE_SUB(CURDATE(), INTERVAL 7 DAY), 1)`;
    case 'month':    return `YEAR(${col}) = YEAR(DATE_SUB(CURDATE(), INTERVAL 1 MONTH)) AND MONTH(${col}) = MONTH(DATE_SUB(CURDATE(), INTERVAL 1 MONTH))`;
    case 'quarter':  return `YEAR(${col}) = YEAR(DATE_SUB(CURDATE(), INTERVAL 3 MONTH)) AND QUARTER(${col}) = QUARTER(DATE_SUB(CURDATE(), INTERVAL 3 MONTH))`;
    case 'year':     return `YEAR(${col}) = YEAR(DATE_SUB(CURDATE(), INTERVAL 1 YEAR))`;
    case 'uptodate': return `DATE(${col}) < DATE_SUB(CURDATE(), INTERVAL 1 MONTH)`;
    // Rolling: shift window back by same number of days
    case 'rolling7':   return `DATE(${col}) BETWEEN DATE_SUB(CURDATE(), INTERVAL 13 DAY) AND DATE_SUB(CURDATE(), INTERVAL 7 DAY)`;
    case 'rolling30':  return `DATE(${col}) BETWEEN DATE_SUB(CURDATE(), INTERVAL 59 DAY) AND DATE_SUB(CURDATE(), INTERVAL 30 DAY)`;
    case 'rolling90':  return `DATE(${col}) BETWEEN DATE_SUB(CURDATE(), INTERVAL 179 DAY) AND DATE_SUB(CURDATE(), INTERVAL 90 DAY)`;
    case 'rolling12m': return `DATE(${col}) BETWEEN DATE_SUB(CURDATE(), INTERVAL 24 MONTH) AND DATE_SUB(CURDATE(), INTERVAL 12 MONTH)`;
    default:         return `YEAR(${col}) = YEAR(DATE_SUB(CURDATE(), INTERVAL 1 MONTH)) AND MONTH(${col}) = MONTH(DATE_SUB(CURDATE(), INTERVAL 1 MONTH))`;
  }
}

const dateRx = /^\d{4}-\d{2}-\d{2}$/;
const safeDate = v => dateRx.test(v || '') ? v : null;
const parseDate = v => { const d = new Date(`${v}T00:00:00Z`); return Number.isNaN(d.getTime()) ? null : d; };
const formatDate = d => d.toISOString().slice(0, 10);
function shiftRange(start, end) {
  const s = parseDate(start), e = parseDate(end);
  if (!s || !e) return null;
  const diffDays = Math.round((e - s) / 86400000) + 1;
  const prevEnd = new Date(s); prevEnd.setUTCDate(prevEnd.getUTCDate() - 1);
  const prevStart = new Date(prevEnd); prevStart.setUTCDate(prevStart.getUTCDate() - diffDays + 1);
  return { prevStart: formatDate(prevStart), prevEnd: formatDate(prevEnd) };
}
function rangeWhere(col, p, start, end) {
  if (start || end) {
    const c = `DATE(${col})`;
    if (start && end) return `${c} BETWEEN '${start}' AND '${end}'`;
    if (start) return `${c} >= '${start}'`;
    return `${c} <= '${end}'`;
  }
  return periodWhere(col, p);
}

// ─── expectedWhere ────────────────────────────────────────────────────────────
// DEFINITION: Expected Collections = sum of scheduled installment amounts
// with duedate WITHIN the selected period window, capped at today.
// "If today — only loans due today. If week — loans due each day Mon→today."
//
// This matches the user's operational expectation and avoids two bugs:
//   Bug A (original): same YEARWEEK window → ~0 for weekly (monthly loan freq)
//   Bug B (v2): cumulative ceiling LAST_DAY/Dec-31 → includes FUTURE dues,
//               causing month=2.98B > all-time=1.75B (nonsensical)
//
// Correct: duedate BETWEEN period_start AND CURDATE() — past/current only.
// For 'uptodate': duedate <= CURDATE() (same as before, already correct).
function expectedWhere(col, p, startDate, endDate) {
  // Custom range: duedate BETWEEN start AND MIN(end, CURDATE()) — cap at today
  if (startDate || endDate) {
    const s = startDate ? `'${startDate}'` : null;
    const e = endDate ? `LEAST(DATE('${endDate}'), CURDATE())` : 'CURDATE()';
    if (s && endDate) return `DATE(${col}) BETWEEN ${s} AND ${e}`;
    if (s)            return `DATE(${col}) >= ${s} AND DATE(${col}) <= CURDATE()`;
    return `DATE(${col}) <= ${e}`;
  }
  // Standard periods: duedate IN period window AND <= CURDATE() (no future dates)
  switch (p) {
    case 'today':
      return `DATE(${col}) = CURDATE()`;
    case 'week':
      // ISO week Monday→today (WEEKDAY: Mon=0, Sun=6)
      return `YEARWEEK(${col}, 1) = YEARWEEK(CURDATE(), 1) AND DATE(${col}) <= CURDATE()`;
    case 'month':
      return `YEAR(${col}) = YEAR(CURDATE()) AND MONTH(${col}) = MONTH(CURDATE()) AND DATE(${col}) <= CURDATE()`;
    case 'quarter':
      return `YEAR(${col}) = YEAR(CURDATE()) AND QUARTER(${col}) = QUARTER(CURDATE()) AND DATE(${col}) <= CURDATE()`;
    case 'year':
      return `YEAR(${col}) = YEAR(CURDATE()) AND DATE(${col}) <= CURDATE()`;
    case 'uptodate':
      return `DATE(${col}) <= CURDATE()`;
    case 'rolling7':
      return `DATE(${col}) BETWEEN DATE_SUB(CURDATE(), INTERVAL 6 DAY) AND CURDATE()`;
    case 'rolling30':
      return `DATE(${col}) BETWEEN DATE_SUB(CURDATE(), INTERVAL 29 DAY) AND CURDATE()`;
    case 'rolling90':
      return `DATE(${col}) BETWEEN DATE_SUB(CURDATE(), INTERVAL 89 DAY) AND CURDATE()`;
    case 'rolling12m':
      return `DATE(${col}) BETWEEN DATE_SUB(CURDATE(), INTERVAL 12 MONTH) AND CURDATE()`;
    default:
      return `YEAR(${col}) = YEAR(CURDATE()) AND MONTH(${col}) = MONTH(CURDATE()) AND DATE(${col}) <= CURDATE()`;
  }
}

// ─── overdueWhere ─────────────────────────────────────────────────────────────
// Missed installments: duedate falls within period window AND is before today.
// Identical to expectedWhere but caps at CURDATE()-1 instead of CURDATE()
// (today's dues are not yet "missed" — they're still due today).
function overdueWhere(col, p, startDate, endDate) {
  if (startDate || endDate) {
    const s = startDate ? `'${startDate}'` : null;
    const e = endDate
      ? `LEAST(DATE('${endDate}'), DATE_SUB(CURDATE(), INTERVAL 1 DAY))`
      : `DATE_SUB(CURDATE(), INTERVAL 1 DAY)`;
    if (s) return `DATE(${col}) BETWEEN ${s} AND ${e}`;
    return `DATE(${col}) <= ${e}`;
  }
  switch (p) {
    case 'today':
      // Today's dues haven't been missed yet — show them as overdue anyway for ops visibility
      return `DATE(${col}) = CURDATE()`;
    case 'week':
      return `YEARWEEK(${col}, 1) = YEARWEEK(CURDATE(), 1) AND DATE(${col}) < CURDATE()`;
    case 'month':
      return `YEAR(${col}) = YEAR(CURDATE()) AND MONTH(${col}) = MONTH(CURDATE()) AND DATE(${col}) < CURDATE()`;
    case 'quarter':
      return `YEAR(${col}) = YEAR(CURDATE()) AND QUARTER(${col}) = QUARTER(CURDATE()) AND DATE(${col}) < CURDATE()`;
    case 'year':
      return `YEAR(${col}) = YEAR(CURDATE()) AND DATE(${col}) < CURDATE()`;
    case 'uptodate':
      return `DATE(${col}) < CURDATE()`;
    case 'rolling7':
      return `DATE(${col}) BETWEEN DATE_SUB(CURDATE(), INTERVAL 6 DAY) AND DATE_SUB(CURDATE(), INTERVAL 1 DAY)`;
    case 'rolling30':
      return `DATE(${col}) BETWEEN DATE_SUB(CURDATE(), INTERVAL 29 DAY) AND DATE_SUB(CURDATE(), INTERVAL 1 DAY)`;
    case 'rolling90':
      return `DATE(${col}) BETWEEN DATE_SUB(CURDATE(), INTERVAL 89 DAY) AND DATE_SUB(CURDATE(), INTERVAL 1 DAY)`;
    case 'rolling12m':
      return `DATE(${col}) BETWEEN DATE_SUB(CURDATE(), INTERVAL 12 MONTH) AND DATE_SUB(CURDATE(), INTERVAL 1 DAY)`;
    default:
      return `YEAR(${col}) = YEAR(CURDATE()) AND MONTH(${col}) = MONTH(CURDATE()) AND DATE(${col}) < CURDATE()`;
  }
}

// ─── Branch filter helpers ────────────────────────────────────────────────────
// RULE: Office ALWAYS resolved via m_client.office_id — ml.office_id does NOT exist.
// bf()     → appends `AND mc.office_id = X` (use where mc is already joined)
// bfTxn()  → subquery for transaction tables that only have loan_id
// Supports single id or comma-separated ids for multi-branch selection
function parseBranchIds(id) {
  if (!id) return null;
  const ids = String(id).split(',').map(s => parseInt(s.trim())).filter(Number.isFinite);
  return ids.length ? ids : null;
}
const bf = id => {
  const ids = parseBranchIds(id);
  if (!ids) return '';
  return ids.length === 1 ? `AND mc.office_id = ${ids[0]}` : `AND mc.office_id IN (${ids.join(',')})`;
};
const bfTxn = id => {
  const ids = parseBranchIds(id);
  if (!ids) return '';
  const inClause = ids.length === 1 ? `= ${ids[0]}` : `IN (${ids.join(',')})`;
  return `AND mlt.loan_id IN (
  SELECT ml2.id FROM m_loan ml2 JOIN m_client mc2 ON mc2.id = ml2.client_id
  WHERE mc2.office_id ${inClause})`;
};

// Multi-branch aware inline helpers
const bfOffice = id => { const ids = parseBranchIds(id); if (!ids) return ''; return ids.length === 1 ? `AND mo.id = ${ids[0]}` : `AND mo.id IN (${ids.join(',')})`; };
const bfStaff  = id => { const ids = parseBranchIds(id); if (!ids) return ''; return ids.length === 1 ? `AND ms.office_id = ${ids[0]}` : `AND ms.office_id IN (${ids.join(',')})`; };
const bfJoinClient = id => {
  const ids = parseBranchIds(id);
  if (!ids) return '';
  const cond = ids.length === 1 ? `= ${ids[0]}` : `IN (${ids.join(',')})`;
  return `JOIN m_client mc ON mc.id = ml.client_id AND mc.office_id ${cond}`;
};

// Product filter helper: AND ml.product_id = X
const pf = id => { const n = parseInt(id); return Number.isFinite(n) ? `AND ml.product_id = ${n}` : ''; };

// ─── /api/nova/kpis ───────────────────────────────────────────────────────────
// PAR → Method A: laa.principal_overdue_derived (HelaPlus-validated)
// Collections → created_date (posting date)
app.get('/api/nova/kpis', async (req, res) => {
  try {
    const p  = req.query.period || 'month';
    const bId = req.query.branch && req.query.branch !== 'all' ? req.query.branch : null;
    const startDate = safeDate(req.query.startDate);
    const endDate   = safeDate(req.query.endDate);
    const prevRange = startDate && endDate ? shiftRange(startDate, endDate) : null;

    const disbW = rangeWhere('ml.disbursedon_date', p, startDate, endDate);
    const disbP = prevRange ? rangeWhere('ml.disbursedon_date', p, prevRange.prevStart, prevRange.prevEnd)
                            : prevWhere('ml.disbursedon_date', p);
    const colW  = rangeWhere('mlt.created_date', p, startDate, endDate);
    const colP  = prevRange ? rangeWhere('mlt.created_date', p, prevRange.prevStart, prevRange.prevEnd)
                            : prevWhere('mlt.created_date', p);
    const PF    = pf(req.query.product);
    const BF    = bf(bId) + ' ' + PF;
    const BFTXN = bfTxn(bId);

    // expectedWhere: same-window as collections, capped at today
    const expW  = expectedWhere('rs.duedate', p, startDate, endDate);
    // Prior period: same window shifted back one period
    const expP  = prevRange
      ? expectedWhere('rs.duedate', p, prevRange.prevStart, prevRange.prevEnd)
      : (() => {
          switch(p) {
            case 'today':    return `DATE(rs.duedate) = DATE_SUB(CURDATE(), INTERVAL 1 DAY)`;
            case 'week':     return `YEARWEEK(rs.duedate,1) = YEARWEEK(DATE_SUB(CURDATE(),INTERVAL 7 DAY),1) AND DATE(rs.duedate) <= DATE_SUB(CURDATE(), INTERVAL 7 DAY)`;
            case 'month':    return `YEAR(rs.duedate)=YEAR(DATE_SUB(CURDATE(),INTERVAL 1 MONTH)) AND MONTH(rs.duedate)=MONTH(DATE_SUB(CURDATE(),INTERVAL 1 MONTH)) AND DATE(rs.duedate) <= LAST_DAY(DATE_SUB(CURDATE(),INTERVAL 1 MONTH))`;
            case 'quarter':  return `YEAR(rs.duedate)=YEAR(DATE_SUB(CURDATE(),INTERVAL 3 MONTH)) AND QUARTER(rs.duedate)=QUARTER(DATE_SUB(CURDATE(),INTERVAL 3 MONTH)) AND DATE(rs.duedate) <= LAST_DAY(DATE_SUB(CURDATE(),INTERVAL 3 MONTH))`;
            case 'year':     return `YEAR(rs.duedate)=YEAR(DATE_SUB(CURDATE(),INTERVAL 1 YEAR)) AND DATE(rs.duedate) <= CONCAT(YEAR(CURDATE())-1,'-12-31')`;
            default:         return `YEAR(rs.duedate)=YEAR(DATE_SUB(CURDATE(),INTERVAL 1 MONTH)) AND MONTH(rs.duedate)=MONTH(DATE_SUB(CURDATE(),INTERVAL 1 MONTH)) AND DATE(rs.duedate)<=LAST_DAY(DATE_SUB(CURDATE(),INTERVAL 1 MONTH))`;
          }
        })();
    // overdueWhere: missed installments — duedate in period AND < today
    const overdueW = overdueWhere('rs.duedate', p, startDate, endDate);

    const [disbCur, disbPrv, colCur, colPrv, expCur, expPrv, active, par, par90, avg, allTime, balance, overdueRows] = await Promise.all([

      // Disbursements – this period
      runSQL(`SELECT COUNT(*) AS cnt, COALESCE(SUM(ml.principal_disbursed_derived),0) AS total
              FROM m_loan ml JOIN m_client mc ON mc.id = ml.client_id
              WHERE ${disbW} AND ml.loan_status_id IN (300,500,602) ${BF}`),

      // Disbursements – prior period
      runSQL(`SELECT COALESCE(SUM(ml.principal_disbursed_derived),0) AS total
              FROM m_loan ml JOIN m_client mc ON mc.id = ml.client_id
              WHERE ${disbP} AND ml.loan_status_id IN (300,500,602) ${BF}`),

      // Collections – this period (created_date = posting date, matches HelaPlus)
      runSQL(`SELECT COUNT(*) AS cnt, COALESCE(SUM(mlt.amount),0) AS total
              FROM m_loan_transaction mlt
              WHERE mlt.transaction_type_enum = 2 AND mlt.is_reversed = 0
                AND ${colW} ${BFTXN}`),

      // Collections – prior period
      runSQL(`SELECT COALESCE(SUM(mlt.amount),0) AS total
              FROM m_loan_transaction mlt
              WHERE mlt.transaction_type_enum = 2 AND mlt.is_reversed = 0
                AND ${colP} ${BFTXN}`),

      // Expected collections – duedate <= end of period (HelaPlus cumulative definition)
      // status IN(300,500,602): active + in-arrears loans only (not closed/written-off)
      runSQL(`SELECT COUNT(DISTINCT rs.loan_id) AS cnt,
                     COALESCE(SUM(
                       rs.principal_amount
                       + COALESCE(rs.interest_amount, 0)
                       + COALESCE(rs.fee_charges_amount, 0)), 0) AS total
              FROM m_loan_repayment_schedule rs
              JOIN m_loan ml ON ml.id = rs.loan_id
              JOIN m_client mc ON mc.id = ml.client_id
              WHERE ml.loan_status_id IN (300,500,602)
                AND rs.completed_derived = 0
                AND ${expW} ${BF}`),

      // Expected collections – prior period (for % change chip)
      runSQL(`SELECT COALESCE(SUM(
                       rs.principal_amount
                       + COALESCE(rs.interest_amount, 0)
                       + COALESCE(rs.fee_charges_amount, 0)), 0) AS total
              FROM m_loan_repayment_schedule rs
              JOIN m_loan ml ON ml.id = rs.loan_id
              JOIN m_client mc ON mc.id = ml.client_id
              WHERE ml.loan_status_id IN (300,500,602)
                AND rs.completed_derived = 0
                AND ${expP} ${BF}`),

      // Active loans snapshot
      runSQL(`SELECT COUNT(*) AS cnt, COUNT(DISTINCT ml.client_id) AS clients,
                     COALESCE(SUM(ml.principal_outstanding_derived),0) AS outstanding
              FROM m_loan ml JOIN m_client mc ON mc.id = ml.client_id
              WHERE ml.loan_status_id = 300 ${BF}`),

      // PAR30 – Method A: laa.principal_overdue_derived (never use overdue_since_date_derived for PAR)
      runSQL(`SELECT COALESCE(SUM(laa.principal_overdue_derived),0) AS overdue,
                     COALESCE(SUM(ml.principal_outstanding_derived),0) AS outstanding
              FROM m_loan ml
              JOIN m_client mc ON mc.id = ml.client_id
              LEFT JOIN m_loan_arrears_aging laa ON laa.loan_id = ml.id
              WHERE ml.loan_status_id = 300 ${BF}`),

      // PAR90 – UMRA regulatory provisioning trigger: loans overdue > 90 days
      runSQL(`SELECT COALESCE(SUM(laa.principal_overdue_derived),0) AS overdue,
                     COALESCE(SUM(ml.principal_outstanding_derived),0) AS outstanding
              FROM m_loan ml
              JOIN m_client mc ON mc.id = ml.client_id
              LEFT JOIN m_loan_arrears_aging laa ON laa.loan_id = ml.id
              WHERE ml.loan_status_id = 300
                AND DATEDIFF(CURDATE(), laa.overdue_since_date_derived) > 90 ${BF}`),

      // Avg loan size – this period
      runSQL(`SELECT COALESCE(AVG(ml.principal_disbursed_derived),0) AS avg_loan
              FROM m_loan ml JOIN m_client mc ON mc.id = ml.client_id
              WHERE ${disbW} AND ml.loan_status_id IN (300,500,602) ${BF}`),

      // All-time book (total portfolio)
      runSQL(`SELECT COUNT(*) AS cnt, COALESCE(SUM(ml.principal_disbursed_derived),0) AS total
              FROM m_loan ml JOIN m_client mc ON mc.id = ml.client_id
              WHERE ml.loan_status_id IN (300,500,600,602,700) ${BF}`),

      // Live balance (portfolio outstanding)
      // HELAPLUS ALIGNMENT: IN(300,500,602) matches HelaPlus Balance Outstanding report.
      // 300=Active, 500=Closed(obligations met), 602=Active-In-Arrears.
      // Using =300 only EXCLUDES loans in arrears status (602), causing undercount vs HelaPlus.
      runSQL(`SELECT COUNT(*) AS cnt,
                     COALESCE(SUM(ml.principal_outstanding_derived),0)   AS total,
                     COALESCE(SUM(ml.total_outstanding_derived),0)        AS total_with_interest
              FROM m_loan ml JOIN m_client mc ON mc.id = ml.client_id
              WHERE ml.loan_status_id IN (300,500,602) ${BF}`),

      // Overdue Amount: unpaid installments whose duedate falls within the period window
      // Uses schedule-based Method B — actual missed installment principal per period
      runSQL(`SELECT COUNT(DISTINCT rs.loan_id) AS loan_count,
                     COALESCE(SUM(GREATEST(
                       rs.principal_amount
                       - COALESCE(rs.principal_completed_derived,0)
                       - COALESCE(rs.principal_writtenoff_derived,0), 0)), 0) AS overdue_principal,
                     COALESCE(SUM(GREATEST(
                       rs.interest_amount
                       - COALESCE(rs.interest_completed_derived,0)
                       - COALESCE(rs.interest_writtenoff_derived,0), 0)), 0) AS overdue_interest
              FROM m_loan_repayment_schedule rs
              JOIN m_loan ml ON ml.id = rs.loan_id
              JOIN m_client mc ON mc.id = ml.client_id
              WHERE ml.loan_status_id IN (300,602)
                AND rs.completed_derived = 0
                AND ${overdueW} ${BF}`)
    ]);

    const disbTotal  = num(disbCur, 'total');
    const disbPrev   = num(disbPrv, 'total');
    const colTotal   = num(colCur, 'total');
    const colPrev    = num(colPrv, 'total');
    const expectedTotal = num(expCur, 'total');
    const expectedPrev  = num(expPrv, 'total');
    const parOverdue = num(par, 'overdue');
    const parPort    = num(par, 'outstanding', 1);
    const parRate    = parPort > 0 ? Math.round(parOverdue / parPort * 10000) / 100 : 0;

    res.json({ success: true, period: p, data: {
      totalPortfolio:       { total: num(allTime, 'total'), loanCount: int(allTime, 'cnt') },
      // portfolioOutstanding: principal only (matches HelaPlus Balance Outstanding principal column)
      portfolioOutstanding: { total: num(balance, 'total'), loanCount: int(balance, 'cnt'),
                              totalWithInterest: num(balance, 'total_with_interest') },
      disbursements:        { total: disbTotal, loanCount: int(disbCur, 'cnt'), change: pct(disbTotal, disbPrev) },
      collections:          { total: colTotal,  txnCount: int(colCur, 'cnt'),   change: pct(colTotal, colPrev) },
      expectedCollections:  { total: expectedTotal, loanCount: int(expCur, 'cnt'), change: pct(expectedTotal, expectedPrev) },
      activeLoans:          { count: int(active,'cnt'), clientCount: int(active,'clients'), totalOutstanding: num(active,'outstanding') },
      par30: { rate: parRate, amount: parOverdue,
               status: parRate < 3 ? 'excellent' : parRate < 5 ? 'good' : parRate < 10 ? 'warning' : 'danger' },
      par90: (() => {
        const p90Overdue = num(par90, 'overdue');
        const p90Port    = num(par90, 'outstanding', 1);
        const p90Rate    = p90Port > 0 ? Math.round(p90Overdue / p90Port * 10000) / 100 : 0;
        return { rate: p90Rate, amount: p90Overdue,
                 status: p90Rate < 1 ? 'excellent' : p90Rate < 3 ? 'good' : p90Rate < 5 ? 'warning' : 'danger' };
      })(),
      avgLoanSize:    { amount: Math.round(num(avg, 'avg_loan')) },
      overdueAmount:  {
        principal: num(overdueRows, 'overdue_principal'),
        interest:  num(overdueRows, 'overdue_interest'),
        total:     num(overdueRows, 'overdue_principal') + num(overdueRows, 'overdue_interest'),
        loanCount: int(overdueRows, 'loan_count')
      },
      collectionRate: expectedTotal > 0 ? Math.round(colTotal / expectedTotal * 1000) / 10 : 0
    }});
  } catch (err) {
    console.error('[kpis]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ─── /api/nova/timeseries ─────────────────────────────────────────────────────
// FIXED: branch filter via m_client.office_id (never ml.office_id)
app.get('/api/nova/timeseries', async (req, res) => {
  try {
    const p   = req.query.period || 'month';
    const bId = req.query.branch && req.query.branch !== 'all' ? req.query.branch : null;
    const startDate = safeDate(req.query.startDate);
    const endDate   = safeDate(req.query.endDate);
    const PF    = pf(req.query.product);
    const BF    = bf(bId) + ' ' + PF;
    const BFTXN = bfTxn(bId);
    const disbW = rangeWhere('ml.disbursedon_date', p, startDate, endDate);
    const colW  = rangeWhere('mlt.created_date', p, startDate, endDate);

    let disbExpr, colExpr, disbGrp, colGrp;
    if (startDate || endDate) {
      disbExpr = disbGrp = `DATE(ml.disbursedon_date)`;
      colExpr  = colGrp  = `DATE(mlt.created_date)`;
    } else if (p === 'today') {
      disbExpr = `DATE_FORMAT(ml.disbursedon_date, '%H:00')`; disbGrp = `HOUR(ml.disbursedon_date)`;
      colExpr  = `DATE_FORMAT(mlt.created_date, '%H:00')`;    colGrp  = `HOUR(mlt.created_date)`;
    } else if (p === 'year' || p === 'uptodate' || p === 'rolling12m') {
      disbExpr = disbGrp = `DATE_FORMAT(ml.disbursedon_date, '%Y-%m')`;
      colExpr  = colGrp  = `DATE_FORMAT(mlt.created_date, '%Y-%m')`;
    } else {
      // week, month, quarter, rolling7, rolling30, rolling90 — group by day
      disbExpr = disbGrp = `DATE(ml.disbursedon_date)`;
      colExpr  = colGrp  = `DATE(mlt.created_date)`;
    }

    const [disbSeries, colSeries] = await Promise.all([
      runSQL(`SELECT ${disbExpr} AS lbl, COUNT(*) AS cnt,
                     COALESCE(SUM(ml.principal_disbursed_derived),0) AS amt
              FROM m_loan ml JOIN m_client mc ON mc.id = ml.client_id
              WHERE ${disbW} AND ml.loan_status_id IN (300,500,602) ${BF}
              GROUP BY ${disbGrp} ORDER BY ${disbGrp}`),

      runSQL(`SELECT ${colExpr} AS lbl, COUNT(*) AS cnt,
                     COALESCE(SUM(mlt.amount),0) AS amt
              FROM m_loan_transaction mlt
              WHERE mlt.transaction_type_enum = 2 AND mlt.is_reversed = 0
                AND ${colW} ${BFTXN}
              GROUP BY ${colGrp} ORDER BY ${colGrp}`)
    ]);

    res.json({ success: true, period: p, data: {
      disbursements: disbSeries.map(r => ({ label: r.lbl, amount: parseFloat(r.amt)||0, count: parseInt(r.cnt)||0 })),
      collections:   colSeries.map(r =>  ({ label: r.lbl, amount: parseFloat(r.amt)||0, count: parseInt(r.cnt)||0 }))
    }});
  } catch (err) {
    console.error('[timeseries]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ─── /api/nova/branch-list ────────────────────────────────────────────────────
app.get('/api/nova/branch-list', async (_req, res) => {
  try {
    const rows = await runSQL(`SELECT id, name FROM m_office WHERE parent_id IS NOT NULL ORDER BY name ASC`);
    res.json({ success: true, data: rows.map(r => ({ id: r.id, name: r.name })) });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ─── /api/nova/product-list ────────────────────────────────────────────────────
app.get('/api/nova/product-list', async (_req, res) => {
  try {
    const rows = await runSQL(`SELECT id, name FROM m_product_loan ORDER BY name ASC`);
    res.json({ success: true, data: rows.map(r => ({ id: r.id, name: r.name })) });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ─── /api/nova/product-loans ──────────────────────────────────────────────────
// Drill-down: loans under a specific product
app.get('/api/nova/product-loans', async (req, res) => {
  try {
    const productId = parseInt(req.query.productId);
    if (!productId) return res.status(400).json({ success: false, error: 'productId required' });
    const bId = req.query.branch && req.query.branch !== 'all' ? req.query.branch : null;
    const BF  = bf(bId);

    const rows = await runSQL(`
      SELECT
        ml.id AS loan_id,
        ml.account_no AS loan_account,
        mc.display_name AS client_name,
        mc.mobile_no AS client_phone,
        mo.name AS branch_name,
        ms.display_name AS officer_name,
        COALESCE(ml.principal_disbursed_derived,0) AS disbursed,
        COALESCE(ml.principal_outstanding_derived,0) AS outstanding,
        COALESCE(DATEDIFF(CURDATE(), MIN(rs.duedate)), 0) AS days_in_arrears
      FROM m_loan ml
      JOIN m_client mc ON mc.id = ml.client_id
      LEFT JOIN m_office mo ON mo.id = mc.office_id
      LEFT JOIN m_staff ms ON ms.id = ml.loan_officer_id
      LEFT JOIN m_loan_repayment_schedule rs ON rs.loan_id = ml.id
        AND rs.completed_derived = 0
        AND rs.duedate < CURDATE()
      WHERE ml.product_id = ${productId}
        AND ml.loan_status_id = 300
        ${BF}
      GROUP BY ml.id, ml.account_no, mc.display_name, mc.mobile_no, mo.name, ms.display_name,
               ml.principal_disbursed_derived, ml.principal_outstanding_derived
      ORDER BY days_in_arrears DESC, outstanding DESC
      LIMIT 200
    `);

    res.json({ success: true, data: rows.map(r => ({
      loanId:        r.loan_id,
      loanAccount:   r.loan_account,
      clientName:    r.client_name,
      clientPhone:   r.client_phone,
      branchName:    r.branch_name,
      officerName:   r.officer_name,
      disbursed:     parseFloat(r.disbursed)||0,
      outstanding:   parseFloat(r.outstanding)||0,
      daysInArrears: parseInt(r.days_in_arrears)||0
    }))});
  } catch (err) {
    console.error('[product-loans]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ─── /api/nova/branches ───────────────────────────────────────────────────────
// FIXED: Split into 2 queries to avoid transaction cartesian product.
//        Office path: m_office → m_client → m_loan (correct join).
app.get('/api/nova/branches', async (req, res) => {
  try {
    const p   = req.query.period || 'month';
    const bId = req.query.branch && req.query.branch !== 'all' ? req.query.branch : null;
    const startDate = safeDate(req.query.startDate);
    const endDate   = safeDate(req.query.endDate);
    const disbW  = rangeWhere('ml.disbursedon_date', p, startDate, endDate);
    const colW   = rangeWhere('mlt.created_date', p, startDate, endDate);
    const bWhere = bfOffice(bId);
    const PF     = pf(req.query.product);

    const [loanRows, colRows] = await Promise.all([
      // Loan metrics — no transaction join here (avoids cartesian)
      runSQL(`
        SELECT
          mo.id   AS branch_id,
          mo.name AS branch_name,
          COUNT(DISTINCT CASE WHEN ${disbW} AND ml.loan_status_id IN (300,500,602) THEN ml.id END) AS loans_disbursed,
          COALESCE(SUM(CASE WHEN ${disbW} AND ml.loan_status_id IN (300,500,602)
                       THEN ml.principal_disbursed_derived ELSE 0 END), 0) AS disbursed_amount,
          COUNT(DISTINCT CASE WHEN ml.loan_status_id = 300 THEN ml.id END) AS active_loans,
          COALESCE(SUM(CASE WHEN ml.loan_status_id = 300
                       THEN ml.principal_outstanding_derived ELSE 0 END), 0) AS total_outstanding,
          COALESCE(SUM(CASE WHEN ml.loan_status_id = 300
                       THEN laa.principal_overdue_derived ELSE 0 END), 0) AS par_overdue
        FROM m_office mo
        JOIN m_client mc ON mc.office_id = mo.id
        LEFT JOIN m_loan ml ON ml.client_id = mc.id
        LEFT JOIN m_loan_arrears_aging laa ON laa.loan_id = ml.id
        WHERE mo.parent_id IS NOT NULL ${bWhere} ${PF}
        GROUP BY mo.id, mo.name
        ORDER BY disbursed_amount DESC
      `),

      // Collections — separate query, grouped by office via m_client
      runSQL(`
        SELECT mc.office_id AS branch_id, COALESCE(SUM(mlt.amount),0) AS collected_amount
        FROM m_loan_transaction mlt
        JOIN m_loan ml  ON ml.id  = mlt.loan_id
        JOIN m_client mc ON mc.id = ml.client_id
        WHERE mlt.transaction_type_enum = 2 AND mlt.is_reversed = 0 AND ${colW}
        ${bf(bId)} ${PF}
        GROUP BY mc.office_id
      `)
    ]);

    const colMap = Object.fromEntries(colRows.map(r => [r.branch_id, parseFloat(r.collected_amount)||0]));

    res.json({ success: true, period: p, data: loanRows.map(r => {
      const outstanding = parseFloat(r.total_outstanding)||0;
      const overdue     = parseFloat(r.par_overdue)||0;
      return {
        branchId:         r.branch_id,
        branchName:       r.branch_name,
        loansDisbursed:   parseInt(r.loans_disbursed)||0,
        disbursedAmount:  parseFloat(r.disbursed_amount)||0,
        collectedAmount:  colMap[r.branch_id] || 0,
        activeLoans:      parseInt(r.active_loans)||0,
        totalOutstanding: outstanding,
        par30Rate:        outstanding > 0 ? Math.round(overdue / outstanding * 10000) / 100 : 0
      };
    })});
  } catch (err) {
    console.error('[branches]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ─── /api/nova/officers ───────────────────────────────────────────────────────
// FIXED: Full metrics (activeLoans, collectedAmount, par30Rate) via 2 queries.
//        PAR → laa.principal_overdue_derived (Method A).
//        No cartesian: single m_loan join with conditional aggregation.
app.get('/api/nova/officers', async (req, res) => {
  try {
    const p   = req.query.period || 'month';
    const bId = req.query.branch && req.query.branch !== 'all' ? req.query.branch : null;
    const startDate = safeDate(req.query.startDate);
    const endDate   = safeDate(req.query.endDate);
    const disbW  = rangeWhere('ml.disbursedon_date', p, startDate, endDate);
    const colW   = rangeWhere('mlt.created_date', p, startDate, endDate);
    const bWhere = bfStaff(bId);
    const PF     = pf(req.query.product);
    // overdueW: missed installments — duedate in period AND < today
    const overdueW = overdueWhere('rs.duedate', p, startDate, endDate);

    const [officerRows, colRows, expRows, ovdRows] = await Promise.all([
      // Loan + PAR stats — single m_loan join, conditional aggregation
      runSQL(`
        SELECT
          ms.id            AS officer_id,
          ms.display_name  AS officer_name,
          mo.name          AS branch_name,
          COUNT(DISTINCT CASE WHEN ${disbW} AND ml.loan_status_id IN (300,500,602) THEN ml.id END) AS loans_disbursed,
          COALESCE(SUM(CASE WHEN ${disbW} AND ml.loan_status_id IN (300,500,602)
                       THEN ml.principal_disbursed_derived ELSE 0 END), 0) AS disbursed_amount,
          COUNT(DISTINCT CASE WHEN ml.loan_status_id = 300 THEN ml.id END) AS active_loans,
          COALESCE(SUM(CASE WHEN ml.loan_status_id = 300
                       THEN ml.principal_outstanding_derived ELSE 0 END), 0) AS total_outstanding,
          COALESCE(SUM(CASE WHEN ml.loan_status_id = 300
                       THEN laa.principal_overdue_derived ELSE 0 END), 0) AS par_overdue
        FROM m_staff ms
        JOIN m_office mo ON mo.id = ms.office_id
        LEFT JOIN m_loan ml ON ml.loan_officer_id = ms.id
        LEFT JOIN m_loan_arrears_aging laa ON laa.loan_id = ml.id AND ml.loan_status_id = 300
        WHERE ms.is_loan_officer = 1 AND ms.is_active = 1 ${bWhere} ${PF}
        GROUP BY ms.id, ms.display_name, mo.name
        HAVING loans_disbursed > 0 OR active_loans > 0
        ORDER BY disbursed_amount DESC
        LIMIT 50
      `),

      // Collections for this period, per officer
      runSQL(`
        SELECT ml.loan_officer_id AS officer_id, COALESCE(SUM(mlt.amount),0) AS collected_amount
        FROM m_loan_transaction mlt
        JOIN m_loan ml ON ml.id = mlt.loan_id
        ${bId ? bfJoinClient(bId) : ''}
        WHERE mlt.transaction_type_enum = 2 AND mlt.is_reversed = 0 AND ${colW}
          AND ml.loan_officer_id IS NOT NULL ${PF}
        GROUP BY ml.loan_officer_id
      `),

      // Expected collections per officer (same-window duedate, capped at today, unpaid only)
      runSQL(`
        SELECT ml.loan_officer_id AS officer_id,
               COALESCE(SUM(rs.principal_amount
                 + COALESCE(rs.interest_amount,0)
                 + COALESCE(rs.fee_charges_amount,0)),0) AS expected_amount
        FROM m_loan_repayment_schedule rs
        JOIN m_loan ml ON ml.id = rs.loan_id
        ${bId ? bfJoinClient(bId) : ''}
        WHERE ml.loan_status_id IN (300,500,602)
          AND rs.completed_derived = 0
          AND ${expectedWhere('rs.duedate', p, startDate, endDate)}
          AND ml.loan_officer_id IS NOT NULL ${PF}
        GROUP BY ml.loan_officer_id
      `),

      // Overdue per officer: unpaid installments in period window (duedate in period, < today)
      runSQL(`
        SELECT ml.loan_officer_id AS officer_id,
               COUNT(DISTINCT rs.loan_id) AS overdue_loan_count,
               COALESCE(SUM(GREATEST(
                 rs.principal_amount
                 - COALESCE(rs.principal_completed_derived,0)
                 - COALESCE(rs.principal_writtenoff_derived,0), 0)), 0) AS overdue_principal
        FROM m_loan_repayment_schedule rs
        JOIN m_loan ml ON ml.id = rs.loan_id
        ${bId ? bfJoinClient(bId) : ''}
        WHERE ml.loan_status_id IN (300,602)
          AND rs.completed_derived = 0
          AND ${overdueW}
          AND ml.loan_officer_id IS NOT NULL ${PF}
        GROUP BY ml.loan_officer_id
      `)
    ]);

    const colMap = Object.fromEntries(colRows.map(r => [r.officer_id, parseFloat(r.collected_amount)||0]));
    const expMap = Object.fromEntries(expRows.map(r => [r.officer_id, parseFloat(r.expected_amount)||0]));
    const ovdMap = Object.fromEntries(ovdRows.map(r => [r.officer_id, parseFloat(r.overdue_principal)||0]));

    res.json({ success: true, period: p, data: officerRows.map(r => {
      const outstanding = parseFloat(r.total_outstanding)||0;
      const overdue     = parseFloat(r.par_overdue)||0;
      const collected   = colMap[r.officer_id] || 0;
      const expected    = expMap[r.officer_id] || 0;
      return {
        officerId:        r.officer_id,
        officerName:      r.officer_name,
        branchName:       r.branch_name,
        loansDisbursed:   parseInt(r.loans_disbursed)||0,
        disbursedAmount:  parseFloat(r.disbursed_amount)||0,
        activeLoans:      parseInt(r.active_loans)||0,
        totalOutstanding: outstanding,
        collectedAmount:  collected,
        expectedAmount:   expected,
        overdueAmount:    ovdMap[r.officer_id] || 0,
        collectionRate:   expected > 0 ? Math.round(collected / expected * 1000) / 10 : 0,
        par30Rate:        outstanding > 0 ? Math.round(overdue / outstanding * 10000) / 100 : 0
      };
    })});
  } catch (err) {
    console.error('[officers]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// â”€â”€â”€ /api/nova/officer-loans â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// Drill-down detail for officer modal (active loans + arrears + contact)
app.get('/api/nova/officer-loans', async (req, res) => {
  try {
    const officerId = parseInt(req.query.officerId);
    if (!officerId) return res.status(400).json({ success: false, error: 'officerId required' });
    const bId = req.query.branch && req.query.branch !== 'all' ? req.query.branch : null;
    const BF  = bf(bId);

    const rows = await runSQL(`
      SELECT
        ml.id AS loan_id,
        ml.account_no AS loan_account,
        mc.display_name AS client_name,
        mc.mobile_no AS client_phone,
        mo.name AS branch_name,
        mpl.name AS product_name,
        COALESCE(ml.principal_outstanding_derived,0) AS outstanding,
        COALESCE(SUM(GREATEST(
          rs.principal_amount
          - COALESCE(rs.principal_completed_derived, 0)
          - COALESCE(rs.principal_writtenoff_derived, 0), 0)), 0) AS principal_overdue,
        COALESCE(DATEDIFF(CURDATE(), MIN(rs.duedate)), 0) AS days_in_arrears
      FROM m_loan ml
      JOIN m_client mc ON mc.id = ml.client_id
      LEFT JOIN m_office mo ON mo.id = mc.office_id
      LEFT JOIN m_product_loan mpl ON mpl.id = ml.product_id
      LEFT JOIN m_loan_repayment_schedule rs ON rs.loan_id = ml.id
        AND rs.completed_derived = 0
        AND rs.duedate < CURDATE()
      WHERE ml.loan_officer_id = ${officerId}
        AND ml.loan_status_id = 300
        ${BF}
      GROUP BY ml.id, ml.account_no, mc.display_name, mc.mobile_no, mo.name, mpl.name, ml.principal_outstanding_derived
      ORDER BY days_in_arrears DESC, principal_overdue DESC
      LIMIT 200
    `);

    res.json({ success: true, data: rows.map(r => ({
      loanId:           r.loan_id,
      loanAccount:      r.loan_account,
      clientName:       r.client_name,
      clientPhone:      r.client_phone,
      branchName:       r.branch_name,
      productName:      r.product_name,
      outstanding:      parseFloat(r.outstanding)||0,
      principalOverdue: parseFloat(r.principal_overdue)||0,
      daysInArrears:    parseInt(r.days_in_arrears)||0
    }))});
  } catch (err) {
    console.error('[officer-loans]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// â”€â”€â”€ /api/nova/collections-due-today â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// Ops list: clients due today + officer + phone
app.get('/api/nova/collections-due-today', async (req, res) => {
  try {
    const bId = req.query.branch && req.query.branch !== 'all' ? req.query.branch : null;
    const BF  = bf(bId);

    const rows = await runSQL(`
      SELECT
        ml.id AS loan_id,
        ml.account_no AS loan_account,
        mc.display_name AS client_name,
        mc.mobile_no AS client_phone,
        ms.display_name AS officer_name,
        mo.name AS branch_name,
        COALESCE(SUM(GREATEST(
          (rs.principal_amount
            + COALESCE(rs.interest_amount, 0)
            + COALESCE(rs.fee_charges_amount, 0))
          - (COALESCE(rs.principal_completed_derived, 0)
            + COALESCE(rs.interest_completed_derived, 0)
            + COALESCE(rs.fee_charges_completed_derived, 0)), 0)), 0) AS amount_due
      FROM m_loan_repayment_schedule rs
      JOIN m_loan ml ON ml.id = rs.loan_id
      JOIN m_client mc ON mc.id = ml.client_id
      LEFT JOIN m_staff ms ON ms.id = ml.loan_officer_id
      LEFT JOIN m_office mo ON mo.id = mc.office_id
      WHERE rs.duedate = CURDATE()
        AND rs.completed_derived = 0
        AND ml.loan_status_id = 300
        ${BF}
      GROUP BY ml.id, ml.account_no, mc.display_name, mc.mobile_no, ms.display_name, mo.name
      ORDER BY amount_due DESC
    `);

    res.json({ success: true, data: rows.map(r => ({
      loanId:      r.loan_id,
      loanAccount: r.loan_account,
      clientName:  r.client_name,
      clientPhone: r.client_phone,
      officerName: r.officer_name,
      branchName:  r.branch_name,
      amountDue:   parseFloat(r.amount_due)||0
    }))});
  } catch (err) {
    console.error('[collections-due-today]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ─── /api/nova/products ───────────────────────────────────────────────────────
// FIXED: branch via m_client (was using ml.office_id)
app.get('/api/nova/products', async (req, res) => {
  try {
    const p   = req.query.period || 'month';
    const bId = req.query.branch && req.query.branch !== 'all' ? req.query.branch : null;
    const startDate = safeDate(req.query.startDate);
    const endDate   = safeDate(req.query.endDate);
    const disbW = rangeWhere('ml.disbursedon_date', p, startDate, endDate);
    const BF    = bf(bId);

    const rows = await runSQL(`
      SELECT mpl.id   AS product_id,
             mpl.name AS product_name,
             COUNT(*)  AS loan_count,
             COALESCE(SUM(ml.principal_disbursed_derived), 0) AS total_amount,
             COUNT(CASE WHEN ml.loan_status_id = 300 THEN 1 END) AS active_count,
             COALESCE(SUM(CASE WHEN ml.loan_status_id = 300 THEN ml.principal_outstanding_derived ELSE 0 END), 0) AS outstanding_amount,
             COALESCE(SUM(CASE WHEN ml.loan_status_id = 300 THEN laa.principal_overdue_derived ELSE 0 END), 0) AS par_overdue_amount
      FROM m_loan ml
      JOIN m_client mc ON mc.id = ml.client_id
      JOIN m_product_loan mpl ON mpl.id = ml.product_id
      LEFT JOIN m_loan_arrears_aging laa ON laa.loan_id = ml.id
      WHERE ${disbW} AND ml.loan_status_id IN (300,500,602) ${BF}
      GROUP BY mpl.id, mpl.name
      ORDER BY total_amount DESC
    `);

    res.json({ success: true, period: p, data: rows.map(r => {
      const outstanding = parseFloat(r.outstanding_amount)||0;
      const parOverdue  = parseFloat(r.par_overdue_amount)||0;
      return {
        productId:         r.product_id,
        productName:       r.product_name,
        loanCount:         parseInt(r.loan_count)||0,
        totalAmount:       parseFloat(r.total_amount)||0,
        activeCount:       parseInt(r.active_count)||0,
        outstandingAmount: outstanding,
        parOverdueAmount:  parOverdue,
        parRate:           outstanding > 0 ? Math.round(parOverdue / outstanding * 10000) / 100 : 0
      };
    })});
  } catch (err) {
    console.error('[products]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ─── /api/nova/arrears ────────────────────────────────────────────────────────
// FIXED: branch via m_client. Uses Method B (schedule-based) for aging bands
//        so overdue principal = actual unpaid installment principal (not full loan balance).
app.get('/api/nova/arrears', async (req, res) => {
  try {
    const bId = req.query.branch && req.query.branch !== 'all' ? req.query.branch : null;
    const BF  = bf(bId);

    const rows = await runSQL(`
      SELECT
        SUM(CASE WHEN days_in_arrears BETWEEN 1  AND 7   THEN 1 ELSE 0 END) AS count_1_7,
        SUM(CASE WHEN days_in_arrears BETWEEN 8  AND 14  THEN 1 ELSE 0 END) AS count_8_14,
        SUM(CASE WHEN days_in_arrears BETWEEN 15 AND 30  THEN 1 ELSE 0 END) AS count_15_30,
        SUM(CASE WHEN days_in_arrears BETWEEN 31 AND 60  THEN 1 ELSE 0 END) AS count_31_60,
        SUM(CASE WHEN days_in_arrears BETWEEN 61 AND 90  THEN 1 ELSE 0 END) AS count_61_90,
        SUM(CASE WHEN days_in_arrears > 90               THEN 1 ELSE 0 END) AS count_90plus,
        COALESCE(SUM(CASE WHEN days_in_arrears BETWEEN 1  AND 7   THEN principal_overdue ELSE 0 END),0) AS amt_1_7,
        COALESCE(SUM(CASE WHEN days_in_arrears BETWEEN 8  AND 14  THEN principal_overdue ELSE 0 END),0) AS amt_8_14,
        COALESCE(SUM(CASE WHEN days_in_arrears BETWEEN 15 AND 30  THEN principal_overdue ELSE 0 END),0) AS amt_15_30,
        COALESCE(SUM(CASE WHEN days_in_arrears BETWEEN 31 AND 60  THEN principal_overdue ELSE 0 END),0) AS amt_31_60,
        COALESCE(SUM(CASE WHEN days_in_arrears BETWEEN 61 AND 90  THEN principal_overdue ELSE 0 END),0) AS amt_61_90,
        COALESCE(SUM(CASE WHEN days_in_arrears > 90               THEN principal_overdue ELSE 0 END),0) AS amt_90plus
      FROM (
        -- Method B: schedule-based — gives actual overdue installment principal per loan
        SELECT ml.id,
          SUM(GREATEST(
            rs.principal_amount
            - COALESCE(rs.principal_completed_derived, 0)
            - COALESCE(rs.principal_writtenoff_derived, 0), 0)) AS principal_overdue,
          DATEDIFF(CURDATE(), MIN(rs.duedate)) AS days_in_arrears
        FROM m_loan ml
        JOIN m_client mc ON mc.id = ml.client_id
        JOIN m_loan_repayment_schedule rs ON rs.loan_id = ml.id
        WHERE ml.loan_status_id = 300
          AND rs.duedate < CURDATE()
          AND rs.completed_derived = 0 ${BF}
        GROUP BY ml.id
      ) sub
      WHERE days_in_arrears > 0
    `);

    res.json({ success: true, data: { bands: [
      { label: '1–7 Days',   count: int(rows,'count_1_7'),    amount: num(rows,'amt_1_7'),   severity: 'early'    },
      { label: '8–14 Days',  count: int(rows,'count_8_14'),   amount: num(rows,'amt_8_14'),  severity: 'early'    },
      { label: '15–30 Days', count: int(rows,'count_15_30'),  amount: num(rows,'amt_15_30'), severity: 'moderate' },
      { label: '31–60 Days', count: int(rows,'count_31_60'),  amount: num(rows,'amt_31_60'), severity: 'high'     },
      { label: '61–90 Days', count: int(rows,'count_61_90'),  amount: num(rows,'amt_61_90'), severity: 'critical' },
      { label: '90+ Days',   count: int(rows,'count_90plus'), amount: num(rows,'amt_90plus'),severity: 'loss'     }
    ]}});
  } catch (err) {
    console.error('[arrears]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ─── /api/nova/collection-target ─────────────────────────────────────────────
// FIXED: branch filter via m_client for schedule; subquery for transactions.
app.get('/api/nova/collection-target', async (req, res) => {
  try {
    const p   = req.query.period || 'month';
    const bId = req.query.branch && req.query.branch !== 'all' ? req.query.branch : null;
    const startDate = safeDate(req.query.startDate);
    const endDate   = safeDate(req.query.endDate);
    const expW   = expectedWhere('rs.duedate', p, startDate, endDate);
    const colW   = rangeWhere('mlt.created_date', p, startDate, endDate);
    const BF     = bf(bId);
    const BFTXN  = bfTxn(bId);

    const [targetRows, colRows] = await Promise.all([
      // Scheduled amount due UP TO end of period (HelaPlus cumulative — excludes already paid)
      runSQL(`SELECT COALESCE(SUM(
                rs.principal_amount
                + COALESCE(rs.interest_amount, 0)
                + COALESCE(rs.fee_charges_amount, 0)), 0) AS total_due
              FROM m_loan_repayment_schedule rs
              JOIN m_loan ml ON ml.id = rs.loan_id
              JOIN m_client mc ON mc.id = ml.client_id
              WHERE ml.loan_status_id IN (300,500,602)
                AND rs.completed_derived = 0
                AND ${expW} ${BF}`),

      // Actual collected in period
      runSQL(`SELECT COALESCE(SUM(mlt.amount),0) AS total_collected
              FROM m_loan_transaction mlt
              WHERE mlt.transaction_type_enum = 2 AND mlt.is_reversed = 0
                AND ${colW} ${BFTXN}`)
    ]);

    const totalDue       = num(targetRows, 'total_due');
    const totalCollected = num(colRows, 'total_collected');
    const achievementPct = totalDue > 0 ? Math.round(totalCollected / totalDue * 1000) / 10 : 0;

    res.json({ success: true, period: p, data: {
      totalDue, totalCollected, achievementPct,
      gap: Math.max(totalDue - totalCollected, 0),
      status: achievementPct >= 90 ? 'excellent' : achievementPct >= 80 ? 'ontrack' : achievementPct >= 50 ? 'moderate' : 'low'
    }});
  } catch (err) {
    console.error('[collection-target]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// â”€â”€â”€ /api/nova/collection-efficiency â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// 12-month actual vs expected collections %
app.get('/api/nova/collection-efficiency', async (req, res) => {
  try {
    const bId = req.query.branch && req.query.branch !== 'all' ? req.query.branch : null;
    const BF     = bf(bId);
    const BFTXN  = bfTxn(bId);

    const [dueRows, colRows] = await Promise.all([
      runSQL(`
        SELECT DATE_FORMAT(rs.duedate, '%Y-%m') AS ym,
               COALESCE(SUM(rs.principal_amount + COALESCE(rs.interest_amount,0) + COALESCE(rs.fee_charges_amount,0)),0) AS total_due
        FROM m_loan_repayment_schedule rs
        JOIN m_loan ml ON ml.id = rs.loan_id
        JOIN m_client mc ON mc.id = ml.client_id
        WHERE rs.duedate >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 11 MONTH), '%Y-%m-01')
          AND rs.duedate <= LAST_DAY(CURDATE())
          AND ml.loan_status_id IN (300,500,602) ${BF}
        GROUP BY ym
        ORDER BY ym
      `),
      runSQL(`
        SELECT DATE_FORMAT(mlt.created_date, '%Y-%m') AS ym,
               COALESCE(SUM(mlt.amount),0) AS total_collected
        FROM m_loan_transaction mlt
        WHERE mlt.transaction_type_enum = 2 AND mlt.is_reversed = 0
          AND mlt.created_date >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 11 MONTH), '%Y-%m-01')
          AND mlt.created_date <= LAST_DAY(CURDATE())
          ${BFTXN}
        GROUP BY ym
        ORDER BY ym
      `)
    ]);

    const dueMap = Object.fromEntries(dueRows.map(r => [r.ym, parseFloat(r.total_due)||0]));
    const colMap = Object.fromEntries(colRows.map(r => [r.ym, parseFloat(r.total_collected)||0]));
    const months = [];
    const now = new Date();
    now.setUTCDate(1);
    for (let i = 11; i >= 0; i--) {
      const d = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() - i, 1));
      const ym = d.toISOString().slice(0, 7);
      months.push(ym);
    }

    res.json({ success: true, data: months.map(ym => {
      const totalDue = dueMap[ym] || 0;
      const totalCollected = colMap[ym] || 0;
      const efficiencyPct = totalDue > 0 ? Math.round(totalCollected / totalDue * 1000) / 10 : 0;
      return { month: ym, totalDue, totalCollected, efficiencyPct };
    })});
  } catch (err) {
    console.error('[collection-efficiency]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ─── /api/nova/retention ─────────────────────────────────────────────────────
// NEW: Repeat-borrower rate.  eligibleClients = all who ever had a loan.
//      retainedClients = those with 2+ loans (any status).
app.get('/api/nova/retention', async (req, res) => {
  try {
    const bId = req.query.branch && req.query.branch !== 'all' ? req.query.branch : null;
    const BF  = bf(bId);

    const rows = await runSQL(`
      SELECT
        COUNT(DISTINCT client_id)                                          AS total_clients,
        COUNT(DISTINCT CASE WHEN loan_count > 1 THEN client_id END)       AS retained_clients,
        COUNT(DISTINCT CASE WHEN loan_count = 1 THEN client_id END)       AS new_clients
      FROM (
        SELECT ml.client_id, COUNT(*) AS loan_count
        FROM m_loan ml JOIN m_client mc ON mc.id = ml.client_id
        WHERE ml.loan_status_id IN (300,500,600,602,700) ${BF}
        GROUP BY ml.client_id
      ) sub
    `);

    const total    = int(rows, 'total_clients');
    const retained = int(rows, 'retained_clients');
    res.json({ success: true, data: {
      retentionPct:    total > 0 ? Math.round(retained / total * 1000) / 10 : 0,
      retainedClients: retained,
      newClients:      int(rows, 'new_clients'),
      eligibleClients: total
    }});
  } catch (err) {
    console.error('[retention]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ─── /api/nova/sparklines ────────────────────────────────────────────────────
// NEW: Last 12 weekly data points for disbursement/collection/loan-count sparklines.
app.get('/api/nova/sparklines', async (req, res) => {
  try {
    const bId   = req.query.branch !== 'all' ? req.query.branch : null;
    const BF    = bf(bId);
    const BFTXN = bfTxn(bId);

    const [disbRows, colRows] = await Promise.all([
      runSQL(`SELECT YEARWEEK(ml.disbursedon_date, 1) AS wk,
                     COALESCE(SUM(ml.principal_disbursed_derived),0) AS amt,
                     COUNT(*) AS cnt
              FROM m_loan ml JOIN m_client mc ON mc.id = ml.client_id
              WHERE ml.loan_status_id IN (300,500,602)
                AND ml.disbursedon_date >= DATE_SUB(CURDATE(), INTERVAL 12 WEEK) ${BF}
              GROUP BY wk ORDER BY wk`),

      runSQL(`SELECT YEARWEEK(DATE(mlt.created_date), 1) AS wk,
                     COALESCE(SUM(mlt.amount),0) AS amt
              FROM m_loan_transaction mlt
              WHERE mlt.transaction_type_enum = 2 AND mlt.is_reversed = 0
                AND mlt.created_date >= DATE_SUB(CURDATE(), INTERVAL 12 WEEK) ${BFTXN}
              GROUP BY wk ORDER BY wk`)
    ]);

    res.json({ success: true, data: {
      disbursements: disbRows.map(r => parseFloat(r.amt)||0),
      collections:   colRows.map(r => parseFloat(r.amt)||0),
      loanCount:     disbRows.map(r => parseInt(r.cnt)||0)
    }});
  } catch (err) {
    console.error('[sparklines]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ─── /api/nova/collection-dow ────────────────────────────────────────────────
// NEW: Day-of-week collection pattern, rolling 90-day window.
app.get('/api/nova/collection-dow', async (_req, res) => {
  try {
    const rows = await runSQL(`
      SELECT
        DAYOFWEEK(DATE(mlt.created_date))  AS dow_num,
        DAYNAME(DATE(mlt.created_date))    AS day_name,
        COUNT(*)                           AS txn_count,
        COALESCE(SUM(mlt.amount), 0)       AS total_amount,
        COUNT(DISTINCT DATE(mlt.created_date)) AS day_count
      FROM m_loan_transaction mlt
      WHERE mlt.transaction_type_enum = 2
        AND mlt.is_reversed = 0
        AND DATE(mlt.created_date) >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
        AND DATE(mlt.created_date) <= CURDATE()
      GROUP BY DAYOFWEEK(DATE(mlt.created_date)), DAYNAME(DATE(mlt.created_date))
      ORDER BY dow_num
    `);

    res.json({ success: true, data: rows.map(r => ({
      dayName:   r.day_name,
      avgDaily:  r.day_count > 0 ? Math.round(parseFloat(r.total_amount) / parseInt(r.day_count)) : 0,
      txnCount:  parseInt(r.txn_count)||0,
      totalAmount: parseFloat(r.total_amount)||0
    }))});
  } catch (err) {
    console.error('[collection-dow]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ─── /api/nova/par-trend ─────────────────────────────────────────────────────
// NEW: 6-month PAR30 trend using schedule-based overdue installment principal.
//      Approximation: uses current payment status against historical due dates.
app.get('/api/nova/par-trend', async (_req, res) => {
  try {
    const rows = await runSQL(`
      SELECT
        m.month_label,
        ROUND(100.0 *
          COALESCE(SUM(GREATEST(
            rs.principal_amount
            - COALESCE(rs.principal_completed_derived,0)
            - COALESCE(rs.principal_writtenoff_derived,0), 0)), 0)
          / NULLIF(
              SUM(CASE WHEN ml.disbursedon_date <= m.month_end
                  THEN ml.principal_outstanding_derived ELSE NULL END), 0),
        2) AS par30Rate
      FROM (
        SELECT
          DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL n MONTH), '%b %Y') AS month_label,
          LAST_DAY(DATE_SUB(CURDATE(), INTERVAL n MONTH))              AS month_end
        FROM (SELECT 5 n UNION SELECT 4 UNION SELECT 3 UNION SELECT 2 UNION SELECT 1 UNION SELECT 0) nums
      ) m
      JOIN m_loan ml ON ml.loan_status_id = 300
        AND ml.disbursedon_date <= m.month_end
      LEFT JOIN m_loan_repayment_schedule rs ON rs.loan_id = ml.id
        AND rs.completed_derived = 0
        AND rs.duedate < DATE_SUB(m.month_end, INTERVAL 30 DAY)
      GROUP BY m.month_label, m.month_end
      ORDER BY m.month_end ASC
    `);

    res.json({ success: true, data: rows.map(r => ({
      month:    r.month_label,
      par30Rate: parseFloat(r.par30Rate)||0
    }))});
  } catch (err) {
    console.error('[par-trend]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ─── Legacy Metabase card/embed endpoints ────────────────────────────────────
async function queryCard(cardId, body = {}) {
  const session = await getSession();
  const r = await axios.post(`${METABASE_URL}/api/card/${cardId}/query/json`, body,
    { headers: { 'X-Metabase-Session': session }, httpsAgent });
  return r.data;
}

app.get('/metabase/card/:id', async (req, res) => {
  try {
    let body = {};
    if (req.query.params) try { body = JSON.parse(req.query.params); } catch {}
    res.json({ success: true, data: await queryCard(req.params.id, body) });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/metabase/embed-url', (req, res) => {
  try {
    const { dashboardId, params = {} } = req.body;
    if (!METABASE_SECRET || !METABASE_SITE_URL)
      return res.status(500).json({ success: false, error: 'METABASE_SECRET / METABASE_SITE_URL not set' });
    if (!dashboardId)
      return res.status(400).json({ success: false, error: 'dashboardId required' });
    const token = jwt.sign(
      { resource: { dashboard: dashboardId }, params, exp: Math.round(Date.now()/1000) + 600 },
      METABASE_SECRET
    );
    res.json({ success: true, url: `${METABASE_SITE_URL}/embed/dashboard/${token}#theme=night&bordered=false&titled=true` });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ─── /api/nova/ai-insight ─────────────────────────────────────────────────────
// Gathers live KPI + branch + officer + arrears data, calls Claude for a
// management briefing. POST body: { period, branch, userContext }
app.post('/api/nova/ai-insight', async (req, res) => {
  try {
    if (!ANTHROPIC_API_KEY)
      return res.status(503).json({ success: false, error: 'ANTHROPIC_API_KEY not set in .env' });

    const { period = 'month', branch, userContext = '' } = req.body;
    const p      = period;
    const bId    = branch && branch !== 'all' ? branch : null;
    const disbW  = periodWhere('ml.disbursedon_date', p);
    const disbP  = prevWhere('ml.disbursedon_date', p);
    const colW   = periodWhere('mlt.created_date', p);
    const colP   = prevWhere('mlt.created_date', p);
    const BF     = bf(bId);
    const BFTXN  = bfTxn(bId);
    const bWhere = bfOffice(bId);
    const oWhere = bfStaff(bId);

    const [
      disbCur, disbPrv, colCur, colPrv, active, par, avg,
      targetRows, colTargetRows,
      loanRows, branchColRows,
      officerRows, officerColRows,
      arrearsRows
    ] = await Promise.all([
      runSQL(`SELECT COUNT(*) AS cnt, COALESCE(SUM(ml.principal_disbursed_derived),0) AS total
              FROM m_loan ml JOIN m_client mc ON mc.id = ml.client_id
              WHERE ${disbW} AND ml.loan_status_id IN (300,500,602) ${BF}`),
      runSQL(`SELECT COALESCE(SUM(ml.principal_disbursed_derived),0) AS total
              FROM m_loan ml JOIN m_client mc ON mc.id = ml.client_id
              WHERE ${disbP} AND ml.loan_status_id IN (300,500,602) ${BF}`),
      runSQL(`SELECT COUNT(*) AS cnt, COALESCE(SUM(mlt.amount),0) AS total
              FROM m_loan_transaction mlt
              WHERE mlt.transaction_type_enum = 2 AND mlt.is_reversed = 0 AND ${colW} ${BFTXN}`),
      runSQL(`SELECT COALESCE(SUM(mlt.amount),0) AS total
              FROM m_loan_transaction mlt
              WHERE mlt.transaction_type_enum = 2 AND mlt.is_reversed = 0 AND ${colP} ${BFTXN}`),
      runSQL(`SELECT COUNT(*) AS cnt, COUNT(DISTINCT ml.client_id) AS clients,
                     COALESCE(SUM(ml.principal_outstanding_derived),0) AS outstanding
              FROM m_loan ml JOIN m_client mc ON mc.id = ml.client_id
              WHERE ml.loan_status_id = 300 ${BF}`),
      runSQL(`SELECT COALESCE(SUM(laa.principal_overdue_derived),0) AS overdue,
                     COALESCE(SUM(ml.principal_outstanding_derived),0) AS outstanding
              FROM m_loan ml JOIN m_client mc ON mc.id = ml.client_id
              LEFT JOIN m_loan_arrears_aging laa ON laa.loan_id = ml.id
              WHERE ml.loan_status_id = 300 ${BF}`),
      runSQL(`SELECT COALESCE(AVG(ml.principal_disbursed_derived),0) AS avg_loan
              FROM m_loan ml JOIN m_client mc ON mc.id = ml.client_id
              WHERE ${disbW} AND ml.loan_status_id IN (300,500,602) ${BF}`),
      runSQL(`SELECT COALESCE(SUM(rs.principal_amount + COALESCE(rs.interest_amount,0) + COALESCE(rs.fee_charges_amount,0)),0) AS total_due
              FROM m_loan_repayment_schedule rs
              JOIN m_loan ml ON ml.id = rs.loan_id
              JOIN m_client mc ON mc.id = ml.client_id
              WHERE ml.loan_status_id IN (300,500,602) AND ${periodWhere('rs.duedate', p)} ${BF}`),
      runSQL(`SELECT COALESCE(SUM(mlt.amount),0) AS total_collected
              FROM m_loan_transaction mlt
              WHERE mlt.transaction_type_enum = 2 AND mlt.is_reversed = 0 AND ${colW} ${BFTXN}`),
      runSQL(`SELECT mo.id AS branch_id, mo.name AS branch_name,
                COUNT(DISTINCT CASE WHEN ${disbW} AND ml.loan_status_id IN (300,500,602) THEN ml.id END) AS loans_disbursed,
                COALESCE(SUM(CASE WHEN ${disbW} AND ml.loan_status_id IN (300,500,602) THEN ml.principal_disbursed_derived ELSE 0 END),0) AS disbursed_amount,
                COUNT(DISTINCT CASE WHEN ml.loan_status_id = 300 THEN ml.id END) AS active_loans,
                COALESCE(SUM(CASE WHEN ml.loan_status_id = 300 THEN ml.principal_outstanding_derived ELSE 0 END),0) AS total_outstanding,
                COALESCE(SUM(CASE WHEN ml.loan_status_id = 300 THEN laa.principal_overdue_derived ELSE 0 END),0) AS par_overdue
              FROM m_office mo
              JOIN m_client mc ON mc.office_id = mo.id
              LEFT JOIN m_loan ml ON ml.client_id = mc.id
              LEFT JOIN m_loan_arrears_aging laa ON laa.loan_id = ml.id
              WHERE mo.parent_id IS NOT NULL ${bWhere}
              GROUP BY mo.id, mo.name ORDER BY disbursed_amount DESC`),
      runSQL(`SELECT mc.office_id AS branch_id, COALESCE(SUM(mlt.amount),0) AS collected_amount
              FROM m_loan_transaction mlt
              JOIN m_loan ml ON ml.id = mlt.loan_id
              JOIN m_client mc ON mc.id = ml.client_id
              WHERE mlt.transaction_type_enum = 2 AND mlt.is_reversed = 0 AND ${colW}
              ${bf(bId)}
              GROUP BY mc.office_id`),
      runSQL(`SELECT ms.id AS officer_id, ms.display_name AS officer_name, mo.name AS branch_name,
                COUNT(DISTINCT CASE WHEN ${disbW} AND ml.loan_status_id IN (300,500,602) THEN ml.id END) AS loans_disbursed,
                COALESCE(SUM(CASE WHEN ${disbW} AND ml.loan_status_id IN (300,500,602) THEN ml.principal_disbursed_derived ELSE 0 END),0) AS disbursed_amount,
                COUNT(DISTINCT CASE WHEN ml.loan_status_id = 300 THEN ml.id END) AS active_loans,
                COALESCE(SUM(CASE WHEN ml.loan_status_id = 300 THEN ml.principal_outstanding_derived ELSE 0 END),0) AS total_outstanding,
                COALESCE(SUM(CASE WHEN ml.loan_status_id = 300 THEN laa.principal_overdue_derived ELSE 0 END),0) AS par_overdue
              FROM m_staff ms
              JOIN m_office mo ON mo.id = ms.office_id
              LEFT JOIN m_loan ml ON ml.loan_officer_id = ms.id
              LEFT JOIN m_loan_arrears_aging laa ON laa.loan_id = ml.id AND ml.loan_status_id = 300
              WHERE ms.is_loan_officer = 1 AND ms.is_active = 1 ${oWhere}
              GROUP BY ms.id, ms.display_name, mo.name
              HAVING loans_disbursed > 0 OR active_loans > 0
              ORDER BY disbursed_amount DESC LIMIT 20`),
      runSQL(`SELECT ml.loan_officer_id AS officer_id, COALESCE(SUM(mlt.amount),0) AS collected_amount
              FROM m_loan_transaction mlt
              JOIN m_loan ml ON ml.id = mlt.loan_id
              ${bId ? bfJoinClient(bId) : ''}
              WHERE mlt.transaction_type_enum = 2 AND mlt.is_reversed = 0 AND ${colW}
                AND ml.loan_officer_id IS NOT NULL
              GROUP BY ml.loan_officer_id`),
      runSQL(`SELECT
                SUM(CASE WHEN days_in_arrears BETWEEN 1  AND 30 THEN 1 ELSE 0 END) AS count_1_30,
                SUM(CASE WHEN days_in_arrears BETWEEN 31 AND 60 THEN 1 ELSE 0 END) AS count_31_60,
                SUM(CASE WHEN days_in_arrears BETWEEN 61 AND 90 THEN 1 ELSE 0 END) AS count_61_90,
                SUM(CASE WHEN days_in_arrears > 90              THEN 1 ELSE 0 END) AS count_90plus,
                SUM(CASE WHEN days_in_arrears BETWEEN 1  AND 30 THEN outstanding ELSE 0 END) AS amt_1_30,
                SUM(CASE WHEN days_in_arrears BETWEEN 31 AND 60 THEN outstanding ELSE 0 END) AS amt_31_60,
                SUM(CASE WHEN days_in_arrears BETWEEN 61 AND 90 THEN outstanding ELSE 0 END) AS amt_61_90,
                SUM(CASE WHEN days_in_arrears > 90              THEN outstanding ELSE 0 END) AS amt_90plus
              FROM (
                SELECT ml.id,
                       DATEDIFF(CURDATE(), MIN(rs.duedate)) AS days_in_arrears,
                       ml.principal_outstanding_derived AS outstanding
                FROM m_loan ml
                JOIN m_client mc ON mc.id = ml.client_id
                JOIN m_loan_repayment_schedule rs ON rs.loan_id = ml.id
                WHERE ml.loan_status_id = 300 AND rs.duedate < CURDATE() AND rs.completed_derived = 0 ${BF}
                GROUP BY ml.id
              ) sub WHERE days_in_arrears > 0`)
    ]);

    // ── Format helpers ──
    const fmtM = n => n >= 1e9 ? `UGX ${(n/1e9).toFixed(2)}B`
                    : n >= 1e6 ? `UGX ${(n/1e6).toFixed(1)}M`
                    : n >= 1e3 ? `UGX ${(n/1e3).toFixed(0)}K`
                    : `UGX ${Math.round(n).toLocaleString()}`;
    const chgStr = (c, pv) => pv > 0
      ? `${c >= pv ? '+' : ''}${((c-pv)/pv*100).toFixed(1)}% vs prior period`
      : 'no prior period data';

    // ── Calculations ──
    const disbTotal  = num(disbCur, 'total');
    const colTotal   = num(colCur,  'total');
    const parOverdue = num(par, 'overdue');
    const parPort    = num(par, 'outstanding', 1);
    const parRate    = parPort > 0 ? Math.round(parOverdue / parPort * 10000) / 100 : 0;
    const parStatus  = parRate < 3 ? 'Excellent (<3%)' : parRate < 5 ? 'Good (3–5%)' : parRate < 10 ? 'At Risk (5–10%)' : 'Critical (>10%)';
    const colRate    = disbTotal > 0 ? (colTotal / disbTotal * 100).toFixed(1) : '0.0';
    const totalDue   = num(targetRows, 'total_due');
    const colTarget  = num(colTargetRows, 'total_collected');
    const achievement = totalDue > 0 ? (colTarget / totalDue * 100).toFixed(1) : '0.0';
    const periodLabel = { today:'Today', week:'This Week', month:'This Month', quarter:'This Quarter', year:'This Year', uptodate:'All Time' }[p] || p;

    // ── Build branch table ──
    const bColMap  = Object.fromEntries(branchColRows.map(r => [r.branch_id, parseFloat(r.collected_amount)||0]));
    const branches = loanRows.map(r => ({
      name: r.branch_name, disbursed: parseFloat(r.disbursed_amount)||0,
      collected: bColMap[r.branch_id] || 0, activeLoans: parseInt(r.active_loans)||0,
      outstanding: parseFloat(r.total_outstanding)||0,
      parRate: (() => { const os=parseFloat(r.total_outstanding)||0; const ov=parseFloat(r.par_overdue)||0; return os>0?Math.round(ov/os*10000)/100:0; })()
    })).filter(b => b.disbursed > 0 || b.activeLoans > 0);
    const branchTable = branches.length > 0
      ? branches.map(b => `  ${b.name}: Disbursed ${fmtM(b.disbursed)}, Collected ${fmtM(b.collected)}, Active ${b.activeLoans} loans, Col.Rate ${b.disbursed>0?(b.collected/b.disbursed*100).toFixed(1):0}%, PAR ${b.parRate}%`).join('\n')
      : '  No branch data';

    // ── Build officer tables ──
    const oColMap  = Object.fromEntries(officerColRows.map(r => [r.officer_id, parseFloat(r.collected_amount)||0]));
    const officers = officerRows.map(r => {
      const os = parseFloat(r.total_outstanding)||0, ov = parseFloat(r.par_overdue)||0;
      return { name: r.officer_name, branch: r.branch_name, disbursed: parseFloat(r.disbursed_amount)||0,
        activeLoans: parseInt(r.active_loans)||0, collected: oColMap[r.officer_id] || 0,
        parRate: os > 0 ? Math.round(ov/os*10000)/100 : 0 };
    });
    const topOfficers = [...officers].sort((a,b) => b.disbursed - a.disbursed).slice(0,5);
    const highPAR     = [...officers].filter(o => o.parRate > 5).sort((a,b) => b.parRate - a.parRate).slice(0,5);
    const officerTable = topOfficers.length > 0
      ? topOfficers.map(o => `  ${o.name} (${o.branch}): Disbursed ${fmtM(o.disbursed)}, Collected ${fmtM(o.collected)}, Active ${o.activeLoans} loans, PAR ${o.parRate}%`).join('\n')
      : '  No officer data';
    const highPARTable = highPAR.length > 0
      ? highPAR.map(o => `  ${o.name} (${o.branch}): PAR ${o.parRate}%, Active ${o.activeLoans} loans`).join('\n')
      : '  None above 5% threshold — good standing';
    const arrearsTable =
      `  1–30 days:  ${int(arrearsRows,'count_1_30')} loans, ${fmtM(num(arrearsRows,'amt_1_30'))}\n` +
      `  31–60 days: ${int(arrearsRows,'count_31_60')} loans, ${fmtM(num(arrearsRows,'amt_31_60'))}\n` +
      `  61–90 days: ${int(arrearsRows,'count_61_90')} loans, ${fmtM(num(arrearsRows,'amt_61_90'))}\n` +
      `  90+ days:   ${int(arrearsRows,'count_90plus')} loans, ${fmtM(num(arrearsRows,'amt_90plus'))}`;

    const contextSection = userContext.trim()
      ? `\nADDITIONAL CONTEXT FROM MANAGEMENT:\n${userContext.trim()}\n` : '';

    const prompt =
`You are a senior financial performance analyst advising the management team of a microfinance institution (MFI) in Uganda called Nova. You have access to live portfolio data.

PERIOD: ${periodLabel}${contextSection}

——— KEY PERFORMANCE INDICATORS ———
Disbursements:         ${fmtM(disbTotal)} — ${chgStr(disbTotal, num(disbPrv,'total'))} (${int(disbCur,'cnt')} loans)
Collections:           ${fmtM(colTotal)} — ${chgStr(colTotal, num(colPrv,'total'))} (${int(colCur,'cnt')} transactions)
Collection Rate:       ${colRate}% (collections as % of disbursements)
Collection vs Target:  ${achievement}% achievement (collected ${fmtM(colTarget)} of ${fmtM(totalDue)} scheduled)
Active Loans:          ${int(active,'cnt')} loans across ${int(active,'clients')} clients
Portfolio Outstanding: ${fmtM(num(active,'outstanding'))}
PAR > 30 Days:         ${parRate}% — ${fmtM(parOverdue)} at risk — Status: ${parStatus}
Average Loan Size:     ${fmtM(Math.round(num(avg,'avg_loan')))}

——— BRANCH PERFORMANCE ———
${branchTable}

——— TOP 5 OFFICERS (by disbursement) ———
${officerTable}

——— HIGH-RISK OFFICERS (PAR > 5%) ———
${highPARTable}

——— ARREARS AGING ———
${arrearsTable}

———————————————————————————————————————————————————————————————————————————————
Generate a management briefing using EXACTLY these five section headers (use ## for each):

## Executive Summary
2-3 sentences: overall portfolio health, the most important win, and the most critical concern.

## Portfolio Health
Analyse disbursement momentum, collection efficiency, and balance sheet strength. Compare to prior period. Identify trends.

## Risk Flags
Name specific branches or officers with high PAR. Highlight arrears aging buckets with concerning volumes. Flag any collection shortfall vs target. Quantify every risk.

## Performance Highlights
Name the top-performing branch and officer. Name the lowest-performing. What does the performance gap tell management?

## Recommended Actions
List 4-5 specific, numbered, prioritised actions for management THIS WEEK. Each must reference actual data (names, amounts, percentages). No generic advice.

Be direct and specific. Use UGX amounts. Write for senior management, not analysts.`;

    const insight = await callClaude(
      'You are a senior MFI financial analyst. Produce structured management briefings with specific data references.',
      prompt, 2048, AI_MODEL_STRONG
    );

    res.json({ success: true, insight, period: periodLabel, generatedAt: new Date().toISOString() });
  } catch (err) {
    console.error('[ai-insight]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ─── /api/nova/ai-followup ───────────────────────────────────────────────────
// Conversational follow-up on a previously generated insight.
app.post('/api/nova/ai-followup', async (req, res) => {
  try {
    if (!ANTHROPIC_API_KEY)
      return res.status(503).json({ success: false, error: 'ANTHROPIC_API_KEY not set in .env' });

    const { previousInsight, question, period = 'month', branch } = req.body;
    if (!question) return res.status(400).json({ success: false, error: 'question is required' });

    const periodLabel = { today:'Today', week:'This Week', month:'This Month', quarter:'This Quarter', year:'This Year', uptodate:'All Time' }[period] || period;

    const answer = await callClaude(
      'You are a senior MFI financial analyst. You previously generated a portfolio briefing. The user has a follow-up question. Answer concisely using the data from the briefing. Use UGX amounts and be specific.',
      `PREVIOUS BRIEFING:\n${previousInsight || '(no briefing provided)'}\n\nPERIOD: ${periodLabel}\n\nFOLLOW-UP QUESTION:\n${question}`,
      1024
    );

    res.json({ success: true, answer, generatedAt: new Date().toISOString() });
  } catch (err) {
    console.error('[ai-followup]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ─── /api/nova/ai-explain-kpi ────────────────────────────────────────────────
// Explains why a specific KPI has its current value, with context.
app.post('/api/nova/ai-explain-kpi', async (req, res) => {
  try {
    if (!ANTHROPIC_API_KEY)
      return res.status(503).json({ success: false, error: 'ANTHROPIC_API_KEY not set in .env' });

    const { kpiName, currentValue, previousValue, period = 'month', branch } = req.body;
    if (!kpiName) return res.status(400).json({ success: false, error: 'kpiName is required' });

    const p   = period;
    const bId = branch && branch !== 'all' ? branch : null;
    const BF  = bf(bId);

    // Gather contextual data based on which KPI
    let contextData = '';
    const kpi = kpiName.toLowerCase();

    if (kpi.includes('par') || kpi.includes('risk')) {
      const [arrears, branchPar] = await Promise.all([
        runSQL(`SELECT
          SUM(CASE WHEN DATEDIFF(CURDATE(), MIN(rs.duedate)) BETWEEN 1 AND 30 THEN 1 ELSE 0 END) AS cnt_1_30,
          SUM(CASE WHEN DATEDIFF(CURDATE(), MIN(rs.duedate)) BETWEEN 31 AND 60 THEN 1 ELSE 0 END) AS cnt_31_60,
          SUM(CASE WHEN DATEDIFF(CURDATE(), MIN(rs.duedate)) BETWEEN 61 AND 90 THEN 1 ELSE 0 END) AS cnt_61_90,
          SUM(CASE WHEN DATEDIFF(CURDATE(), MIN(rs.duedate)) > 90 THEN 1 ELSE 0 END) AS cnt_90plus
          FROM m_loan ml JOIN m_client mc ON mc.id = ml.client_id
          JOIN m_loan_repayment_schedule rs ON rs.loan_id = ml.id
          WHERE ml.loan_status_id = 300 AND rs.duedate < CURDATE() AND rs.completed_derived = 0 ${BF}
          GROUP BY ml.id`),
        runSQL(`SELECT mo.name AS branch, ROUND(100*COALESCE(SUM(laa.principal_overdue_derived),0)/NULLIF(SUM(ml.principal_outstanding_derived),0),2) AS par
          FROM m_loan ml JOIN m_client mc ON mc.id = ml.client_id
          JOIN m_office mo ON mo.id = mc.office_id
          LEFT JOIN m_loan_arrears_aging laa ON laa.loan_id = ml.id
          WHERE ml.loan_status_id = 300 ${BF}
          GROUP BY mo.id, mo.name HAVING par > 0 ORDER BY par DESC LIMIT 5`)
      ]);
      contextData = `Arrears aging: 1-30d: ${arrears.length} loans, 31-60d: see data, 61-90d: see data, 90+: see data\nBranch PAR breakdown (top 5):\n${branchPar.map(r=>`  ${r.branch}: ${r.par}%`).join('\n')}`;
    } else if (kpi.includes('collection')) {
      const [branchCol, officerCol] = await Promise.all([
        runSQL(`SELECT mo.name AS branch, COALESCE(SUM(mlt.amount),0) AS collected
          FROM m_loan_transaction mlt JOIN m_loan ml ON ml.id = mlt.loan_id
          JOIN m_client mc ON mc.id = ml.client_id JOIN m_office mo ON mo.id = mc.office_id
          WHERE mlt.transaction_type_enum = 2 AND mlt.is_reversed = 0 AND ${periodWhere('mlt.created_date', p)}
          ${bf(bId)}
          GROUP BY mo.id, mo.name ORDER BY collected DESC LIMIT 5`),
        runSQL(`SELECT ms.display_name AS officer, COALESCE(SUM(mlt.amount),0) AS collected
          FROM m_loan_transaction mlt JOIN m_loan ml ON ml.id = mlt.loan_id
          JOIN m_staff ms ON ms.id = ml.loan_officer_id
          WHERE mlt.transaction_type_enum = 2 AND mlt.is_reversed = 0 AND ${periodWhere('mlt.created_date', p)}
          ${(() => { const ids = parseBranchIds(bId); if (!ids) return ''; const cond = ids.length === 1 ? `= ${ids[0]}` : `IN (${ids.join(',')})`; return `AND ml.loan_id IN (SELECT ml2.id FROM m_loan ml2 JOIN m_client mc2 ON mc2.id=ml2.client_id WHERE mc2.office_id ${cond})`; })()}
          GROUP BY ms.id, ms.display_name ORDER BY collected DESC LIMIT 5`)
      ]);
      contextData = `Top branches by collection:\n${branchCol.map(r=>`  ${r.branch}: UGX ${parseFloat(r.collected).toLocaleString()}`).join('\n')}\nTop officers:\n${officerCol.map(r=>`  ${r.officer}: UGX ${parseFloat(r.collected).toLocaleString()}`).join('\n')}`;
    } else if (kpi.includes('disburs')) {
      const [branchDisb, productMix] = await Promise.all([
        runSQL(`SELECT mo.name AS branch, COALESCE(SUM(ml.principal_disbursed_derived),0) AS disbursed
          FROM m_loan ml JOIN m_client mc ON mc.id = ml.client_id JOIN m_office mo ON mo.id = mc.office_id
          WHERE ${periodWhere('ml.disbursedon_date', p)} AND ml.loan_status_id IN (300,500,602) ${BF}
          GROUP BY mo.id, mo.name ORDER BY disbursed DESC LIMIT 5`),
        runSQL(`SELECT mpl.name AS product, COUNT(*) AS cnt, COALESCE(SUM(ml.principal_disbursed_derived),0) AS total
          FROM m_loan ml JOIN m_client mc ON mc.id = ml.client_id JOIN m_product_loan mpl ON mpl.id = ml.product_id
          WHERE ${periodWhere('ml.disbursedon_date', p)} AND ml.loan_status_id IN (300,500,602) ${BF}
          GROUP BY mpl.id, mpl.name ORDER BY total DESC LIMIT 5`)
      ]);
      contextData = `Top branches by disbursement:\n${branchDisb.map(r=>`  ${r.branch}: UGX ${parseFloat(r.disbursed).toLocaleString()}`).join('\n')}\nProduct mix:\n${productMix.map(r=>`  ${r.product}: ${r.cnt} loans, UGX ${parseFloat(r.total).toLocaleString()}`).join('\n')}`;
    }

    const periodLabel = { today:'Today', week:'This Week', month:'This Month', quarter:'This Quarter', year:'This Year', uptodate:'All Time' }[p] || p;
    const explanation = await callClaude(
      'You are a microfinance analyst in Uganda. Explain KPI metrics in 3-5 concise sentences for senior management. Reference specific branches, officers, or products when the data supports it. Use UGX amounts.',
      `KPI: ${kpiName}\nCurrent Value: ${currentValue}\n${previousValue ? `Previous Period Value: ${previousValue}\n` : ''}Period: ${periodLabel}\n\nContextual Data:\n${contextData || 'No additional context available'}\n\nExplain what this metric means, what is driving its current value, and suggest one specific action.`,
      512
    );

    res.json({ success: true, explanation, generatedAt: new Date().toISOString() });
  } catch (err) {
    console.error('[ai-explain-kpi]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ─── /api/nova/ai-query ──────────────────────────────────────────────────────
// Natural language → SQL → results → AI summary
app.post('/api/nova/ai-query', async (req, res) => {
  try {
    if (!ANTHROPIC_API_KEY)
      return res.status(503).json({ success: false, error: 'ANTHROPIC_API_KEY not set in .env' });

    const { question } = req.body;
    if (!question) return res.status(400).json({ success: false, error: 'question is required' });

    // Step 1: Translate natural language to SQL
    const schemaPrompt = `You are a SQL expert for a Fineract-based microfinance database (MySQL). Given a natural language question, produce a SELECT query.

SCHEMA (key tables and columns):
- m_loan: id, client_id, product_id, loan_officer_id, loan_status_id (300=Active, 500=Closed, 602=In-Arrears), principal_disbursed_derived, principal_outstanding_derived, total_outstanding_derived, disbursedon_date, closedon_date
- m_client: id, display_name, office_id, activation_date
- m_office: id, name, parent_id (NULL = head office, NOT NULL = branch)
- m_loan_transaction: id, loan_id, transaction_type_enum (2=Repayment), amount, created_date, is_reversed (0=valid)
- m_loan_repayment_schedule: id, loan_id, duedate, principal_amount, interest_amount, fee_charges_amount, completed_derived (0=unpaid, 1=paid), principal_completed_derived
- m_loan_arrears_aging: loan_id, principal_overdue_derived, overdue_since_date_derived
- m_product_loan: id, name
- m_staff: id, display_name, office_id, is_loan_officer, is_active

RULES:
- Always use SELECT only. Never use INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE.
- Branch = m_office where parent_id IS NOT NULL.
- Always JOIN through m_client for office/branch filtering (m_loan has no office_id).
- Use CURDATE() for current date references.
- Add LIMIT 100 if the query might return many rows.
- Currency is UGX (Ugandan Shillings).

Respond with ONLY a JSON object: {"sql": "SELECT ...", "explanation": "Brief explanation of what this query does"}
No markdown, no code blocks — just the raw JSON.`;

    const sqlResponse = await callClaude(schemaPrompt, question, 1024, AI_MODEL_STRONG);

    let parsed;
    try {
      // Try to extract JSON from the response (handle potential markdown wrapping)
      const jsonMatch = sqlResponse.match(/\{[\s\S]*\}/);
      parsed = JSON.parse(jsonMatch ? jsonMatch[0] : sqlResponse);
    } catch (e) {
      return res.status(400).json({ success: false, error: 'AI could not generate a valid query. Try rephrasing your question.' });
    }

    const sql = (parsed.sql || '').trim();
    const explanation = parsed.explanation || '';

    // Safety: only allow SELECT
    if (!sql.toUpperCase().startsWith('SELECT')) {
      return res.status(400).json({ success: false, error: 'Only SELECT queries are allowed.' });
    }
    const forbidden = /\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|GRANT|REVOKE)\b/i;
    if (forbidden.test(sql)) {
      return res.status(400).json({ success: false, error: 'Query contains forbidden statements.' });
    }

    // Step 2: Execute
    let data;
    try {
      data = await runSQL(sql);
    } catch (sqlErr) {
      return res.status(400).json({ success: false, error: `Query failed: ${sqlErr.message}`, sql, explanation });
    }

    // Step 3: Summarize results
    const preview = data.slice(0, 20);
    const summary = await callClaude(
      'You are a microfinance analyst. Summarize query results in 2-4 sentences for senior management. Use UGX for amounts. Be specific and direct.',
      `QUESTION: ${question}\nQUERY: ${sql}\nRESULTS (${data.length} rows, showing first ${preview.length}):\n${JSON.stringify(preview, null, 2)}`,
      512
    );

    res.json({ success: true, sql, explanation, data: data.slice(0, 100), totalRows: data.length, summary, generatedAt: new Date().toISOString() });
  } catch (err) {
    console.error('[ai-query]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// OPERATIONAL TOOLS — actionable data for field operations
// ═══════════════════════════════════════════════════════════════════════════════

// ─── Overdue Follow-Up List ──────────────────────────────────────────────────
// Clients with missed repayments, ordered by days overdue (most urgent first)
app.get('/api/nova/follow-up-list', async (req, res) => {
  try {
    const bId = req.query.branch && req.query.branch !== 'all' ? req.query.branch : null;
    const BF  = bf(bId);
    const rows = await runSQL(`
      SELECT
        ms.display_name AS officer_name,
        mc.display_name AS client_name,
        mc.mobile_no AS client_phone,
        ml.account_no AS loan_account,
        mo.name AS branch_name,
        MIN(rs.duedate) AS oldest_missed_date,
        DATEDIFF(CURDATE(), MIN(rs.duedate)) AS days_overdue,
        COUNT(DISTINCT rs.id) AS missed_installments,
        COALESCE(SUM(GREATEST(
          (rs.principal_amount + COALESCE(rs.interest_amount,0) + COALESCE(rs.fee_charges_amount,0))
          - (COALESCE(rs.principal_completed_derived,0) + COALESCE(rs.interest_completed_derived,0)
             + COALESCE(rs.fee_charges_completed_derived,0)), 0)), 0) AS total_overdue
      FROM m_loan_repayment_schedule rs
      JOIN m_loan ml ON ml.id = rs.loan_id
      JOIN m_client mc ON mc.id = ml.client_id
      LEFT JOIN m_staff ms ON ms.id = ml.loan_officer_id
      LEFT JOIN m_office mo ON mo.id = mc.office_id
      WHERE rs.duedate < CURDATE()
        AND rs.completed_derived = 0
        AND ml.loan_status_id = 300
        ${BF}
      GROUP BY ms.display_name, mc.display_name, mc.mobile_no, ml.account_no, mo.name
      ORDER BY days_overdue DESC
      LIMIT 200
    `);
    res.json({ success: true, data: rows.map(r => ({
      officerName:       r.officer_name,
      clientName:        r.client_name,
      clientPhone:       r.client_phone || '—',
      loanAccount:       r.loan_account,
      branchName:        r.branch_name,
      oldestMissedDate:  r.oldest_missed_date,
      daysOverdue:       parseInt(r.days_overdue)||0,
      missedInstallments:parseInt(r.missed_installments)||0,
      totalOverdue:      parseFloat(r.total_overdue)||0
    }))});
  } catch (err) {
    console.error('[follow-up-list]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ─── This Week's Due Schedule ────────────────────────────────────────────────
// Day-by-day repayment schedule for the current week, with client + officer
app.get('/api/nova/week-due-schedule', async (req, res) => {
  try {
    const bId = req.query.branch && req.query.branch !== 'all' ? req.query.branch : null;
    const BF  = bf(bId);
    const rows = await runSQL(`
      SELECT
        rs.duedate AS due_date,
        DAYNAME(rs.duedate) AS day_name,
        mc.display_name AS client_name,
        mc.mobile_no AS client_phone,
        ms.display_name AS officer_name,
        mo.name AS branch_name,
        ml.account_no AS loan_account,
        COALESCE(
          (rs.principal_amount + COALESCE(rs.interest_amount,0) + COALESCE(rs.fee_charges_amount,0))
          - (COALESCE(rs.principal_completed_derived,0) + COALESCE(rs.interest_completed_derived,0)
             + COALESCE(rs.fee_charges_completed_derived,0)), 0) AS amount_due,
        rs.completed_derived AS is_paid
      FROM m_loan_repayment_schedule rs
      JOIN m_loan ml ON ml.id = rs.loan_id
      JOIN m_client mc ON mc.id = ml.client_id
      LEFT JOIN m_staff ms ON ms.id = ml.loan_officer_id
      LEFT JOIN m_office mo ON mo.id = mc.office_id
      WHERE rs.duedate BETWEEN
        DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) DAY)
        AND DATE_ADD(DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) DAY), INTERVAL 6 DAY)
        AND ml.loan_status_id = 300
        ${BF}
      ORDER BY rs.duedate, ms.display_name, mc.display_name
      LIMIT 500
    `);
    res.json({ success: true, data: rows.map(r => ({
      dueDate:     r.due_date,
      dayName:     r.day_name,
      clientName:  r.client_name,
      clientPhone: r.client_phone || '—',
      officerName: r.officer_name,
      branchName:  r.branch_name,
      loanAccount: r.loan_account,
      amountDue:   parseFloat(r.amount_due)||0,
      isPaid:      parseInt(r.is_paid)===1 ? 'Yes' : 'No'
    }))});
  } catch (err) {
    console.error('[week-due-schedule]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ─── Officer Workload / Task Summary ─────────────────────────────────────────
// Per-officer: how many clients due today, overdue, and total outstanding
app.get('/api/nova/officer-workload', async (req, res) => {
  try {
    const bId = req.query.branch && req.query.branch !== 'all' ? req.query.branch : null;
    const BF  = bf(bId);
    const rows = await runSQL(`
      SELECT
        ms.display_name AS officer_name,
        mo.name AS branch_name,
        COUNT(DISTINCT CASE WHEN rs.duedate = CURDATE() AND rs.completed_derived = 0 THEN ml.client_id END) AS clients_due_today,
        COUNT(DISTINCT CASE WHEN rs.duedate < CURDATE() AND rs.completed_derived = 0 THEN ml.client_id END) AS clients_overdue,
        COUNT(DISTINCT CASE WHEN rs.duedate = CURDATE() AND rs.completed_derived = 0 THEN ml.id END) AS loans_due_today,
        COUNT(DISTINCT CASE WHEN rs.duedate < CURDATE() AND rs.completed_derived = 0 THEN ml.id END) AS loans_overdue,
        COALESCE(SUM(CASE WHEN rs.duedate = CURDATE() AND rs.completed_derived = 0
          THEN GREATEST((rs.principal_amount + COALESCE(rs.interest_amount,0))
               - (COALESCE(rs.principal_completed_derived,0) + COALESCE(rs.interest_completed_derived,0)), 0) ELSE 0 END), 0) AS amount_due_today,
        COALESCE(SUM(CASE WHEN rs.duedate < CURDATE() AND rs.completed_derived = 0
          THEN GREATEST((rs.principal_amount + COALESCE(rs.interest_amount,0))
               - (COALESCE(rs.principal_completed_derived,0) + COALESCE(rs.interest_completed_derived,0)), 0) ELSE 0 END), 0) AS amount_overdue
      FROM m_loan_repayment_schedule rs
      JOIN m_loan ml ON ml.id = rs.loan_id
      JOIN m_client mc ON mc.id = ml.client_id
      LEFT JOIN m_staff ms ON ms.id = ml.loan_officer_id
      LEFT JOIN m_office mo ON mo.id = mc.office_id
      WHERE (rs.duedate <= CURDATE()) AND rs.completed_derived = 0
        AND ml.loan_status_id = 300
        ${BF}
      GROUP BY ms.display_name, mo.name
      ORDER BY amount_overdue DESC
    `);
    res.json({ success: true, data: rows.map(r => ({
      officerName:    r.officer_name,
      branchName:     r.branch_name,
      clientsDueToday:parseInt(r.clients_due_today)||0,
      clientsOverdue: parseInt(r.clients_overdue)||0,
      loansDueToday:  parseInt(r.loans_due_today)||0,
      loansOverdue:   parseInt(r.loans_overdue)||0,
      amountDueToday: parseFloat(r.amount_due_today)||0,
      amountOverdue:  parseFloat(r.amount_overdue)||0
    }))});
  } catch (err) {
    console.error('[officer-workload]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ─── Disbursement Pipeline ───────────────────────────────────────────────────
// Loans approved but not yet disbursed — pending pipeline
app.get('/api/nova/disbursement-pipeline', async (req, res) => {
  try {
    const bId = req.query.branch && req.query.branch !== 'all' ? req.query.branch : null;
    const BF  = bf(bId);
    const rows = await runSQL(`
      SELECT
        ml.account_no AS loan_account,
        mc.display_name AS client_name,
        mc.mobile_no AS client_phone,
        ms.display_name AS officer_name,
        mo.name AS branch_name,
        mpl.name AS product_name,
        ml.approved_principal AS approved_amount,
        ml.approvedon_date AS approved_date,
        DATEDIFF(CURDATE(), ml.approvedon_date) AS days_since_approval
      FROM m_loan ml
      JOIN m_client mc ON mc.id = ml.client_id
      LEFT JOIN m_staff ms ON ms.id = ml.loan_officer_id
      LEFT JOIN m_office mo ON mo.id = mc.office_id
      LEFT JOIN m_product_loan mpl ON mpl.id = ml.product_id
      WHERE ml.loan_status_id = 200
        ${BF}
      ORDER BY ml.approvedon_date ASC
      LIMIT 200
    `);
    res.json({ success: true, data: rows.map(r => ({
      loanAccount:      r.loan_account,
      clientName:       r.client_name,
      clientPhone:      r.client_phone || '—',
      officerName:      r.officer_name,
      branchName:       r.branch_name,
      productName:      r.product_name,
      approvedAmount:   parseFloat(r.approved_amount)||0,
      approvedDate:     r.approved_date,
      daysSinceApproval:parseInt(r.days_since_approval)||0
    }))});
  } catch (err) {
    console.error('[disbursement-pipeline]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ─── At-Risk Clients (PAR by client) ─────────────────────────────────────────
// Individual clients whose loans are 30+ days overdue — for recovery prioritization
app.get('/api/nova/at-risk-clients', async (req, res) => {
  try {
    const bId = req.query.branch && req.query.branch !== 'all' ? req.query.branch : null;
    const BF  = bf(bId);
    const rows = await runSQL(`
      SELECT
        mc.display_name AS client_name,
        mc.mobile_no AS client_phone,
        ml.account_no AS loan_account,
        ms.display_name AS officer_name,
        mo.name AS branch_name,
        mpl.name AS product_name,
        ml.principal_outstanding_derived AS outstanding,
        ml.total_overdue_derived AS total_overdue,
        DATEDIFF(CURDATE(), MIN(rs.duedate)) AS max_days_overdue,
        CASE
          WHEN DATEDIFF(CURDATE(), MIN(rs.duedate)) <= 30 THEN '1-30 days'
          WHEN DATEDIFF(CURDATE(), MIN(rs.duedate)) <= 60 THEN '31-60 days'
          WHEN DATEDIFF(CURDATE(), MIN(rs.duedate)) <= 90 THEN '61-90 days'
          ELSE '90+ days'
        END AS aging_bucket
      FROM m_loan ml
      JOIN m_client mc ON mc.id = ml.client_id
      LEFT JOIN m_staff ms ON ms.id = ml.loan_officer_id
      LEFT JOIN m_office mo ON mo.id = mc.office_id
      LEFT JOIN m_product_loan mpl ON mpl.id = ml.product_id
      JOIN m_loan_repayment_schedule rs ON rs.loan_id = ml.id
        AND rs.duedate < DATE_SUB(CURDATE(), INTERVAL 30 DAY)
        AND rs.completed_derived = 0
      WHERE ml.loan_status_id = 300
        ${BF}
      GROUP BY mc.display_name, mc.mobile_no, ml.account_no, ms.display_name,
               mo.name, mpl.name, ml.principal_outstanding_derived, ml.total_overdue_derived
      ORDER BY max_days_overdue DESC
      LIMIT 200
    `);
    res.json({ success: true, data: rows.map(r => ({
      clientName:    r.client_name,
      clientPhone:   r.client_phone || '—',
      loanAccount:   r.loan_account,
      officerName:   r.officer_name,
      branchName:    r.branch_name,
      productName:   r.product_name,
      outstanding:   parseFloat(r.outstanding)||0,
      totalOverdue:  parseFloat(r.total_overdue)||0,
      maxDaysOverdue:parseInt(r.max_days_overdue)||0,
      agingBucket:   r.aging_bucket
    }))});
  } catch (err) {
    console.error('[at-risk-clients]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ─── Repayment Collected Today ───────────────────────────────────────────────
// Real-time feed of payments received today
app.get('/api/nova/collections-received-today', async (req, res) => {
  try {
    const bId = req.query.branch && req.query.branch !== 'all' ? req.query.branch : null;
    const BFTXN = bfTxn(bId);
    const rows = await runSQL(`
      SELECT
        mlt.created_date AS payment_time,
        mc.display_name AS client_name,
        ml.account_no AS loan_account,
        ms.display_name AS officer_name,
        mo.name AS branch_name,
        mlt.amount AS amount_paid,
        mlt.transaction_type_enum AS txn_type
      FROM m_loan_transaction mlt
      JOIN m_loan ml ON ml.id = mlt.loan_id
      JOIN m_client mc ON mc.id = ml.client_id
      LEFT JOIN m_staff ms ON ms.id = ml.loan_officer_id
      LEFT JOIN m_office mo ON mo.id = mc.office_id
      WHERE DATE(mlt.created_date) = CURDATE()
        AND mlt.transaction_type_enum = 2
        AND mlt.is_reversed = 0
        ${BFTXN}
      ORDER BY mlt.created_date DESC
      LIMIT 300
    `);
    res.json({ success: true, data: rows.map(r => ({
      paymentTime: r.payment_time,
      clientName:  r.client_name,
      loanAccount: r.loan_account,
      officerName: r.officer_name,
      branchName:  r.branch_name,
      amountPaid:  parseFloat(r.amount_paid)||0
    }))});
  } catch (err) {
    console.error('[collections-received-today]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ─── /api/nova/portfolio-outstanding ─────────────────────────────────────────
app.get('/api/nova/portfolio-outstanding', async (req, res) => {
  try {
    const bId = req.query.branch && req.query.branch !== 'all' ? req.query.branch : null;
    const BF = bf(bId);
    const period = req.query.period || 'rolling12m';
    const startDate = req.query.start || null;
    const endDate = req.query.end || null;

    // Date filter: custom range or period-based
    let dateFilter;
    if (startDate && endDate) {
      dateFilter = `DATE(snap_date) BETWEEN '${startDate}' AND '${endDate}'`;
    } else {
      // For portfolio outstanding timeseries we look at loan disbursal dates to build monthly snapshots
      dateFilter = periodWhere('snap_date', period);
    }

    // Monthly portfolio outstanding: for each month, sum principal_outstanding of loans active at that month-end
    const rows = await runSQL(`
      SELECT
        DATE_FORMAT(snap.snap_date, '%Y-%m') AS month,
        snap.snap_date,
        COUNT(DISTINCT ml.id) AS active_loans,
        COUNT(DISTINCT ml.client_id) AS active_clients,
        COALESCE(SUM(ml.principal_outstanding_derived), 0) AS principal_outstanding,
        COALESCE(SUM(ml.total_outstanding_derived), 0) AS total_outstanding,
        COALESCE(SUM(ml.principal_disbursed_derived), 0) AS total_disbursed,
        COALESCE(SUM(CASE WHEN ml.loan_status_id = 602 THEN ml.principal_outstanding_derived ELSE 0 END), 0) AS arrears_outstanding
      FROM (
        SELECT LAST_DAY(DATE_SUB(CURDATE(), INTERVAL n MONTH)) AS snap_date
        FROM (
          SELECT 0 AS n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3
          UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7
          UNION SELECT 8 UNION SELECT 9 UNION SELECT 10 UNION SELECT 11
          UNION SELECT 12 UNION SELECT 13 UNION SELECT 14 UNION SELECT 15
          UNION SELECT 16 UNION SELECT 17 UNION SELECT 18 UNION SELECT 19
          UNION SELECT 20 UNION SELECT 21 UNION SELECT 22 UNION SELECT 23
        ) months
      ) snap
      JOIN m_loan ml ON ml.disbursedon_date <= snap.snap_date
        AND (ml.closedon_date IS NULL OR ml.closedon_date > snap.snap_date)
        AND ml.loan_status_id IN (300, 500, 602)
      JOIN m_client mc ON mc.id = ml.client_id
      WHERE ${dateFilter}
        ${BF}
      GROUP BY snap.snap_date
      ORDER BY snap.snap_date ASC
    `);

    const data = rows.map(r => ({
      month:                r.month,
      snapshotDate:         r.snap_date,
      activeLoans:          parseInt(r.active_loans) || 0,
      activeClients:        parseInt(r.active_clients) || 0,
      principalOutstanding: parseFloat(r.principal_outstanding) || 0,
      totalOutstanding:     parseFloat(r.total_outstanding) || 0,
      totalDisbursed:       parseFloat(r.total_disbursed) || 0,
      arrearsOutstanding:   parseFloat(r.arrears_outstanding) || 0,
      parRate:              (() => {
        const os = parseFloat(r.principal_outstanding) || 0;
        const ar = parseFloat(r.arrears_outstanding) || 0;
        return os > 0 ? Math.round(ar / os * 10000) / 100 : 0;
      })()
    }));

    res.json({ success: true, data });
  } catch (err) {
    console.error('[portfolio-outstanding]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ─── /api/nova/ai-status ─────────────────────────────────────────────────────
app.get('/api/nova/ai-status', (_req, res) => {
  res.json({ success: true, configured: !!ANTHROPIC_API_KEY });
});

app.use((_req, res) => res.status(404).json({ success: false, error: 'Not found' }));

app.listen(PORT, () => {
  console.log(`Nova Dashboard  →  http://localhost:${PORT}`);
  console.log(`Metabase URL    →  ${METABASE_URL}`);
  console.log(`DB ID           →  ${NOVA_DB_ID}`);
});
