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

const {
  METABASE_SITE_URL,
  METABASE_URL,
  METABASE_USERNAME,
  METABASE_PASSWORD,
  METABASE_SECRET,
  PORT          = 3000,
  CACHE_TTL     = 3600,   // session cache: 1 hr
  NOVA_DB_ID    = 2
} = process.env;

if (!METABASE_URL) console.warn('[WARN] METABASE_URL not set');

// ─── SSL agent (self-signed cert on Metabase host) ───────────────────────────
const httpsAgent = new https.Agent({ rejectUnauthorized: false });

// ─── Session cache ────────────────────────────────────────────────────────────
let _session = null, _sessionAt = 0;

async function getSession() {
  const now = Date.now() / 1000;
  if (_session && now - _sessionAt < CACHE_TTL) return _session;
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
    default:         return `YEAR(${col}) = YEAR(DATE_SUB(CURDATE(), INTERVAL 1 MONTH)) AND MONTH(${col}) = MONTH(DATE_SUB(CURDATE(), INTERVAL 1 MONTH))`;
  }
}

// ─── Branch filter helpers ────────────────────────────────────────────────────
// RULE: Office ALWAYS resolved via m_client.office_id — ml.office_id does NOT exist.
// bf()     → appends `AND mc.office_id = X` (use where mc is already joined)
// bfTxn()  → subquery for transaction tables that only have loan_id
const bf    = id => id ? `AND mc.office_id = ${parseInt(id)}` : '';
const bfTxn = id => !id ? '' : `AND mlt.loan_id IN (
  SELECT ml2.id FROM m_loan ml2 JOIN m_client mc2 ON mc2.id = ml2.client_id
  WHERE mc2.office_id = ${parseInt(id)})`;

// ─── /api/nova/kpis ───────────────────────────────────────────────────────────
// PAR → Method A: laa.principal_overdue_derived (HelaPlus-validated)
// Collections → created_date (posting date)
app.get('/api/nova/kpis', async (req, res) => {
  try {
    const p  = req.query.period || 'month';
    const bId = req.query.branch !== 'all' ? req.query.branch : null;

    const disbW = periodWhere('ml.disbursedon_date', p);
    const disbP = prevWhere('ml.disbursedon_date', p);
    const colW  = periodWhere('mlt.created_date', p);
    const colP  = prevWhere('mlt.created_date', p);
    const BF    = bf(bId);
    const BFTXN = bfTxn(bId);

    const [disbCur, disbPrv, colCur, colPrv, active, par, avg, allTime, balance] = await Promise.all([

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

      // Active loans snapshot
      runSQL(`SELECT COUNT(*) AS cnt, COUNT(DISTINCT ml.client_id) AS clients,
                     COALESCE(SUM(ml.principal_outstanding_derived),0) AS outstanding
              FROM m_loan ml JOIN m_client mc ON mc.id = ml.client_id
              WHERE ml.loan_status_id = 300 ${BF}`),

      // PAR – Method A: laa.principal_overdue_derived (never use overdue_since_date_derived for PAR)
      runSQL(`SELECT COALESCE(SUM(laa.principal_overdue_derived),0) AS overdue,
                     COALESCE(SUM(ml.principal_outstanding_derived),0) AS outstanding
              FROM m_loan ml
              JOIN m_client mc ON mc.id = ml.client_id
              LEFT JOIN m_loan_arrears_aging laa ON laa.loan_id = ml.id
              WHERE ml.loan_status_id = 300 ${BF}`),

      // Avg loan size – this period
      runSQL(`SELECT COALESCE(AVG(ml.principal_disbursed_derived),0) AS avg_loan
              FROM m_loan ml JOIN m_client mc ON mc.id = ml.client_id
              WHERE ${disbW} AND ml.loan_status_id IN (300,500,602) ${BF}`),

      // All-time book (total portfolio)
      runSQL(`SELECT COUNT(*) AS cnt, COALESCE(SUM(ml.principal_disbursed_derived),0) AS total
              FROM m_loan ml JOIN m_client mc ON mc.id = ml.client_id
              WHERE ml.loan_status_id IN (300,500,600,602,700) ${BF}`),

      // Live balance (portfolio outstanding)
      runSQL(`SELECT COUNT(*) AS cnt, COALESCE(SUM(ml.principal_outstanding_derived),0) AS total
              FROM m_loan ml JOIN m_client mc ON mc.id = ml.client_id
              WHERE ml.loan_status_id = 300 ${BF}`)
    ]);

    const disbTotal  = num(disbCur, 'total');
    const disbPrev   = num(disbPrv, 'total');
    const colTotal   = num(colCur, 'total');
    const colPrev    = num(colPrv, 'total');
    const parOverdue = num(par, 'overdue');
    const parPort    = num(par, 'outstanding', 1);
    const parRate    = parPort > 0 ? Math.round(parOverdue / parPort * 10000) / 100 : 0;

    res.json({ success: true, period: p, data: {
      totalPortfolio:       { total: num(allTime, 'total'), loanCount: int(allTime, 'cnt') },
      portfolioOutstanding: { total: num(balance, 'total'), loanCount: int(balance, 'cnt') },
      disbursements:        { total: disbTotal, loanCount: int(disbCur, 'cnt'), change: pct(disbTotal, disbPrev) },
      collections:          { total: colTotal,  txnCount: int(colCur, 'cnt'),   change: pct(colTotal, colPrev) },
      activeLoans:          { count: int(active,'cnt'), clientCount: int(active,'clients'), totalOutstanding: num(active,'outstanding') },
      par30: { rate: parRate, amount: parOverdue,
               status: parRate < 3 ? 'excellent' : parRate < 5 ? 'good' : parRate < 10 ? 'warning' : 'danger' },
      avgLoanSize:    { amount: Math.round(num(avg, 'avg_loan')) },
      collectionRate: disbTotal > 0 ? Math.round(colTotal / disbTotal * 1000) / 10 : 0
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
    const bId = req.query.branch !== 'all' ? req.query.branch : null;
    const BF    = bf(bId);
    const BFTXN = bfTxn(bId);
    const disbW = periodWhere('ml.disbursedon_date', p);
    const colW  = periodWhere('mlt.created_date', p);

    let disbExpr, colExpr, disbGrp, colGrp;
    if (p === 'today') {
      disbExpr = `DATE_FORMAT(ml.disbursedon_date, '%H:00')`; disbGrp = `HOUR(ml.disbursedon_date)`;
      colExpr  = `DATE_FORMAT(mlt.created_date, '%H:00')`;    colGrp  = `HOUR(mlt.created_date)`;
    } else if (p === 'year' || p === 'uptodate') {
      disbExpr = disbGrp = `DATE_FORMAT(ml.disbursedon_date, '%Y-%m')`;
      colExpr  = colGrp  = `DATE_FORMAT(mlt.created_date, '%Y-%m')`;
    } else {
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

// ─── /api/nova/branches ───────────────────────────────────────────────────────
// FIXED: Split into 2 queries to avoid transaction cartesian product.
//        Office path: m_office → m_client → m_loan (correct join).
app.get('/api/nova/branches', async (req, res) => {
  try {
    const p   = req.query.period || 'month';
    const bId = req.query.branch !== 'all' ? req.query.branch : null;
    const disbW  = periodWhere('ml.disbursedon_date', p);
    const colW   = periodWhere('mlt.created_date', p);
    const bWhere = bId ? `AND mo.id = ${parseInt(bId)}` : '';

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
        WHERE mo.parent_id IS NOT NULL ${bWhere}
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
        ${bId ? `AND mc.office_id = ${parseInt(bId)}` : ''}
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
    const bId = req.query.branch !== 'all' ? req.query.branch : null;
    const disbW  = periodWhere('ml.disbursedon_date', p);
    const colW   = periodWhere('mlt.created_date', p);
    const bWhere = bId ? `AND ms.office_id = ${parseInt(bId)}` : '';

    const [officerRows, colRows] = await Promise.all([
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
        WHERE ms.is_loan_officer = 1 AND ms.is_active = 1 ${bWhere}
        GROUP BY ms.id, ms.display_name, mo.name
        HAVING loans_disbursed > 0 OR active_loans > 0
        ORDER BY disbursed_amount DESC
        LIMIT 50
      `),

      // Collections for this period, per officer (via loan → client for branch safety)
      runSQL(`
        SELECT ml.loan_officer_id AS officer_id, COALESCE(SUM(mlt.amount),0) AS collected_amount
        FROM m_loan_transaction mlt
        JOIN m_loan ml ON ml.id = mlt.loan_id
        ${bId ? `JOIN m_client mc ON mc.id = ml.client_id AND mc.office_id = ${parseInt(bId)}` : ''}
        WHERE mlt.transaction_type_enum = 2 AND mlt.is_reversed = 0 AND ${colW}
          AND ml.loan_officer_id IS NOT NULL
        GROUP BY ml.loan_officer_id
      `)
    ]);

    const colMap = Object.fromEntries(colRows.map(r => [r.officer_id, parseFloat(r.collected_amount)||0]));

    res.json({ success: true, period: p, data: officerRows.map(r => {
      const outstanding = parseFloat(r.total_outstanding)||0;
      const overdue     = parseFloat(r.par_overdue)||0;
      return {
        officerId:        r.officer_id,
        officerName:      r.officer_name,
        branchName:       r.branch_name,
        loansDisbursed:   parseInt(r.loans_disbursed)||0,
        disbursedAmount:  parseFloat(r.disbursed_amount)||0,
        activeLoans:      parseInt(r.active_loans)||0,
        totalOutstanding: outstanding,
        collectedAmount:  colMap[r.officer_id] || 0,
        par30Rate:        outstanding > 0 ? Math.round(overdue / outstanding * 10000) / 100 : 0
      };
    })});
  } catch (err) {
    console.error('[officers]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ─── /api/nova/products ───────────────────────────────────────────────────────
// FIXED: branch via m_client (was using ml.office_id)
app.get('/api/nova/products', async (req, res) => {
  try {
    const p   = req.query.period || 'month';
    const bId = req.query.branch !== 'all' ? req.query.branch : null;
    const disbW = periodWhere('ml.disbursedon_date', p);
    const BF    = bf(bId);

    const rows = await runSQL(`
      SELECT mpl.id   AS product_id,
             mpl.name AS product_name,
             COUNT(*)  AS loan_count,
             COALESCE(SUM(ml.principal_disbursed_derived), 0) AS total_amount,
             COUNT(CASE WHEN ml.loan_status_id = 300 THEN 1 END) AS active_count,
             COALESCE(SUM(CASE WHEN ml.loan_status_id = 300 THEN ml.principal_outstanding_derived ELSE 0 END), 0) AS outstanding_amount
      FROM m_loan ml
      JOIN m_client mc ON mc.id = ml.client_id
      JOIN m_product_loan mpl ON mpl.id = ml.product_id
      WHERE ${disbW} AND ml.loan_status_id IN (300,500,602) ${BF}
      GROUP BY mpl.id, mpl.name
      ORDER BY total_amount DESC
    `);

    res.json({ success: true, period: p, data: rows.map(r => ({
      productId:         r.product_id,
      productName:       r.product_name,
      loanCount:         parseInt(r.loan_count)||0,
      totalAmount:       parseFloat(r.total_amount)||0,
      activeCount:       parseInt(r.active_count)||0,
      outstandingAmount: parseFloat(r.outstanding_amount)||0
    }))});
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
    const bId = req.query.branch !== 'all' ? req.query.branch : null;
    const BF  = bf(bId);

    const rows = await runSQL(`
      SELECT
        SUM(CASE WHEN days_in_arrears BETWEEN 1  AND 30  THEN 1 ELSE 0 END) AS count_1_30,
        SUM(CASE WHEN days_in_arrears BETWEEN 31 AND 60  THEN 1 ELSE 0 END) AS count_31_60,
        SUM(CASE WHEN days_in_arrears BETWEEN 61 AND 90  THEN 1 ELSE 0 END) AS count_61_90,
        SUM(CASE WHEN days_in_arrears > 90               THEN 1 ELSE 0 END) AS count_90plus,
        COALESCE(SUM(CASE WHEN days_in_arrears BETWEEN 1  AND 30  THEN principal_overdue ELSE 0 END),0) AS amt_1_30,
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
      { label: '1–30 Days',  count: int(rows,'count_1_30'),   amount: num(rows,'amt_1_30')   },
      { label: '31–60 Days', count: int(rows,'count_31_60'),  amount: num(rows,'amt_31_60')  },
      { label: '61–90 Days', count: int(rows,'count_61_90'),  amount: num(rows,'amt_61_90')  },
      { label: '90+ Days',   count: int(rows,'count_90plus'), amount: num(rows,'amt_90plus') }
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
    const bId = req.query.branch !== 'all' ? req.query.branch : null;
    const schedW = periodWhere('rs.duedate', p);
    const colW   = periodWhere('mlt.created_date', p);
    const BF     = bf(bId);
    const BFTXN  = bfTxn(bId);

    const [targetRows, colRows] = await Promise.all([
      // Scheduled amount due in period
      runSQL(`SELECT COALESCE(SUM(
                rs.principal_amount
                + COALESCE(rs.interest_amount, 0)
                + COALESCE(rs.fee_charges_amount, 0)), 0) AS total_due
              FROM m_loan_repayment_schedule rs
              JOIN m_loan ml ON ml.id = rs.loan_id
              JOIN m_client mc ON mc.id = ml.client_id
              WHERE ml.loan_status_id IN (300,500,602) AND ${schedW} ${BF}`),

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

// ─── /api/nova/retention ─────────────────────────────────────────────────────
// NEW: Repeat-borrower rate.  eligibleClients = all who ever had a loan.
//      retainedClients = those with 2+ loans (any status).
app.get('/api/nova/retention', async (req, res) => {
  try {
    const bId = req.query.branch !== 'all' ? req.query.branch : null;
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

app.use((_req, res) => res.status(404).json({ success: false, error: 'Not found' }));

app.listen(PORT, () => {
  console.log(`Nova Dashboard  →  http://localhost:${PORT}`);
  console.log(`Metabase URL    →  ${METABASE_URL}`);
  console.log(`DB ID           →  ${NOVA_DB_ID}`);
});
