/**
 * NOVA MICROFINANCE — Cost Data API (Google Apps Script)
 * ─────────────────────────────────────────────────────
 * Sheet structure: ONE TAB PER MONTH, named "2026-03", "2026-04", etc.
 *
 * Each tab layout:
 *   Row 1:    STAFF COSTS section title (merged, teal)
 *   Row 2:    Staff column headers
 *   Row 3-22: Staff data (one row per officer, 20 rows pre-filled)
 *   Row 23:   blank separator
 *   Row 24:   BRANCH COSTS section title (merged, blue)
 *   Row 25:   Branch column headers
 *   Row 26-35:Branch data (one row per branch, 10 rows pre-filled)
 *
 * Staff columns (A→L):
 *   A:staff_id  B:officer_name  C:branch_id  D:branch_name
 *   E:salary  F:transport  G:airtime  H:insurance
 *   I:nssf  J:commission  K:other_direct  L:total_direct (formula)
 *
 * Branch columns (A→H):
 *   A:branch_id  B:branch_name  C:rent  D:utilities
 *   E:supplies  F:fixed_overhead_per_officer  G:other_overhead  H:total_overhead (formula)
 *
 * Deploy as Web App → Execute as: Me → Access: Anyone
 * Then set SHEETS_COSTS_URL env var on DigitalOcean to the Web App URL.
 *
 * One-time setup: run setupNext12Months() from the Apps Script editor.
 */

// ─── Script property: paste your Sheet ID in Script Properties ───────────────
const SHEET_ID = PropertiesService.getScriptProperties().getProperty('SHEET_ID');

// Column index constants (1-based for getRange, 0-based for array)
const SC = {
  staff_id:1, officer_name:2, branch_id:3, branch_name:4,
  salary:5, transport:6, airtime:7, insurance:8,
  nssf:9, commission:10, other_direct:11, total_direct:12
};
const BC = {
  branch_id:1, branch_name:2, rent:3, utilities:4,
  supplies:5, fixed_overhead_per_officer:6, other_overhead:7, total_overhead:8
};

// Fixed row positions in each tab
const STAFF_TITLE_ROW  = 1;
const STAFF_HEADER_ROW = 2;
const STAFF_DATA_START = 3;
const STAFF_DATA_ROWS  = 20;   // rows 3–22
const BRANCH_TITLE_ROW = 24;
const BRANCH_HEADER_ROW= 25;
const BRANCH_DATA_START= 26;
const BRANCH_DATA_ROWS = 10;   // rows 26–35

// ─── CORS / response helpers ──────────────────────────────────────────────────
function cors(out) {
  return out
    .setHeader('Access-Control-Allow-Origin', '*')
    .setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
    .setHeader('Access-Control-Allow-Headers', 'Content-Type');
}
function jsonOut(obj) {
  return cors(ContentService
    .createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON));
}

// ─── Router ───────────────────────────────────────────────────────────────────
function doGet(e) {
  try {
    const action = (e.parameter.action || 'costs');
    const month  = (e.parameter.month  || currentMonth());
    if (action === 'staff')  return jsonOut({ success:true, month, data: readStaff(month) });
    if (action === 'branch') return jsonOut({ success:true, month, data: readBranch(month) });
    if (action === 'months') return jsonOut({ success:true, data: listMonths() });
    // default: both
    return jsonOut({ success:true, month, data:{ staff:readStaff(month), branch:readBranch(month) } });
  } catch(err) {
    return jsonOut({ success:false, error:err.message });
  }
}

function doPost(e) {
  try {
    const body = JSON.parse(e.postData.contents);
    if (body.action === 'upsert_staff')  return jsonOut(upsertStaff(body));
    if (body.action === 'upsert_branch') return jsonOut(upsertBranch(body));
    if (body.action === 'setup_month')   return jsonOut({ success:true, message: setupTab(body.month) });
    return jsonOut({ success:false, error:'Unknown action: ' + body.action });
  } catch(err) {
    return jsonOut({ success:false, error:err.message });
  }
}

// ─── READ staff costs ─────────────────────────────────────────────────────────
function readStaff(month) {
  const sheet = getSheet(month, false);
  if (!sheet) return [];
  const vals = sheet.getRange(STAFF_DATA_START, 1, STAFF_DATA_ROWS, 12).getValues();
  return vals
    .filter(r => r[0] !== '')           // skip blank rows
    .map(r => ({
      staff_id:     r[SC.staff_id-1],
      officer_name: r[SC.officer_name-1],
      branch_id:    r[SC.branch_id-1],
      branch_name:  r[SC.branch_name-1],
      month,
      salary:       n(r[SC.salary-1]),
      transport:    n(r[SC.transport-1]),
      airtime:      n(r[SC.airtime-1]),
      insurance:    n(r[SC.insurance-1]),
      nssf:         n(r[SC.nssf-1]),
      commission:   n(r[SC.commission-1]),
      other_direct: n(r[SC.other_direct-1]),
      total_direct: n(r[SC.salary-1])+n(r[SC.transport-1])+n(r[SC.airtime-1])+
                    n(r[SC.insurance-1])+n(r[SC.nssf-1])+n(r[SC.commission-1])+
                    n(r[SC.other_direct-1])
    }));
}

// ─── READ branch costs ────────────────────────────────────────────────────────
function readBranch(month) {
  const sheet = getSheet(month, false);
  if (!sheet) return [];
  const vals = sheet.getRange(BRANCH_DATA_START, 1, BRANCH_DATA_ROWS, 8).getValues();
  return vals
    .filter(r => r[0] !== '')
    .map(r => ({
      branch_id:                  r[BC.branch_id-1],
      branch_name:                r[BC.branch_name-1],
      month,
      rent:                       n(r[BC.rent-1]),
      utilities:                  n(r[BC.utilities-1]),
      supplies:                   n(r[BC.supplies-1]),
      fixed_overhead_per_officer: n(r[BC.fixed_overhead_per_officer-1]),
      other_overhead:             n(r[BC.other_overhead-1]),
      total_branch_overhead:      n(r[BC.rent-1])+n(r[BC.utilities-1])+
                                  n(r[BC.supplies-1])+n(r[BC.other_overhead-1])
    }));
}

// ─── UPSERT staff row ─────────────────────────────────────────────────────────
function upsertStaff(body) {
  const month = body.month || currentMonth();
  const sheet = getSheet(month, true);
  const staffId = String(body.staff_id);

  // Find existing row
  const vals = sheet.getRange(STAFF_DATA_START, 1, STAFF_DATA_ROWS, 1).getValues();
  let targetRow = -1;
  for (let i = 0; i < vals.length; i++) {
    if (String(vals[i][0]) === staffId) { targetRow = STAFF_DATA_START + i; break; }
  }
  // If not found, use next blank row
  if (targetRow < 0) {
    for (let i = 0; i < vals.length; i++) {
      if (vals[i][0] === '') { targetRow = STAFF_DATA_START + i; break; }
    }
  }
  if (targetRow < 0) return { success:false, error:'Staff data area full — add more rows to tab ' + month };

  const row = [
    body.staff_id, body.officer_name||'', body.branch_id||'', body.branch_name||'',
    n(body.salary), n(body.transport), n(body.airtime), n(body.insurance),
    n(body.nssf), n(body.commission), n(body.other_direct),
    n(body.salary)+n(body.transport)+n(body.airtime)+n(body.insurance)+
      n(body.nssf)+n(body.commission)+n(body.other_direct)
  ];
  sheet.getRange(targetRow, 1, 1, row.length).setValues([row]);
  return { success:true, action: vals.some(r=>String(r[0])===staffId)?'updated':'inserted', month, staff_id:staffId };
}

// ─── UPSERT branch row ────────────────────────────────────────────────────────
function upsertBranch(body) {
  const month = body.month || currentMonth();
  const sheet = getSheet(month, true);
  const branchId = String(body.branch_id);

  const vals = sheet.getRange(BRANCH_DATA_START, 1, BRANCH_DATA_ROWS, 1).getValues();
  let targetRow = -1;
  for (let i = 0; i < vals.length; i++) {
    if (String(vals[i][0]) === branchId) { targetRow = BRANCH_DATA_START + i; break; }
  }
  if (targetRow < 0) {
    for (let i = 0; i < vals.length; i++) {
      if (vals[i][0] === '') { targetRow = BRANCH_DATA_START + i; break; }
    }
  }
  if (targetRow < 0) return { success:false, error:'Branch data area full — add more rows to tab ' + month };

  const row = [
    body.branch_id, body.branch_name||'',
    n(body.rent), n(body.utilities), n(body.supplies),
    n(body.fixed_overhead_per_officer), n(body.other_overhead),
    n(body.rent)+n(body.utilities)+n(body.supplies)+n(body.other_overhead)
  ];
  sheet.getRange(targetRow, 1, 1, row.length).setValues([row]);
  return { success:true, action: vals.some(r=>String(r[0])===branchId)?'updated':'inserted', month, branch_id:branchId };
}

// ─── Tab scaffolding ──────────────────────────────────────────────────────────
function setupTab(month) {
  const label = month || currentMonth();
  const ss    = SpreadsheetApp.openById(SHEET_ID);

  // Remove existing tab if any
  const existing = ss.getSheetByName(label);
  if (existing) ss.deleteSheet(existing);

  const sheet = ss.insertSheet(label);
  const TEAL  = '#10b981', BLUE = '#3b82f6', WHITE = '#ffffff';

  // ── STAFF section ──────────────────────────────────────────────────────────
  sheet.getRange(STAFF_TITLE_ROW, 1, 1, 12).merge()
    .setValue(`STAFF COSTS — ${label}  |  All amounts in UGX  |  One row per loan officer`)
    .setBackground(TEAL).setFontColor(WHITE).setFontWeight('bold').setFontSize(10);

  sheet.getRange(STAFF_HEADER_ROW, 1, 1, 12).setValues([[
    'staff_id','officer_name','branch_id','branch_name',
    'salary','transport','airtime','insurance',
    'nssf','commission','other_direct','total_direct'
  ]]).setBackground('#d1fae5').setFontWeight('bold').setFontSize(9).setFontColor('#065f46');

  // Data rows with total formula
  for (let r = STAFF_DATA_START; r < STAFF_DATA_START + STAFF_DATA_ROWS; r++) {
    sheet.getRange(r, SC.total_direct)
      .setFormula(`=IF(A${r}="","",SUM(E${r}:K${r}))`);
    sheet.getRange(r, 1, 1, 11).setNumberFormat('#,##0');
    sheet.getRange(r, SC.total_direct).setNumberFormat('#,##0').setFontWeight('bold').setBackground('#f0fdf4');
  }

  // ── BRANCH section ─────────────────────────────────────────────────────────
  sheet.getRange(BRANCH_TITLE_ROW, 1, 1, 8).merge()
    .setValue(`BRANCH COSTS — ${label}  |  One row per branch  |  fixed_overhead_per_officer = management-set fixed allocation`)
    .setBackground(BLUE).setFontColor(WHITE).setFontWeight('bold').setFontSize(10);

  sheet.getRange(BRANCH_HEADER_ROW, 1, 1, 8).setValues([[
    'branch_id','branch_name','rent','utilities',
    'supplies','fixed_overhead_per_officer','other_overhead','total_overhead'
  ]]).setBackground('#dbeafe').setFontWeight('bold').setFontSize(9).setFontColor('#1e3a8a');

  for (let r = BRANCH_DATA_START; r < BRANCH_DATA_START + BRANCH_DATA_ROWS; r++) {
    sheet.getRange(r, BC.total_overhead)
      .setFormula(`=IF(A${r}="","",SUM(C${r}:G${r}))`);
    sheet.getRange(r, 1, 1, 7).setNumberFormat('#,##0');
    sheet.getRange(r, BC.total_overhead).setNumberFormat('#,##0').setFontWeight('bold').setBackground('#eff6ff');
  }

  // ── Column widths ──────────────────────────────────────────────────────────
  sheet.setFrozenRows(STAFF_HEADER_ROW);
  sheet.setColumnWidth(1, 90);    // id
  sheet.setColumnWidth(2, 170);   // name
  sheet.setColumnWidth(3, 80);    // branch_id
  sheet.setColumnWidth(4, 140);   // branch_name
  sheet.setColumnWidths(5, 8, 115); // cost columns
  sheet.setColumnWidth(12, 120);  // total_direct (wider, bold)

  // ── Footer note ────────────────────────────────────────────────────────────
  sheet.getRange(BRANCH_DATA_START + BRANCH_DATA_ROWS + 1, 1, 1, 8).merge()
    .setValue(`Nova Microfinance · ${label} · Do NOT rename this tab · currency: UGX`)
    .setFontColor('#94a3b8').setFontSize(8).setFontStyle('italic');

  // Move to front
  ss.moveActiveSheet(1);

  return `Tab "${label}" created`;
}

// ─── Utilities ────────────────────────────────────────────────────────────────
function getSheet(month, createIfMissing) {
  const ss    = SpreadsheetApp.openById(SHEET_ID);
  let sheet   = ss.getSheetByName(month);
  if (!sheet && createIfMissing) { setupTab(month); sheet = ss.getSheetByName(month); }
  return sheet || null;
}
function listMonths() {
  return SpreadsheetApp.openById(SHEET_ID).getSheets()
    .map(s => s.getName()).filter(n => /^\d{4}-\d{2}$/.test(n)).sort().reverse();
}
function n(v) { return parseFloat(v) || 0; }
function currentMonth() {
  return Utilities.formatDate(new Date(), 'Africa/Nairobi', 'yyyy-MM');
}

// ─── Run once from editor: scaffold next 12 months ────────────────────────────
function setupNext12Months() {
  const now = new Date();
  for (let i = 0; i < 12; i++) {
    const d = new Date(now.getFullYear(), now.getMonth() + i, 1);
    setupTab(Utilities.formatDate(d, 'Africa/Nairobi', 'yyyy-MM'));
    Utilities.sleep(800);
  }
  Logger.log('Done — 12 month tabs created');
}
