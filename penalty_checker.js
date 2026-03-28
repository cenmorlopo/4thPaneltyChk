const axios = require('axios');
const cheerio = require('cheerio');
const fs = require('fs');
const path = require('path');

const FILES = {
  input: path.join(__dirname, 'result_students.txt'),
  creditMaster: path.join(__dirname, 'credit_master.txt'),
  creditTotals: path.join(__dirname, 'credit_totals.txt'),
  out: path.join(__dirname, 'penalty_calc_output.txt'),
  audit: path.join(__dirname, 'penalty_audit.txt'),
  absent: path.join(__dirname, 'penalty_absent.txt'),
  manual: path.join(__dirname, 'penalty_manual_review.txt'),
  failed: path.join(__dirname, 'penalty_failed.txt'),
  log: path.join(__dirname, 'penalty_log.txt'),
  state: path.join(__dirname, 'penalty_state.json'),
  seen: path.join(__dirname, 'penalty_seen.txt')
};

const CONFIG = {
  currentResult: {
    year: '2025',
    semester: 'IV',
    examHeld: 'December/2025'
  },
  oldHtmlBase: 'http://results.beup.ac.in/ResultsBTech4thSem2024_B2022Pub.aspx',
  timeoutMs: 30000,
  maxRetries: 3,
  retryDelayMs: 1500,
  politeDelayMs: 900,
  maxRuntimeMs: 5 * 60 * 60 * 1000,
  sgpaDecimals: 2,
  cgpaDecimals: 2
};

const SEM_ORDER = ['I', 'II', 'III', 'IV'];

const GRADE_POINTS = {
  'A+': 10,
  'A': 9,
  'B': 8,
  'C': 7,
  'D': 6,
  'P': 5,
  'F': 0
};

function ensureFile(filePath, defaultContent = '') {
  if (!fs.existsSync(filePath)) {
    fs.writeFileSync(filePath, defaultContent, 'utf8');
  }
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function log(message) {
  const line = `[${new Date().toISOString()}] ${message}`;
  console.log(line);
  fs.appendFileSync(FILES.log, line + '\n', 'utf8');
}

function normalize(v) {
  return String(v || '').trim();
}

function normalizeGrade(v) {
  return String(v || '')
    .trim()
    .toUpperCase()
    .replace(/\*/g, '');
}

function parseNumber(raw) {
  const cleaned = String(raw || '').replace(/[^\d.]/g, '');
  const n = parseFloat(cleaned);
  return Number.isFinite(n) ? n : null;
}

function hasGrace(raw) {
  return String(raw || '').includes('*');
}

function isAbsent(raw) {
  return /\bAB\b/i.test(String(raw || '').trim());
}

function roundTo(value, decimals) {
  if (!Number.isFinite(value)) return null;
  return Number(value.toFixed(decimals));
}

function safeJoin(arr) {
  return arr.map(x => normalize(x)).join(',');
}

function parseRegNo(regNo) {
  const r = normalize(regNo);
  return {
    reg_no: r,
    admission_year: r.slice(0, 2),
    branch_code: r.slice(2, 5),
    college_code: r.slice(5, 8),
    roll_no: r.slice(8)
  };
}

function buildCurrentBackendUrl(regNo) {
  const u = new URL('https://beu-bih.ac.in/backend/v1/result/get-result');
  u.searchParams.set('year', CONFIG.currentResult.year);
  u.searchParams.set('redg_no', regNo);
  u.searchParams.set('semester', CONFIG.currentResult.semester);
  u.searchParams.set('exam_held', CONFIG.currentResult.examHeld);
  return u.toString();
}

function buildOldResultUrl(regNo) {
  return `${CONFIG.oldHtmlBase}?Sem=IV&RegNo=${regNo}`;
}

function loadState() {
  ensureFile(FILES.state, JSON.stringify({ lineIndex: 0 }, null, 2));
  try {
    return JSON.parse(fs.readFileSync(FILES.state, 'utf8'));
  } catch {
    return { lineIndex: 0 };
  }
}

function saveState(lineIndex) {
  fs.writeFileSync(
    FILES.state,
    JSON.stringify({ lineIndex, updatedAt: new Date().toISOString() }, null, 2),
    'utf8'
  );
}

function loadSet(filePath) {
  ensureFile(filePath, '');
  return new Set(
    fs.readFileSync(filePath, 'utf8')
      .split(/\r?\n/)
      .map(x => x.trim())
      .filter(Boolean)
  );
}

function appendUnique(filePath, setObj, value) {
  if (!setObj.has(value)) {
    fs.appendFileSync(filePath, value + '\n', 'utf8');
    setObj.add(value);
  }
}

function initOutputs() {
  ensureFile(FILES.log, '');
  ensureFile(FILES.failed, 'reg_no | reason | detail | old_result_url | new_result_url\n');
  ensureFile(FILES.absent, 'reg_no | branch_code | subject_codes | subject_names | reason | old_result_url | new_result_url\n');
  ensureFile(FILES.manual, 'reg_no | branch_code | subject_codes | subject_names | reason | old_result_url | new_result_url\n');
  ensureFile(
    FILES.audit,
    'reg_no | sem | branch_code | subject_code | subject_name | subject_type | credit | old_shown_grade | new_shown_grade | should_be_grade | shown_gp | corrected_gp | delta_points | new_ese | new_ia | new_total | old_result_url | new_result_url\n'
  );
  ensureFile(
    FILES.out,
    'reg_no | branch_code | penalized_subject_codes | subject_names | old_shown_grades | new_shown_grades | should_be_grades | shown_sgpa | corrected_sgpa | shown_cgpa | corrected_cgpa | status | old_result_url | new_result_url\n'
  );
  ensureFile(FILES.seen, '');
}

function loadCreditMaster() {
  if (!fs.existsSync(FILES.creditMaster)) {
    throw new Error(`Missing credit master file: ${FILES.creditMaster}`);
  }

  const map = new Map();
  const lines = fs.readFileSync(FILES.creditMaster, 'utf8')
    .split(/\r?\n/)
    .map(x => x.trim())
    .filter(Boolean)
    .filter(line => !/^sem\s*\|/i.test(line));

  for (const line of lines) {
    const p = line.split('|').map(x => x.trim());
    if (p.length < 5) continue;

    const sem = p[0];
    const branchCode = p[1];
    const branchName = p[2];
    const subjectCode = p[3];
    const credit = parseNumber(p[4]);

    if (!Number.isFinite(credit)) continue;

    map.set(`${sem}|${branchCode}|${subjectCode}`, {
      sem,
      branchCode,
      branchName,
      subjectCode,
      credit
    });
  }

  return map;
}

function loadCreditTotals() {
  if (!fs.existsSync(FILES.creditTotals)) {
    throw new Error(`Missing credit totals file: ${FILES.creditTotals}`);
  }

  const map = new Map();
  const lines = fs.readFileSync(FILES.creditTotals, 'utf8')
    .split(/\r?\n/)
    .map(x => x.trim())
    .filter(Boolean)
    .filter(line => !/^sem\s*\|/i.test(line));

  for (const line of lines) {
    const p = line.split('|').map(x => x.trim());
    if (p.length < 4) continue;

    const sem = p[0];
    const branchCode = p[1];
    const branchName = p[2];
    const totalSemCredit = parseNumber(p[3]);

    if (!Number.isFinite(totalSemCredit)) continue;

    map.set(`${sem}|${branchCode}`, {
      sem,
      branchCode,
      branchName,
      totalSemCredit
    });
  }

  return map;
}

function loadInputGrouped() {
  if (!fs.existsSync(FILES.input)) {
    throw new Error(`Missing input file: ${FILES.input}`);
  }

  const grouped = new Map();
  const lines = fs.readFileSync(FILES.input, 'utf8')
    .split(/\r?\n/)
    .map(x => x.trim())
    .filter(Boolean)
    .filter(line => !/^reg_no\s*\|/i.test(line));

  for (const line of lines) {
    const p = line.split('|').map(x => x.trim());
    if (p.length < 2) continue;

    const regNo = p[0];
    const codes = p[1].split(',').map(x => x.trim()).filter(Boolean);
    const names = (p[2] || '').split(',').map(x => x.trim());

    if (!grouped.has(regNo)) {
      grouped.set(regNo, {
        reg_no: regNo,
        branch_code: parseRegNo(regNo).branch_code,
        backCodeSet: new Set(),
        inputNameMap: new Map()
      });
    }

    const g = grouped.get(regNo);

    codes.forEach((code, idx) => {
      g.backCodeSet.add(code);
      if (names[idx]) {
        g.inputNameMap.set(code, names[idx]);
      }
    });
  }

  return Array.from(grouped.values())
    .map(g => ({
      reg_no: g.reg_no,
      branch_code: g.branch_code,
      back_codes: Array.from(g.backCodeSet).sort(),
      input_name_map: g.inputNameMap
    }))
    .sort((a, b) => a.reg_no.localeCompare(b.reg_no));
}

async function fetchWithRetries(url, expectJson = false) {
  let delay = CONFIG.retryDelayMs;

  for (let attempt = 1; attempt <= CONFIG.maxRetries; attempt++) {
    try {
      const response = await axios.get(url, {
        timeout: CONFIG.timeoutMs,
        headers: {
          'User-Agent': 'Mozilla/5.0',
          'Accept': expectJson ? 'application/json, text/plain, */*' : 'text/html,application/xhtml+xml'
        },
        validateStatus: status => status >= 200 && status < 500
      });

      if (expectJson) {
        return {
          kind: 'HTTP',
          status: response.status,
          data: response.data
        };
      }

      if (response.status === 200 && typeof response.data === 'string') {
        const html = response.data;
        if (/No\s*Record\s*Found\s*!{0,3}/i.test(html)) {
          return { kind: 'NO_RECORD' };
        }
        return { kind: 'FOUND', html };
      }

      return { kind: 'NO_RECORD' };
    } catch (error) {
      if (attempt === CONFIG.maxRetries) {
        return { kind: 'ERROR', error: error.message };
      }
      await sleep(delay);
      delay *= 2;
    }
  }

  return { kind: 'ERROR', error: 'Unknown fetch error' };
}

function parseOldHtml(html) {
  const $ = cheerio.load(html);
  const theory = new Map();
  const practical = new Map();

  $('#ContentPlaceHolder1_GridView1 tr').slice(1).each((_, el) => {
    const cells = $(el).find('td');
    if (cells.length >= 7) {
      const code = normalize($(cells[0]).text());
      theory.set(code, {
        code,
        name: normalize($(cells[1]).text()),
        ese: normalize($(cells[2]).text()),
        ia: normalize($(cells[3]).text()),
        total: normalize($(cells[4]).text()),
        grade: normalizeGrade($(cells[5]).text()),
        credit: normalize($(cells[6]).text()),
        subject_type: 'theory'
      });
    }
  });

  $('#ContentPlaceHolder1_GridView2 tr').slice(1).each((_, el) => {
    const cells = $(el).find('td');
    if (cells.length >= 7) {
      const code = normalize($(cells[0]).text());
      practical.set(code, {
        code,
        name: normalize($(cells[1]).text()),
        ese: normalize($(cells[2]).text()),
        ia: normalize($(cells[3]).text()),
        total: normalize($(cells[4]).text()),
        grade: normalizeGrade($(cells[5]).text()),
        credit: normalize($(cells[6]).text()),
        subject_type: 'practical'
      });
    }
  });

  return { theory, practical };
}

function parseNewJson(payload) {
  if (!payload || payload.status !== 200 || !payload.data) {
    return null;
  }

  const d = payload.data;
  const theory = new Map();
  const practical = new Map();

  for (const s of d.theorySubjects || []) {
    theory.set(normalize(s.code), { ...s, subject_type: 'theory' });
  }

  for (const s of d.practicalSubjects || []) {
    practical.set(normalize(s.code), { ...s, subject_type: 'practical' });
  }

  return {
    reg_no: normalize(d.redg_no),
    student_name: normalize(d.name),
    branch_code: normalize(d.course_code),
    branch_name: normalize(d.course),
    cgpa: normalize(d.cgpa),
    sgpaList: Array.isArray(d.sgpa) ? d.sgpa.map(x => normalize(x)) : [],
    fail_any: normalize(d.fail_any),
    theory,
    practical
  };
}

function gradeFromPercent(percent) {
  if (percent >= 90) return 'A+';
  if (percent >= 80) return 'A';
  if (percent >= 70) return 'B';
  if (percent >= 60) return 'C';
  if (percent >= 50) return 'D';
  if (percent >= 35) return 'P';
  return 'F';
}

function oneStepLower(expected) {
  const map = {
    'A+': 'A',
    'A': 'B',
    'B': 'C',
    'C': 'D',
    'D': 'P'
  };
  return map[expected] || null;
}

function classifyNewSubject(newSubject, subjectType) {
  const eseRaw = normalize(newSubject.ese);
  const iaRaw = normalize(newSubject.ia);
  const totalRaw = normalize(newSubject.total);
  const actualGrade = normalizeGrade(newSubject.grade);

  if (isAbsent(eseRaw)) {
    return {
      subject_type: subjectType,
      ese_raw: eseRaw,
      ia_raw: iaRaw,
      total_raw: totalRaw,
      actual_grade: actualGrade || '-',
      expected_grade: '-',
      status: 'ABSENT_IN_CARRY',
      reason: 'External exam marked AB in current carry result'
    };
  }

  const eseNum = parseNumber(eseRaw);
  const totalNum = parseNumber(totalRaw);
  const grace = hasGrace(eseRaw);

  if (eseNum === null || totalNum === null || !actualGrade) {
    return {
      subject_type: subjectType,
      ese_raw: eseRaw,
      ia_raw: iaRaw,
      total_raw: totalRaw,
      actual_grade: actualGrade || '-',
      expected_grade: '-',
      status: 'MANUAL_REVIEW',
      reason: 'Malformed ESE/total/grade values in current result'
    };
  }

  const totalMax = subjectType === 'practical' ? 50 : 100;
  const totalPassMin = totalMax * 0.35;
  const esePassMin = subjectType === 'practical' ? 10.5 : 24.5;
  const thresholdPass = grace || eseNum >= esePassMin;

  let expected = 'F';

  if (totalNum < totalPassMin) {
    expected = 'F';
  } else if (!thresholdPass) {
    expected = 'F';
  } else {
    expected = gradeFromPercent((totalNum / totalMax) * 100);
  }

  if (expected === actualGrade) {
    return {
      subject_type: subjectType,
      ese_raw: eseRaw,
      ia_raw: iaRaw,
      total_raw: totalRaw,
      actual_grade: actualGrade,
      expected_grade: expected,
      status: expected === 'F' ? 'NO_PENALTY_STILL_FAILED' : 'NO_PENALTY',
      reason: 'Expected grade matches actual grade'
    };
  }

  if (expected !== 'F' && actualGrade === oneStepLower(expected)) {
    return {
      subject_type: subjectType,
      ese_raw: eseRaw,
      ia_raw: iaRaw,
      total_raw: totalRaw,
      actual_grade: actualGrade,
      expected_grade: expected,
      status: 'PENALTY_SUSPECTED',
      reason: `Expected ${expected} but published ${actualGrade}`
    };
  }

  return {
    subject_type: subjectType,
    ese_raw: eseRaw,
    ia_raw: iaRaw,
    total_raw: totalRaw,
    actual_grade: actualGrade,
    expected_grade: expected,
    status: 'MANUAL_REVIEW',
    reason: `Unexpected pattern: expected ${expected}, published ${actualGrade}`
  };
}

function getSubjectFromMaps(code, maps) {
  if (maps.theory.has(code)) return maps.theory.get(code);
  if (maps.practical.has(code)) return maps.practical.get(code);
  return null;
}

function buildCurrentSemRows(newParsed, branchCode, creditMaster, semKey) {
  const rows = [];

  for (const [, s] of newParsed.theory) {
    const code = normalize(s.code);
    const creditInfo = creditMaster.get(`${semKey}|${branchCode}|${code}`);
    const credit = creditInfo ? creditInfo.credit : parseNumber(s.credit);

    rows.push({
      subject_code: code,
      subject_type: 'theory',
      grade: normalizeGrade(s.grade),
      credit
    });
  }

  for (const [, s] of newParsed.practical) {
    const code = normalize(s.code);
    const creditInfo = creditMaster.get(`${semKey}|${branchCode}|${code}`);
    const credit = creditInfo ? creditInfo.credit : parseNumber(s.credit);

    rows.push({
      subject_code: code,
      subject_type: 'practical',
      grade: normalizeGrade(s.grade),
      credit
    });
  }

  return rows;
}

function computeShownSemPoints(rows) {
  let total = 0;

  for (const row of rows) {
    const gp = GRADE_POINTS[row.grade];
    if (Number.isFinite(gp) && Number.isFinite(row.credit)) {
      total += gp * row.credit;
    }
  }

  return total;
}

function appendLine(filePath, line) {
  fs.appendFileSync(filePath, line + '\n', 'utf8');
}

function formatMaybe(value, decimals = 2) {
  if (value === null || value === undefined || value === '') return '-';
  if (typeof value === 'number' && Number.isFinite(value)) return value.toFixed(decimals);
  return String(value);
}

function getDisplayedSgpa(newParsed, semKey) {
  const index = SEM_ORDER.indexOf(semKey);
  if (index === -1) return null;
  return parseNumber(newParsed.sgpaList[index]);
}

async function run() {
  initOutputs();

  const rows = loadInputGrouped();
  const creditMaster = loadCreditMaster();
  const creditTotals = loadCreditTotals();
  const state = loadState();
  const seen = loadSet(FILES.seen);
  const startedAt = Date.now();

  log(`Loaded ${rows.length} grouped students from ${path.basename(FILES.input)}`);
  log(`Resuming from lineIndex=${state.lineIndex}`);

  for (let i = state.lineIndex; i < rows.length; i++) {
    if (Date.now() - startedAt > CONFIG.maxRuntimeMs) {
      log('STOP max runtime reached');
      saveState(i);
      return;
    }

    const row = rows[i];
    const regNo = row.reg_no;

    if (seen.has(regNo)) {
      log(`[${i + 1}/${rows.length}] ${regNo} -> DUP_ALREADY_PROCESSED`);
      saveState(i + 1);
      continue;
    }

    const oldUrl = buildOldResultUrl(regNo);
    const newUrl = buildCurrentBackendUrl(regNo);

    const oldFetched = await fetchWithRetries(oldUrl, false);
    if (oldFetched.kind === 'ERROR') {
      appendLine(FILES.failed, `${regNo} | OLD_FETCH_ERROR | ${oldFetched.error} | ${oldUrl} | ${newUrl}`);
      log(`[${i + 1}/${rows.length}] ${regNo} -> OLD_FETCH_ERROR -> ${oldFetched.error}`);
      saveState(i + 1);
      await sleep(CONFIG.politeDelayMs);
      continue;
    }

    if (oldFetched.kind === 'NO_RECORD') {
      appendLine(FILES.failed, `${regNo} | OLD_NO_RECORD | old result not found | ${oldUrl} | ${newUrl}`);
      log(`[${i + 1}/${rows.length}] ${regNo} -> OLD_NO_RECORD`);
      saveState(i + 1);
      await sleep(CONFIG.politeDelayMs);
      continue;
    }

    const newFetched = await fetchWithRetries(newUrl, true);
    if (newFetched.kind === 'ERROR') {
      appendLine(FILES.failed, `${regNo} | NEW_FETCH_ERROR | ${newFetched.error} | ${oldUrl} | ${newUrl}`);
      log(`[${i + 1}/${rows.length}] ${regNo} -> NEW_FETCH_ERROR -> ${newFetched.error}`);
      saveState(i + 1);
      await sleep(CONFIG.politeDelayMs);
      continue;
    }

    if (newFetched.status !== 200 || !newFetched.data || newFetched.data.status !== 200 || !newFetched.data.data) {
      appendLine(FILES.failed, `${regNo} | NEW_NO_RESULT | backend result not found | ${oldUrl} | ${newUrl}`);
      log(`[${i + 1}/${rows.length}] ${regNo} -> NEW_NO_RESULT`);
      saveState(i + 1);
      await sleep(CONFIG.politeDelayMs);
      continue;
    }

    const oldParsed = parseOldHtml(oldFetched.html);
    const newParsed = parseNewJson(newFetched.data);

    if (!newParsed) {
      appendLine(FILES.failed, `${regNo} | NEW_PARSE_ERROR | invalid backend payload | ${oldUrl} | ${newUrl}`);
      log(`[${i + 1}/${rows.length}] ${regNo} -> NEW_PARSE_ERROR`);
      saveState(i + 1);
      await sleep(CONFIG.politeDelayMs);
      continue;
    }

    const branchCode = row.branch_code;
    const sem1TotalInfo = creditTotals.get(`I|${branchCode}`);
    const sem2TotalInfo = creditTotals.get(`II|${branchCode}`);
    const sem3TotalInfo = creditTotals.get(`III|${branchCode}`);
    const sem4TotalInfo = creditTotals.get(`IV|${branchCode}`);

    if (!sem1TotalInfo || !sem2TotalInfo || !sem3TotalInfo || !sem4TotalInfo) {
      appendLine(
        FILES.failed,
        `${regNo} | CREDIT_TOTAL_MISSING | missing one or more semester totals for branch ${branchCode} | ${oldUrl} | ${newUrl}`
      );
      log(`[${i + 1}/${rows.length}] ${regNo} -> CREDIT_TOTAL_MISSING`);
      saveState(i + 1);
      await sleep(CONFIG.politeDelayMs);
      continue;
    }

    const sem1ShownSgpa = getDisplayedSgpa(newParsed, 'I');
    const sem2ShownSgpa = getDisplayedSgpa(newParsed, 'II');
    const sem3ShownSgpa = getDisplayedSgpa(newParsed, 'III');
    const sem4ShownSgpa = getDisplayedSgpa(newParsed, 'IV');

    if (
      !Number.isFinite(sem1ShownSgpa) ||
      !Number.isFinite(sem2ShownSgpa) ||
      !Number.isFinite(sem3ShownSgpa)
    ) {
      appendLine(
        FILES.failed,
        `${regNo} | PREV_SGPA_MISSING | cannot compute corrected CGPA because semester I/II/III SGPA missing | ${oldUrl} | ${newUrl}`
      );
      log(`[${i + 1}/${rows.length}] ${regNo} -> PREV_SGPA_MISSING`);
      saveState(i + 1);
      await sleep(CONFIG.politeDelayMs);
      continue;
    }

    const sem4Rows = buildCurrentSemRows(newParsed, branchCode, creditMaster, 'IV');
    const shownSem4Points = computeShownSemPoints(sem4Rows);
    let correctedSem4Points = shownSem4Points;

    const shownSgpa = normalize(newParsed.sgpaList[3]);
    const shownCgpa = normalize(newParsed.cgpa);

    const penaltyItems = [];
    const absentItems = [];
    const manualItems = [];

    for (const code of row.back_codes) {
      const oldSubject = getSubjectFromMaps(code, oldParsed);
      const newSubject = getSubjectFromMaps(code, newParsed);
      const inputName = row.input_name_map.get(code) || '';

      if (!oldSubject) {
        manualItems.push({
          code,
          name: inputName || '-',
          reason: 'Backlog subject code not found in old result HTML'
        });
        continue;
      }

      if (!newSubject) {
        manualItems.push({
          code,
          name: oldSubject.name || inputName || '-',
          reason: 'Backlog subject code not found in current backend result'
        });
        continue;
      }

      const classified = classifyNewSubject(newSubject, newSubject.subject_type);
      const subjectName = normalize(newSubject.name) || normalize(oldSubject.name) || inputName || '-';
      const creditInfo = creditMaster.get(`IV|${branchCode}|${code}`);
      const credit = creditInfo ? creditInfo.credit : parseNumber(newSubject.credit);
      const shownGp = Number.isFinite(GRADE_POINTS[classified.actual_grade]) ? GRADE_POINTS[classified.actual_grade] : null;
      const correctedGp = Number.isFinite(GRADE_POINTS[classified.expected_grade]) ? GRADE_POINTS[classified.expected_grade] : null;

      if (classified.status === 'ABSENT_IN_CARRY') {
        absentItems.push({
          code,
          name: subjectName,
          reason: classified.reason
        });
        continue;
      }

      if (classified.status === 'MANUAL_REVIEW') {
        manualItems.push({
          code,
          name: subjectName,
          reason: classified.reason
        });
        continue;
      }

      if (classified.status === 'PENALTY_SUSPECTED') {
        const deltaPoints = Number.isFinite(credit) && correctedGp !== null && shownGp !== null
          ? (correctedGp - shownGp) * credit
          : null;

        if (deltaPoints !== null) {
          correctedSem4Points += deltaPoints;
        }

        penaltyItems.push({
          code,
          name: subjectName,
          old_grade: normalizeGrade(oldSubject.grade) || '-',
          new_grade: classified.actual_grade,
          should_grade: classified.expected_grade,
          credit,
          shown_gp: shownGp,
          corrected_gp: correctedGp,
          delta_points: deltaPoints,
          subject_type: newSubject.subject_type,
          new_ese: classified.ese_raw,
          new_ia: classified.ia_raw,
          new_total: classified.total_raw
        });

        appendLine(
          FILES.audit,
          [
            regNo,
            'IV',
            branchCode,
            code,
            subjectName,
            newSubject.subject_type,
            formatMaybe(credit),
            normalizeGrade(oldSubject.grade) || '-',
            classified.actual_grade,
            classified.expected_grade,
            shownGp === null ? '-' : shownGp,
            correctedGp === null ? '-' : correctedGp,
            deltaPoints === null ? '-' : formatMaybe(deltaPoints),
            classified.ese_raw,
            classified.ia_raw,
            classified.total_raw,
            oldUrl,
            newUrl
          ].join(' | ')
        );
      }
    }

    if (penaltyItems.length > 0) {
      const correctedSgpaVal = correctedSem4Points / sem4TotalInfo.totalSemCredit;

      const correctedCgpaVal =
        (
          (sem1ShownSgpa * sem1TotalInfo.totalSemCredit) +
          (sem2ShownSgpa * sem2TotalInfo.totalSemCredit) +
          (sem3ShownSgpa * sem3TotalInfo.totalSemCredit) +
          correctedSem4Points
        ) /
        (
          sem1TotalInfo.totalSemCredit +
          sem2TotalInfo.totalSemCredit +
          sem3TotalInfo.totalSemCredit +
          sem4TotalInfo.totalSemCredit
        );

      appendLine(
        FILES.out,
        [
          regNo,
          branchCode,
          safeJoin(penaltyItems.map(x => x.code)),
          safeJoin(penaltyItems.map(x => x.name)),
          safeJoin(penaltyItems.map(x => x.old_grade)),
          safeJoin(penaltyItems.map(x => x.new_grade)),
          safeJoin(penaltyItems.map(x => x.should_grade)),
          shownSgpa || (Number.isFinite(sem4ShownSgpa) ? formatMaybe(sem4ShownSgpa, CONFIG.sgpaDecimals) : '-'),
          formatMaybe(roundTo(correctedSgpaVal, CONFIG.sgpaDecimals), CONFIG.sgpaDecimals),
          shownCgpa || '-',
          formatMaybe(roundTo(correctedCgpaVal, CONFIG.cgpaDecimals), CONFIG.cgpaDecimals),
          'PENALTY_CONFIRMED',
          oldUrl,
          newUrl
        ].join(' | ')
      );
    }

    if (penaltyItems.length === 0 && absentItems.length > 0 && manualItems.length === 0) {
      appendLine(
        FILES.absent,
        [
          regNo,
          branchCode,
          safeJoin(absentItems.map(x => x.code)),
          safeJoin(absentItems.map(x => x.name)),
          safeJoin(absentItems.map(x => x.reason)),
          oldUrl,
          newUrl
        ].join(' | ')
      );
    }

    if (manualItems.length > 0) {
      appendLine(
        FILES.manual,
        [
          regNo,
          branchCode,
          safeJoin(manualItems.map(x => x.code)),
          safeJoin(manualItems.map(x => x.name)),
          safeJoin(manualItems.map(x => x.reason)),
          oldUrl,
          newUrl
        ].join(' | ')
      );
    }

    appendUnique(FILES.seen, seen, regNo);

    log(
      `[${i + 1}/${rows.length}] ${regNo} | penalty=${penaltyItems.length} | absent=${absentItems.length} | manual=${manualItems.length} -> ${newUrl}`
    );

    saveState(i + 1);
    await sleep(CONFIG.politeDelayMs);
  }

  log('COMPLETE all students finished');
}

run().catch(err => {
  log(`FATAL ${err.stack || err.message}`);
  process.exit(1);
});
