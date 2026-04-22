// Google Apps Script prototype for a Sheets sidebar that evaluates call transcripts
// from an uploaded CSV sent directly from the sidebar.
//
// Key updates:
// - Reduced storage-heavy progress updates
// - Bilingual English/Spanish optimization
// - Inbound: do NOT evaluate C5 (Mini Miranda), C6 (recording disclosure), C10 (UDAAP)
// - Outbound: do NOT evaluate C13 (UDAAP)
// - Settlement disclosure only applies when settlement is actually accepted/applied in the transcript
// - Outbound recording disclosure may occur anytime early in the call
// - Outbound Mini Miranda must occur after borrower authentication
// - Borrower authentication = first/last name + 2 pieces of PII (last 4 SSN, DOB, or physical address)
// - Simplified disclosure checks:
//   * Inbound C29 is satisfied leniently if agent says their name followed by a number
//   * Outbound badge number satisfied if transcript mentions agent/badge number like "53"
//   * Mini Miranda satisfied if transcript mentions "debt collector" (or Spanish equivalent)
//   * Recording disclosure satisfied more leniently by any language including "recorded" or "recording"
// - If transcript appears cut off, later criteria are set to N/A
// - Errors note transcript cut off
// - Outbound C12 is satisfied if transcript mentions "January" or "January Technologies"
// - Scorecard links point to sheets containing all calls for each FIRST_NAME in the run
// - Scorecard sheets also include all Verdict and Evidence columns
//
// STATUS-based additions:
// - CSV must include STATUS column
// - C2 debt details are conditional on STATUS:
//   * If STATUS='canceled', do not require last 4, full debt description, or balance
//   * Improve language recognition so general debt descriptions like "préstamo personal" count
// - Inbound C9 / Outbound C8 OOS disclosure only evaluated if STATUS='placed' and OOS applies
// - Inbound C26 / Outbound C27 recap:
//   * Leniently evaluate recap for STATUS='canceled'; no full debt summary required
//
// Additional updates in this version:
// - Do NOT evaluate/consider:
//   * Inbound C21, C22, C25
//   * Outbound C28
// - If transcript appears cut off, continue noting it in errors but do NOT score the call
// - C11/C14 email-only contact confirmation applies only for DSC/third-party scenarios

const OUTPUT_SHEET_NAME = 'QA Evaluations';
const PROGRESS_KEY = 'JAN_QA_PROGRESS';
const DEFAULT_MODEL = 'claude-sonnet-4-6';
const ROW_BATCH_WRITE_SIZE = 10;
const SLEEP_MS_BETWEEN_ROWS = 150;
const SCORECARD_FOLDER_ID = '1VV7lo0p0qLUgIdwWgqdZ57k-co8W0vXE';

const PROGRESS_WRITE_EVERY_N_ROWS = 5;
const PROGRESS_WRITE_MIN_INTERVAL_MS = 4000;

const STIPULATION_COMPANIES = [
  'Baxter Credit Union',
  'Randolph Brooks Federal Credit Union',
  'Flexible Finance Inc.'
];

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('January QA')
    .addItem('Open Evaluator', 'showSidebar')
    .addToUi();
}

function showSidebar() {
  const html = HtmlService.createHtmlOutputFromFile('Sidebar')
    .setTitle('Transcript Evaluator');
  SpreadsheetApp.getUi().showSidebar(html);
}

function getProgress() {
  const props = PropertiesService.getDocumentProperties();
  const raw = props.getProperty(PROGRESS_KEY);
  if (!raw) {
    return {
      status: 'idle',
      total: 0,
      processed: 0,
      written: 0,
      skipped: 0,
      percent: 0,
      message: 'Ready.',
      updatedAt: ''
    };
  }

  const progress = JSON.parse(raw);
  progress.percent = progress.total
    ? Math.floor((progress.processed / progress.total) * 100)
    : 0;
  return progress;
}

function runEvaluations(options) {
  options = options || {};
  const limit = Number(options.limit || 0);
  const outputSheetName = String(options.outputSheetName || OUTPUT_SHEET_NAME).trim() || OUTPUT_SHEET_NAME;

  if (!options.fileName || !options.base64Data) {
    throw new Error('No CSV was provided. Load a CSV first.');
  }

  const bytes = Utilities.base64Decode(options.base64Data);
  let text = Utilities.newBlob(bytes).getDataAsString('UTF-8');
  text = text.replace(/^\uFEFF/, '');

  const rows = Utilities.parseCsv(text);
  if (!rows || rows.length < 2) {
    throw new Error('The CSV appears to be empty or missing data rows.');
  }

  const headers = rows[0].map(String);
  const idx = indexMap_(headers);
  validateHeaders_(idx);

  const eligibleIndexes = [];
  for (let i = 1; i < rows.length; i++) {
    const row = rows[i];
    const direction = normalizeDirection_(row[idx.DIRECTION]);
    const transcript = String(row[idx.TRANSCRIPT] || '').trim();
    if (isVoicemailTranscript_(transcript)) continue;
    if ((direction === 'INBOUND' || direction === 'OUTBOUND') && transcript) {
      eligibleIndexes.push(i);
    }
  }

  const total = limit > 0 ? Math.min(limit, eligibleIndexes.length) : eligibleIndexes.length;
  const ss = SpreadsheetApp.getActive();
  const outputSheet = ensureOutputSheet_(ss, outputSheetName);
  const today = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyy-MM-dd');

  const errors = [];
  let processed = 0;
  let written = 0;
  let skipped = 0;
  let pendingRows = [];
  const scorecardGroups = {};
  let progressState = createProgressState_();

  setProgressThrottled_(progressState, {
    force: true,
    status: 'running',
    fileName: options.fileName,
    total: total,
    processed: 0,
    written: 0,
    skipped: 0,
    errors: [],
    message: 'Starting evaluation...'
  });

  for (let k = 0; k < total; k++) {
    const csvRowIndex = eligibleIndexes[k];
    const row = rows[csvRowIndex];

    try {
      const rowCtx = buildRowContext_(row, idx);
      const prompt = buildEvaluationPrompt_(rowCtx);
      const responseText = callClaudeSingle_(prompt);
      const parsed = parseModelJsonSafe_(responseText, rowCtx);
      const normalized = normalizeEvaluationsForRow_(parsed, rowCtx);
      const cltParsed = parseClt_(row[idx.CLT]);

      const firstName = idx.FIRST_NAME !== undefined ? String(row[idx.FIRST_NAME] || '').trim() : '';
      const scorecardKey = firstName && firstName.toLowerCase() !== 'unknown' ? firstName : '';

      const outputRow = buildWideOutputRow_(normalized, {
        uuid: row[idx.UUID],
        firstName: firstName,
        agentUuid: idx.AGENT_UUID !== undefined ? row[idx.AGENT_UUID] : '',
        direction: rowCtx.direction,
        surveyDate: today,
        callDate: cltParsed.date,
        callTime: cltParsed.time,
        language: rowCtx.language,
        accountNumber: idx.ACCOUNT_NUMBER !== undefined ? row[idx.ACCOUNT_NUMBER] : '',
        apPortal: idx['AP Portal'] !== undefined ? row[idx['AP Portal']] : (idx.AP_PORTAL !== undefined ? row[idx.AP_PORTAL] : ''),
        commChannel: row[idx.COMM_CHANNEL],
        createdAt: row[idx.CREATED_AT],
        callCommUuid: row[idx.CALL_COMM_UUID],
        originalCreditor: row[idx.ORIGINAL_CREDITOR],
        companyName: row[idx.COMPANY_NAME],
        addressState: rowCtx.addressState,
        scorecard: scorecardKey ? `PENDING:${scorecardKey}` : ''
      });

      pendingRows.push(outputRow);
      written++;

      if (scorecardKey) {
        if (!scorecardGroups[scorecardKey]) {
          scorecardGroups[scorecardKey] = [];
        }

        scorecardGroups[scorecardKey].push(buildScorecardRecord_(normalized, {
          uuid: row[idx.UUID],
          firstName: firstName,
          agentUuid: idx.AGENT_UUID !== undefined ? row[idx.AGENT_UUID] : '',
          direction: rowCtx.direction,
          createdAt: row[idx.CREATED_AT],
          clt: row[idx.CLT],
          apPortal: idx['AP Portal'] !== undefined ? row[idx['AP Portal']] : (idx.AP_PORTAL !== undefined ? row[idx.AP_PORTAL] : ''),
          finalScore: normalized.final_score,
          summary: normalized.summary || '',
          errors: normalized.errors || ''
        }));
      }
    } catch (err) {
      skipped++;
      errors.push(`Row ${csvRowIndex + 1} (${row[idx.UUID] || 'Unknown UUID'}): ${err.message}`);
    }

    processed++;

    if (pendingRows.length >= ROW_BATCH_WRITE_SIZE || processed === total) {
      if (pendingRows.length) {
        const startRow = outputSheet.getLastRow() + 1;
        outputSheet.getRange(startRow, 1, pendingRows.length, pendingRows[0].length).setValues(pendingRows);
        pendingRows = [];
      }
    }

    setProgressThrottled_(progressState, {
      status: 'running',
      fileName: options.fileName,
      total: total,
      processed: processed,
      written: written,
      skipped: skipped,
      errors: errors.slice(-10),
      message: `Processed ${processed} of ${total} rows.`
    });

    Utilities.sleep(SLEEP_MS_BETWEEN_ROWS);
  }

  const scorecardLinks = createScorecards_(scorecardGroups);
  if (scorecardLinks.length) {
    updateScorecardLinks_(outputSheet, scorecardLinks);
  }

  const result = {
    ok: errors.length === 0,
    fileName: options.fileName,
    outputSheet: outputSheetName,
    rowsWritten: written,
    skipped: skipped,
    scorecardsCreated: scorecardLinks.length,
    scorecardLinks: scorecardLinks,
    errors: errors
  };

  setProgressThrottled_(progressState, {
    force: true,
    status: 'done',
    fileName: options.fileName,
    total: total,
    processed: processed,
    written: written,
    skipped: skipped,
    errors: errors.slice(-10),
    message: `Done. Appended ${written} rows. Skipped ${skipped}. Created ${scorecardLinks.length} scorecards.`
  });

  return result;
}

function callClaudeSingle_(prompt) {
  const apiKey = PropertiesService.getScriptProperties().getProperty('CLAUDE_API_KEY');
  const model = PropertiesService.getScriptProperties().getProperty('CLAUDE_MODEL') || DEFAULT_MODEL;

  if (!apiKey) {
    throw new Error('Missing CLAUDE_API_KEY in Script Properties.');
  }

  const response = UrlFetchApp.fetch('https://api.anthropic.com/v1/messages', {
    method: 'post',
    contentType: 'application/json',
    muteHttpExceptions: true,
    headers: {
      'x-api-key': apiKey,
      'anthropic-version': '2023-06-01'
    },
    payload: JSON.stringify({
      model: model,
      max_tokens: 3500,
      temperature: 0,
      messages: [{ role: 'user', content: prompt }]
    })
  });

  const code = response.getResponseCode();
  const text = response.getContentText();

  if (code < 200 || code >= 300) {
    throw new Error(`Claude API error ${code}: ${text}`);
  }

  const data = JSON.parse(text);
  const content = Array.isArray(data.content) ? data.content : [];
  const textParts = content
    .filter(function(part) { return part.type === 'text'; })
    .map(function(part) { return part.text; })
    .join('\n');

  if (!textParts) {
    throw new Error('Claude returned no text content.');
  }

  return textParts;
}

function buildEvaluationPrompt_(rowCtx) {
  const criteria = getCriteriaForRow_(rowCtx);

  const rubric = criteria.map(function(c) {
    const applicability = c.applicable
      ? 'Evaluate this criterion.'
      : 'Mark this as N/A. Do not count it in the final score.';
    return `${c.id}. ${c.criterion} (${applicability})`;
  }).join('\n');

  const stateRule = rowCtx.addressState === 'NY'
    ? 'Borrower is a New York resident. Preferred-language criterion must be Yes or No based on transcript evidence if that criterion exists for this direction.'
    : 'Borrower is not a New York resident. Preferred-language criterion should be N/A unless language preference was explicitly discussed.';

  const bilingualInstructions = rowCtx.language === 'Spanish'
    ? [
        'This transcript is likely in Spanish.',
        'Evaluate the call using Spanish transcript evidence when present.',
        'Recognize Spanish equivalents of required disclosures, authentication, settlement language, recording disclosure, payment terms, dispute language, badge number, transfer language, and debt descriptions.',
        'Examples of debt descriptions that should count include "préstamo personal", "tarjeta de crédito", "línea de crédito", "préstamo", "cuenta", or similar Spanish debt/product descriptions.',
        'For inbound C29, count it leniently if the agent says their name followed closely by a number, even if they do not explicitly say "badge" or "number".',
        'Return verdicts in English as Yes, No, or N/A.',
        'Evidence may quote or paraphrase Spanish.'
      ].join('\n')
    : [
        'This transcript is likely in English.',
        'Evaluate using English transcript evidence.',
        'General debt descriptions such as personal loan, credit card, loan, line of credit, or account can count when the criterion is being evaluated leniently for canceled status.',
        'For inbound C29, count it leniently if the agent says their name followed closely by a number, even if they do not explicitly say "badge" or "number".'
      ].join('\n');

  const rules = [
    'QA rules:',
    '- Be somewhat liberal rather than conservative when evaluating evidence.',
    '- Give reasonable benefit of the doubt when transcript evidence strongly suggests compliance.',
    '- For inbound calls, do NOT evaluate Mini Miranda, call recording disclosure, or UDAAP.',
    '- For outbound calls, do NOT evaluate UDAAP.',
    '- Do NOT evaluate these criteria at all in this rubric version:',
    '  * Inbound C21, C22, C25',
    '  * Outbound C28',
    '- Settlement disclosure applies ONLY when settlement is actually accepted, applied, or finalized in the transcript.',
    '- Outbound company name disclosure criterion is satisfied if transcript mentions "January" or "January Technologies" anywhere.',
    '- For outbound recording disclosure, order does not matter as long as it appears early in the call.',
    '- Recording disclosure can be judged leniently. Any transcript language including "recorded" or "recording" can satisfy it.',
    '- For outbound Mini Miranda, it must be provided only after borrower authentication.',
    '- Borrower authentication means first or last name PLUS 2 pieces of PII: last 4 SSN, DOB, or physical address.',
    '- Inbound C29 is satisfied leniently if the transcript includes the agent name followed by a number, even without explicitly saying "badge" or "number".',
    '- Outbound badge number criterion is satisfied if the transcript mentions an agent or badge number like "53", "badge 53", or similar.',
    '- Mini Miranda criterion is satisfied if the transcript mentions "debt collector" or a Spanish equivalent.',
    '- STATUS matters for several criteria and must be applied exactly as follows:',
    '  * If STATUS = canceled, C2 should be lenient: do not require last 4, full debt description, or balance.',
    '  * If STATUS = placed, OOS disclosure criteria are evaluated only if OOS is applicable.',
    '  * For contact information criteria, email-only is allowed only for DSC/third-party scenarios; canceled status alone does not make C11/C14 email-only.',
    '  * For recap criteria, if STATUS = canceled, a simplified recap is acceptable and no full debt summary is required.',
    '- If the borrower disconnects early, recap/summary and later wrap-up items can be N/A.',
    '- If transcript appears cut off, later criteria may be N/A and the call should not receive a numeric score.',
    '- Settlement disclosure script should match one of these concepts:',
    '  1) Automatic approval: If the full negotiated payment amount is not received within the agreed timeframe or is returned unpaid, the agreement is void and a new agreement must be reached.',
    '  2) Approval required: If the request is approved, payment must be set up within 30 days, and if the full negotiated payment amount is not received within the agreed timeframe or is returned unpaid, the agreement is void and a new agreement must be reached.',
    '- Use only the criteria listed below. Do not invent extra criteria.'
  ].join('\n');

  return [
    'You are auditing a January debt collection call transcript.',
    'Return ONLY strict valid JSON. Do not include explanatory text before or after the JSON.',
    'Do not say you need more information. Do not refuse. Output JSON only.',
    '',
    'For each listed criterion determine:',
    '- Verdict: Yes, No, or N/A',
    '- Evidence: ONE concise quote or paraphrase from the transcript supporting the verdict',
    '',
    'Rules:',
    '- Be somewhat liberal rather than conservative.',
    '- If evidence is strongly suggestive, you may mark Yes.',
    '- If evidence is missing, mark No or N/A.',
    '- Evidence must come directly from the transcript.',
    '- Evidence must be short, maximum one sentence.',
    '- Use only the criterion numbers listed below.',
    '- Identify the agent name from the transcript if possible, otherwise use Unknown.',
    '- Do not evaluate deleted criteria.',
    '- For criteria marked not applicable, return N/A.',
    '- For order-sensitive checks, evaluate sequence carefully.',
    '',
    `Call direction: ${rowCtx.direction}`,
    `Borrower state: ${rowCtx.addressState || 'Unknown'}`,
    `IS_OOS: ${rowCtx.isOos ? 'OOS' : 'Not OOS / Unknown'}`,
    `STATUS: ${rowCtx.status || 'Unknown'}`,
    `CALL_CODE: ${rowCtx.callCode || 'Unknown'}`,
    `Company Name: ${rowCtx.companyName || 'Unknown'}`,
    `Language detected: ${rowCtx.language}`,
    stateRule,
    '',
    bilingualInstructions,
    '',
    rules,
    '',
    'Criteria to score:',
    rubric,
    '',
    'Return JSON with this exact shape. Output must begin with { and end with }:',
    JSON.stringify({
      agent_name: 'string',
      summary: 'string',
      evaluations: [{ id: 1, verdict: 'Yes|No|N/A', evidence: 'short supporting evidence' }]
    }, null, 2),
    '',
    'Transcript:',
    rowCtx.transcript
  ].join('\n');
}

function normalizeEvaluationsForRow_(model, rowCtx) {
  const criteria = getCriteriaForRow_(rowCtx);
  const byId = {};

  (Array.isArray(model.evaluations) ? model.evaluations : []).forEach(function(ev) {
    byId[String(ev.id)] = ev;
  });

  const normalizedEvaluations = criteria.map(function(c) {
    const raw = byId[String(c.id)] || {};
    let verdict = c.applicable ? normalizeVerdict_(raw.verdict || 'No') : 'N/A';
    let evidence = c.applicable ? String(raw.evidence || '') : c.naReason;

    return {
      id: c.id,
      criterion: c.criterion,
      verdict: verdict,
      evidence: evidence
    };
  });

  applyHardRuleOverrides_(normalizedEvaluations, rowCtx);
  applyCutoffBenefitOfDoubt_(normalizedEvaluations, rowCtx);

  const applicable = normalizedEvaluations.filter(function(ev) {
    return ev.verdict !== 'N/A';
  });

  const yesCount = applicable.filter(function(ev) {
    return ev.verdict === 'Yes';
  }).length;

  const finalScore = rowCtx.flags.transcriptAppearsCutOff
    ? ''
    : (applicable.length ? Math.round((yesCount / applicable.length) * 100) : 0);

  return {
    agent_name: model.agent_name || 'Unknown',
    summary: model.summary || '',
    errors: buildErrorsText_(normalizedEvaluations, rowCtx),
    final_score: finalScore,
    evaluations: normalizedEvaluations
  };
}

function applyHardRuleOverrides_(evaluations, rowCtx) {
  const byId = {};
  evaluations.forEach(function(ev) {
    byId[String(ev.id)] = ev;
  });

  const badgeId = rowCtx.direction === 'INBOUND' ? '29' : '31';
  if (byId[badgeId] && rowCtx.flags.hasBadgeNumberMention) {
    byId[badgeId].verdict = 'Yes';
    byId[badgeId].evidence = rowCtx.direction === 'INBOUND'
      ? 'Transcript includes the agent name followed by a number or another acceptable identifier reference.'
      : 'Transcript includes an agent or badge number reference.';
  }

  if (rowCtx.direction === 'OUTBOUND' && byId['12'] && rowCtx.flags.hasJanuaryNameMention) {
    byId['12'].verdict = 'Yes';
    byId['12'].evidence = 'Transcript mentions January or January Technologies.';
  }

  if (rowCtx.direction === 'OUTBOUND' && byId['5']) {
    if (rowCtx.flags.hasRecordingDisclosureEarly && rowCtx.flags.hasMiniMirandaAfterAuth) {
      byId['5'].verdict = 'Yes';
      byId['5'].evidence = 'Recording disclosure appeared early and debt-collector language appeared after borrower authentication.';
    } else {
      byId['5'].verdict = 'No';
      byId['5'].evidence = buildOutboundC5Evidence_(rowCtx);
    }
  }

  if (rowCtx.direction === 'INBOUND' && byId['6'] && rowCtx.flags.hasRecordingDisclosureAny) {
    byId['6'].verdict = 'Yes';
    byId['6'].evidence = 'Transcript includes recording-related language.';
  }

  const settlementId = rowCtx.direction === 'INBOUND' ? '7' : '9';
  if (byId[settlementId]) {
    if (!rowCtx.flags.settlementAcceptedOrApplied) {
      byId[settlementId].verdict = 'N/A';
      byId[settlementId].evidence = 'Not evaluated because settlement was not accepted or applied in the transcript.';
    } else if (rowCtx.flags.hasValidSettlementDisclosure) {
      byId[settlementId].verdict = 'Yes';
      byId[settlementId].evidence = 'Settlement disclosure language was provided when settlement was accepted or applied.';
    } else {
      byId[settlementId].verdict = 'No';
      byId[settlementId].evidence = 'Settlement was accepted or applied, but the required settlement disclosure language was not clearly stated.';
    }
  }

  if (byId['2'] && rowCtx.status === 'canceled') {
    if (rowCtx.flags.hasCanceledDebtDetails) {
      byId['2'].verdict = 'Yes';
      byId['2'].evidence = 'Canceled account: creditor/debt context was stated, so last 4 and balance were not required.';
    } else if (byId['2'].verdict === 'No') {
      byId['2'].evidence = 'Canceled account: transcript still did not clearly identify creditor/debt context even under lenient rules.';
    }
  }

  const contactId = rowCtx.direction === 'INBOUND' ? '11' : '14';
  if (byId[contactId]) {
    if (rowCtx.flags.relaxedContactInfoScenario) {
      if (rowCtx.flags.hasEmailContactInfo) {
        byId[contactId].verdict = 'Yes';
        byId[contactId].evidence = 'Email contact information was captured, which is sufficient for DSC/third-party scenarios.';
      } else if (byId[contactId].verdict === 'No') {
        byId[contactId].evidence = 'Email contact information was not clearly captured, which is required in this DSC/third-party scenario.';
      }
    }
  }

  const recapId = rowCtx.direction === 'INBOUND' ? '26' : '27';
  if (byId[recapId] && rowCtx.flags.borrowerDisconnectedEarly) {
    byId[recapId].verdict = 'N/A';
    byId[recapId].evidence = 'Not evaluated because borrower disconnected early.';
  } else if (byId[recapId] && rowCtx.status === 'canceled') {
    if (rowCtx.flags.hasSimplifiedRecap) {
      byId[recapId].verdict = 'Yes';
      byId[recapId].evidence = 'Canceled account: a simplified recap was provided, so a full debt summary was not required.';
    } else if (byId[recapId].verdict === 'No') {
      byId[recapId].evidence = 'Canceled account: no simplified recap or closeout summary was clearly provided.';
    }
  }
}

function applyCutoffBenefitOfDoubt_(evaluations, rowCtx) {
  if (!rowCtx.flags.transcriptAppearsCutOff) return;

  const keepIds = getKeepCriteriaForCutoff_(rowCtx.direction);

  evaluations.forEach(function(ev) {
    if (keepIds.indexOf(ev.id) === -1) {
      ev.verdict = 'N/A';
      ev.evidence = 'Not evaluated because transcript appears cut off.';
    }
  });
}

function getKeepCriteriaForCutoff_(direction) {
  if (direction === 'INBOUND') {
    return [1, 2, 3, 4, 7, 8, 9, 11, 12, 13, 16, 26, 27, 28, 29, 32];
  }
  return [1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 18, 27, 29, 30, 31, 34];
}

function buildOutboundC5Evidence_(rowCtx) {
  if (!rowCtx.flags.hasRecordingDisclosureEarly && !rowCtx.flags.hasMiniMirandaAfterAuth) {
    return 'Early recording language and post-authentication debt-collector language were not both clearly present.';
  }
  if (!rowCtx.flags.hasRecordingDisclosureEarly) {
    return 'Recording language including "recorded" or "recording" was not clearly present early in the call.';
  }
  return 'Debt-collector language was not clearly stated after borrower authentication.';
}

function buildWideOutputRow_(model, meta) {
  const evaluations = Array.isArray(model.evaluations) ? model.evaluations : [];
  const byId = {};

  evaluations.forEach(function(ev) {
    byId[String(ev.id)] = ev;
  });

  const row = [
    meta.uuid || '',
    meta.firstName || '',
    meta.agentUuid || '',
    meta.direction || '',
    model.agent_name || 'Unknown',
    'LLM',
    meta.surveyDate || '',
    meta.callDate || '',
    meta.callTime || '',
    meta.language || '',
    meta.accountNumber || '',
    meta.apPortal || '',
    meta.commChannel || '',
    meta.createdAt || '',
    meta.callCommUuid || '',
    meta.originalCreditor || '',
    meta.companyName || '',
    meta.addressState || '',
    model.final_score === '' ? '' : Number(model.final_score || 0),
    model.summary || '',
    model.errors || '',
    meta.scorecard || ''
  ];

  getUnifiedCriteriaList_().forEach(function(c) {
    const ev = byId[String(c.id)] || {};
    row.push(ev.verdict || '');
    row.push(String(ev.evidence || ''));
  });

  return row;
}

function buildScorecardRecord_(model, meta) {
  const evaluations = Array.isArray(model.evaluations) ? model.evaluations : [];
  const byId = {};

  evaluations.forEach(function(ev) {
    byId[String(ev.id)] = ev;
  });

  const row = [
    meta.uuid || '',
    meta.firstName || '',
    meta.agentUuid || '',
    meta.direction || '',
    meta.createdAt || '',
    meta.clt || '',
    meta.apPortal || '',
    meta.finalScore === '' ? '' : Number(meta.finalScore || 0),
    meta.summary || '',
    meta.errors || '',
    ''
  ];

  getUnifiedCriteriaList_().forEach(function(c) {
    const ev = byId[String(c.id)] || {};
    row.push(ev.verdict || '');
    row.push(String(ev.evidence || ''));
  });

  return row;
}

function writeOutputHeader_(sheet) {
  const headers = buildExpectedHeaders_();
  sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
}

function ensureOutputSheet_(ss, name) {
  let sheet = ss.getSheetByName(name);
  const expectedHeaders = buildExpectedHeaders_();

  if (!sheet) {
    sheet = ss.insertSheet(name);
    writeOutputHeader_(sheet);
    sheet.setFrozenRows(1);
    return sheet;
  }

  if (sheet.getLastRow() === 0) {
    writeOutputHeader_(sheet);
    sheet.setFrozenRows(1);
    return sheet;
  }

  const currentHeaders = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0].map(String);
  if (JSON.stringify(currentHeaders) !== JSON.stringify(expectedHeaders)) {
    throw new Error('Existing output sheet headers do not match the updated schema. Use a new output sheet name.');
  }

  return sheet;
}

function buildExpectedHeaders_() {
  const headers = [
    'UUID', 'FIRST_NAME', 'AGENT_UUID', 'Direction', 'Agent', 'Evaluator', 'Date of Survey', 'Date', 'Time',
    'Language', 'ACCOUNT_NUMBER', 'AP Portal', 'Comm Channel', 'Created At', 'Call Comm UUID', 'Original Creditor',
    'Company Name', 'Address State', 'Final Score', 'Summary', 'Errors', 'Scorecard'
  ];

  getUnifiedCriteriaList_().forEach(function(c) {
    headers.push(`C${c.id} Verdict`);
    headers.push(`C${c.id} Evidence`);
  });

  return headers;
}

function buildRowContext_(row, idx) {
  const transcript = String(row[idx.TRANSCRIPT] || '');
  const direction = normalizeDirection_(row[idx.DIRECTION]);
  const addressState = String(row[idx.ADDRESS_STATE] || '').trim().toUpperCase();
  const companyName = String(row[idx.COMPANY_NAME] || '').trim();
  const callCode = String(row[idx.CALL_CODE] || '').trim().toLowerCase();
  const isOosRaw = String(row[idx.IS_OOS] || '').trim().toUpperCase();
  const status = idx.STATUS !== undefined ? String(row[idx.STATUS] || '').trim().toLowerCase() : '';
  const language = detectLanguage_(transcript);

  return {
    direction: direction,
    transcript: transcript,
    addressState: addressState,
    companyName: companyName,
    callCode: callCode,
    isOos: isOosRaw === 'OOS',
    status: status,
    language: language,
    flags: deriveCallFlags_(transcript, callCode, companyName, isOosRaw, status)
  };
}

function deriveCallFlags_(transcript, callCode, companyName, isOosRaw, status) {
  const t = String(transcript || '');
  const code = String(callCode || '').toLowerCase();
  const normalizedCompany = normalizeCompanyName_(companyName);
  const normalizedStatus = String(status || '').toLowerCase();

  const patterns = {
    paymentMade: /(payment (was )?successfully processed|processing that now|authorization to proceed with this payment|making a payment of|partial payment of|card ending in|routing number|account number|pago procesado|autoriza el pago|procesar el pago|haciendo un pago|pago parcial|tarjeta terminando en|número de ruta|número de cuenta)/i,
    settlementAcceptedOrApplied: /(i accept|accepted the settlement|we can do that settlement|let's move forward with the settlement|apply the settlement|set up that settlement|approved settlement|i will take the offer|agree to the settlement|acepto|acepta la oferta|vamos a hacer ese acuerdo|aplicar el acuerdo|liquidación aprobada|tomaré la oferta|de acuerdo con la liquidación|payment of the settlement amount|pago del monto de liquidación)/i,
    disputeDiscussed: /(dispute|fraud|identity theft|not responsible|do not recognize this debt|disputa|fraude|robo de identidad|no reconozco esta deuda|no soy responsable)/i,
    oneTimePaymentDiscussed: /(one[- ]time payment|one time payment|pay today|make a payment today|single payment|lump sum|pago único|pagar hoy|pago hoy|un solo pago)/i,
    paymentPlanDiscussed: /(payment plan|monthly payment|draft each month|draft each week|reoccurring payments|recurring payments|schedule it out|plan de pagos|pago mensual|pagos recurrentes|programar pagos)/i,
    transferOccurred: /(transfer you|connect you with|direct your call|specialist|transferred|transferir|transferencia|conectarlo con|especialista|lo transfiero)/i,
    askedWhoCalling: /(who is this|who's calling|who am i speaking with|what company is this|who called me|quién habla|quién llama|de qué compañía llama|quién me llama)/i,
    borrowerDisconnectedEarly: /(call dropped|disconnect|disconnected|hang up|hung up|line went dead|se cortó la llamada|se desconectó|colgó|la línea se cayó)/i,
    unauthorizedThirdParty: /(calling on behalf of|on behalf of someone else|not authorized|unauthorized third party|i am her husband|i am his wife|power of attorney|poa|llamo de parte de|no autorizado|tercero no autorizado|soy su esposo|soy su esposa|poder legal)/i,
    dscOrThirdParty: /(dsc|debt settlement company|settlement company|debt resolution company|third party|third-party|tercero|compañía de liquidación|empresa de liquidación|empresa de resolución de deudas|negociadora)/i,
    resolutionCompleted: /(paid in full|full negotiated payment amount received|settled in full|resolved account|account resolved|payment completed|completed settlement payment|pagado en su totalidad|monto negociado completo recibido|liquidado en su totalidad|cuenta resuelta|pago completado)/i,
    januaryMention: /\bjanuary\b|\bjanuary technologies\b/i
  };

  const paymentMade = patterns.paymentMade.test(t);
  const settlementAcceptedOrApplied = patterns.settlementAcceptedOrApplied.test(t) || code === 'settlement_accepted' || code === 'settlement';
  const disputeDiscussed = patterns.disputeDiscussed.test(t) || code === 'dispute';
  const oneTimePaymentDiscussed = patterns.oneTimePaymentDiscussed.test(t);
  const paymentPlanDiscussed = patterns.paymentPlanDiscussed.test(t);
  const transferOccurred = patterns.transferOccurred.test(t);
  const askedWhoCalling = patterns.askedWhoCalling.test(t);
  const borrowerDisconnectedEarly = patterns.borrowerDisconnectedEarly.test(t);
  const unauthorizedThirdParty = patterns.unauthorizedThirdParty.test(t);
  const dscOrThirdParty = patterns.dscOrThirdParty.test(t);
  const resolutionCompleted = patterns.resolutionCompleted.test(t);
  const hasThirdPartyJoin = /(new[, ]+third party|third party joined|joined the call|someone else('|’)s debit card|someone else('|’)s card|authorized to participate on a call|tercero se unió|otra persona se unió|alguien más se unió)/i.test(t);

  const authenticationAnalysis = detectAuthenticationSequence_(t);
  const hasRecordingDisclosureAny = detectRecordingDisclosure_(t);
  const hasRecordingDisclosureEarly = detectEarlyRecordingDisclosure_(t);
  const hasMiniMirandaAfterAuth = detectMiniMirandaAfterAuthentication_(t, authenticationAnalysis.authEndIndex);
  const hasValidSettlementDisclosure = detectSettlementDisclosure_(t);
  const hasBadgeNumberMention = detectBadgeNumberMention_(t);
  const transcriptAppearsCutOff = detectTranscriptCutoff_(t);
  const hasJanuaryNameMention = patterns.januaryMention.test(t);
  const hasGeneralDebtDescription = detectGeneralDebtDescription_(t);
  const hasCreditorMention = detectCreditorMention_(t, companyName);
  const hasCanceledDebtDetails = hasGeneralDebtDescription && (hasCreditorMention || /creditor|acreedor|debt|deuda|account|cuenta/i.test(t));
  const hasEmailContactInfo = detectEmailContactInfo_(t);
  const hasSimplifiedRecap = detectSimplifiedRecap_(t);

  const someoneOtherThanBorrowerOnCall = unauthorizedThirdParty || dscOrThirdParty || hasThirdPartyJoin || /(on behalf of|de parte de|my husband|my wife|mi esposo|mi esposa|my son|my daughter|mi hijo|mi hija|authorized representative|representative|spouse|parent|relative|friend)/i.test(t);
  const relaxedContactInfoScenario = unauthorizedThirdParty || dscOrThirdParty;
  const authorizationConfirmed = detectThirdPartyAuthorizationConfirmed_(t);
  const sharedAccountDetailsBeforeAuthorization = detectSharedAccountDetailsBeforeAuthorization_(t, authorizationConfirmed);

  return {
    hasThirdPartyJoin: hasThirdPartyJoin,
    unauthorizedThirdParty: unauthorizedThirdParty,
    dscOrThirdParty: dscOrThirdParty,
    relaxedContactInfoScenario: relaxedContactInfoScenario,
    someoneOtherThanBorrowerOnCall: someoneOtherThanBorrowerOnCall,
    paymentMade: paymentMade,
    settlementAcceptedOrApplied: settlementAcceptedOrApplied,
    disputeDiscussed: disputeDiscussed,
    oneTimePaymentDiscussed: oneTimePaymentDiscussed,
    paymentPlanDiscussed: paymentPlanDiscussed,
    companyRequiresClientSpecificDispute: companyRequiresClientSpecificDispute_(normalizedCompany),
    companyRequiresStipulation: companyRequiresStipulation_(normalizedCompany),
    aboutSettlement: settlementAcceptedOrApplied,
    resolutionCompleted: resolutionCompleted,
    transferOccurred: transferOccurred,
    askedWhoCalling: askedWhoCalling,
    borrowerDisconnectedEarly: borrowerDisconnectedEarly,
    hasRecordingDisclosureAny: hasRecordingDisclosureAny,
    hasRecordingDisclosureEarly: hasRecordingDisclosureEarly,
    hasMiniMirandaAfterAuth: hasMiniMirandaAfterAuth,
    hasValidSettlementDisclosure: hasValidSettlementDisclosure,
    hasBadgeNumberMention: hasBadgeNumberMention,
    authSatisfied: authenticationAnalysis.authSatisfied,
    authEndIndex: authenticationAnalysis.authEndIndex,
    transcriptAppearsCutOff: transcriptAppearsCutOff,
    hasJanuaryNameMention: hasJanuaryNameMention,
    hasGeneralDebtDescription: hasGeneralDebtDescription,
    hasCreditorMention: hasCreditorMention,
    hasCanceledDebtDetails: hasCanceledDebtDetails,
    hasEmailContactInfo: hasEmailContactInfo,
    hasSimplifiedRecap: hasSimplifiedRecap,
    authorizationConfirmed: authorizationConfirmed,
    sharedAccountDetailsBeforeAuthorization: sharedAccountDetailsBeforeAuthorization
  };
}

function companyRequiresClientSpecificDispute_(normalizedCompany) {
  return normalizedCompany === 'klarna' || normalizedCompany === 'cavalry portfolio services, llc';
}

function companyRequiresStipulation_(normalizedCompany) {
  return STIPULATION_COMPANIES.map(normalizeCompanyName_).indexOf(normalizedCompany) !== -1;
}

function normalizeCompanyName_(value) {
  return String(value || '').trim().toLowerCase();
}

function getCriteriaForRow_(rowCtx) {
  const flags = rowCtx.flags;
  const isCanceled = rowCtx.status === 'canceled';
  const isPlacedAndOos = rowCtx.status === 'placed' && (String(rowCtx.isOos) === 'true' || rowCtx.isOos === true);

  if (rowCtx.direction === 'INBOUND') {
    return [
      crit_(1, 'Agent authenticated the account by requesting first or last name plus 2 pieces of PII (last 4 SSN, DOB, or physical address).', true),
      crit_(2,
        isCanceled
          ? 'Agent stated the original and current creditor and a general description of the debt. For canceled status, do not require last 4, full debt description, or balance.'
          : 'Agent stated the original and current creditor, last 4 of account number, description of debt, and current balance.',
        true
      ),
      crit_(3, 'Agent did not use discriminatory language or behavior.', true),
      crit_(4, 'Agent followed payment compliance guidelines, including required third-party authorization when applicable.', flags.paymentMade, 'Not evaluated because no payment activity occurred.'),
      crit_(5, 'Mini Miranda / debt collector disclosure.', false, 'Not evaluated for inbound calls.'),
      crit_(6, 'Recording disclosure when new third party joined.', false, 'Not evaluated for inbound calls.'),
      crit_(7, 'Agent provided the settlement disclosure when settlement was accepted or applied.', flags.settlementAcceptedOrApplied, 'Not evaluated because settlement was not accepted or applied in the transcript.'),
      crit_(8, 'Agent confirmed or updated borrower preferred language for a New York resident.', rowCtx.addressState === 'NY', 'Not evaluated because borrower is not a New York resident.'),
      crit_(9, 'Agent provided the OOS disclosure.', isPlacedAndOos, 'Not evaluated because account is not placed and OOS, so the OOS disclosure does not apply.'),
      crit_(10, 'UDAAP risk evaluation.', false, 'Not evaluated for inbound calls.'),
      crit_(11,
        flags.relaxedContactInfoScenario
          ? 'Agent confirmed or captured at least an email address. For DSC/third-party scenarios, full borrower contact confirmation is not required.'
          : 'Agent confirmed or updated borrower contact info (phone, email, address).',
        true
      ),
      crit_(12, 'Agent demonstrated call control.', true),
      crit_(13, 'Agent was personable and expressed empathy.', true),
      crit_(16, 'Agent filed a dispute under the correct category and advised borrower to send supporting documents when applicable.', flags.disputeDiscussed, 'Not evaluated because no dispute was discussed.'),
      crit_(26,
        isCanceled
          ? 'Agent provided a recap or simplified closeout summary of the call. For canceled status, no full debt summary is required.'
          : 'Agent provided a recap or summary of the call.',
        !flags.borrowerDisconnectedEarly,
        'Not evaluated because borrower disconnected early.'
      ),
      crit_(27, 'Agent correctly addressed a Klarna or Cavalry dispute.', flags.companyRequiresClientSpecificDispute && flags.disputeDiscussed, 'Not evaluated because this was not a Klarna/Cavalry dispute call.'),
      crit_(28, 'Agent informed borrower of BCU, RBFCU, or Flexible Finance stipulation prior to offering settlement.', flags.companyRequiresStipulation && flags.settlementAcceptedOrApplied, 'Not evaluated because stipulation did not apply.'),
      crit_(29, 'Agent stated their badge number.', true),
      crit_(32, 'Agent transferred the call or account to the appropriate party when needed.', flags.transferOccurred, 'Not evaluated because no transfer occurred.')
    ];
  }

  return [
    crit_(1, 'Agent authenticated the account by requesting first or last name plus 2 pieces of PII (last 4 SSN, DOB, or physical address).', true),
    crit_(2,
      isCanceled
        ? 'Agent stated the original and current creditor and a general description of the debt. For canceled status, do not require last 4, full debt description, or balance.'
        : 'Agent stated the original and current creditor, last 4 of account number, description of debt, and current balance.',
      true
    ),
    crit_(4, 'Agent did not use discriminatory language or behavior.', true),
    crit_(5, 'Agent gave call recording disclosure early in the call and provided debt-collector language after borrower authentication.', true),
    crit_(6, 'Agent did not disclose the purpose of the call or leave a message with an unauthorized third party.', true),
    crit_(7, 'Agent followed payment compliance guidelines, including payment plan disclosure and EFTA compliance when applicable.', flags.paymentMade || flags.paymentPlanDiscussed, 'Not evaluated because no payment or payment-plan activity occurred.'),
    crit_(8, 'Agent provided the OOS disclosure.', isPlacedAndOos, 'Not evaluated because account is not placed and OOS, so the OOS disclosure does not apply.'),
    crit_(9, 'Agent provided the settlement disclosure when settlement was accepted or applied.', flags.settlementAcceptedOrApplied, 'Not evaluated because settlement was not accepted or applied in the transcript.'),
    crit_(10, 'Agent confirmed or updated borrower preferred language for a New York resident.', rowCtx.addressState === 'NY', 'Not evaluated because borrower is not a New York resident.'),
    crit_(11, 'Agent provided recording disclosure if a new third party joined the phone call.', flags.hasThirdPartyJoin, 'Not evaluated because no new third party joined the call.'),
    crit_(12, 'Agent did not disclose company name before authentication unless explicitly asked by the borrower.', true),
    crit_(13, 'UDAAP risk evaluation.', false, 'Not evaluated for outbound calls.'),
    crit_(14,
      flags.relaxedContactInfoScenario
        ? 'Agent confirmed or captured at least an email address. For DSC/third-party scenarios, full borrower contact confirmation is not required.'
        : 'Agent confirmed or updated borrower contact info (phone, email, address).',
      true
    ),
    crit_(15, 'Agent was personable and expressed empathy or professionalism.', true),
    crit_(18, 'Agent filed a dispute under the correct category and advised borrower to send supporting documents when applicable.', flags.disputeDiscussed, 'Not evaluated because no dispute was discussed.'),
    crit_(27,
      isCanceled
        ? 'Agent provided a recap or simplified closeout summary of the call. For canceled status, no full debt summary is required.'
        : 'Agent provided a recap or summary of the call.',
      !flags.borrowerDisconnectedEarly,
      'Not evaluated because borrower disconnected early.'
    ),
    crit_(29, 'Agent correctly addressed a Klarna or Cavalry dispute.', flags.companyRequiresClientSpecificDispute && flags.disputeDiscussed, 'Not evaluated because this was not a Klarna/Cavalry dispute call.'),
    crit_(30, 'Agent informed borrower of BCU, RBFCU, or Flexible Finance stipulation prior to offering settlement.', flags.companyRequiresStipulation && flags.settlementAcceptedOrApplied, 'Not evaluated because stipulation did not apply.'),
    crit_(31, 'Agent stated their badge number.', true),
    crit_(34, 'Agent transferred the call or account to the appropriate party when needed.', flags.transferOccurred, 'Not evaluated because no transfer occurred.')
  ];
}

function buildErrorsText_(evaluations, rowCtx) {
  const deficiencies = evaluations
    .filter(function(ev) { return ev.verdict === 'No'; })
    .map(function(ev) {
      const evidence = String(ev.evidence || '').trim();
      return 'C' + ev.id + ' ' + ev.criterion + ': Not satisfied' + (
        evidence ? ' — ' + evidence : ' — No supporting evidence found in transcript.'
      );
    });

  if (rowCtx && rowCtx.flags && rowCtx.flags.transcriptAppearsCutOff) {
    deficiencies.unshift('Transcript appears cut off — call was not numerically scored.');
  }

  return deficiencies.join(' | ');
}

function crit_(id, criterion, applicable, naReason) {
  return {
    id: id,
    criterion: criterion,
    applicable: applicable,
    naReason: naReason || 'Not applicable for this call.'
  };
}

function getUnifiedCriteriaList_() {
  return [
    { id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }, { id: 5 }, { id: 6 }, { id: 7 }, { id: 8 }, { id: 9 },
    { id: 10 }, { id: 11 }, { id: 12 }, { id: 13 }, { id: 14 }, { id: 15 }, { id: 16 }, { id: 18 },
    { id: 26 }, { id: 27 }, { id: 28 }, { id: 29 }, { id: 30 }, { id: 32 }, { id: 31 }, { id: 34 }
  ];
}

function normalizeVerdict_(value) {
  const v = String(value || '').trim().toUpperCase();
  if (v === 'YES' || v === 'PASS') return 'Yes';
  if (v === 'NO' || v === 'FAIL') return 'No';
  return 'N/A';
}

function parseModelJson_(text) {
  const cleaned = String(text || '')
    .trim()
    .replace(/^```json\s*/i, '')
    .replace(/^```\s*/i, '')
    .replace(/```\s*$/i, '');

  return JSON.parse(cleaned);
}

function parseModelJsonSafe_(text, rowCtx) {
  try {
    return parseModelJson_(text);
  } catch (err1) {
    const extracted = extractFirstJsonObject_(text);
    if (extracted) {
      try {
        return JSON.parse(extracted);
      } catch (err2) {}
    }

    try {
      const repaired = repairJsonResponse_(text, rowCtx);
      return parseModelJson_(repaired);
    } catch (err3) {
      return buildFallbackJson_(rowCtx, text);
    }
  }
}

function extractFirstJsonObject_(text) {
  const s = String(text || '');
  const start = s.indexOf('{');
  if (start === -1) return '';

  let depth = 0;
  let inString = false;
  let escape = false;

  for (let i = start; i < s.length; i++) {
    const ch = s[i];

    if (inString) {
      if (escape) {
        escape = false;
      } else if (ch === '\\') {
        escape = true;
      } else if (ch === '"') {
        inString = false;
      }
      continue;
    }

    if (ch === '"') {
      inString = true;
    } else if (ch === '{') {
      depth++;
    } else if (ch === '}') {
      depth--;
      if (depth === 0) {
        return s.slice(start, i + 1);
      }
    }
  }

  return '';
}

function repairJsonResponse_(badText, rowCtx) {
  const apiKey = PropertiesService.getScriptProperties().getProperty('CLAUDE_API_KEY');
  const model = PropertiesService.getScriptProperties().getProperty('CLAUDE_MODEL') || DEFAULT_MODEL;

  if (!apiKey) {
    throw new Error('Missing CLAUDE_API_KEY in Script Properties.');
  }

  const criteria = getCriteriaForRow_(rowCtx);
  const repairPrompt = [
    'Convert the following malformed model output into STRICT valid JSON only.',
    'Do not add commentary.',
    'Output only JSON starting with { and ending with }.',
    '',
    'Return exactly this schema:',
    JSON.stringify({
      agent_name: 'string',
      summary: 'string',
      evaluations: criteria.map(function(c) {
        return { id: c.id, verdict: 'Yes|No|N/A', evidence: 'short supporting evidence' };
      })
    }, null, 2),
    '',
    'If the malformed output is unusable, fill applicable criteria with verdict "No" and a short evidence note.',
    '',
    'Malformed output:',
    badText
  ].join('\n');

  const response = UrlFetchApp.fetch('https://api.anthropic.com/v1/messages', {
    method: 'post',
    contentType: 'application/json',
    muteHttpExceptions: true,
    headers: {
      'x-api-key': apiKey,
      'anthropic-version': '2023-06-01'
    },
    payload: JSON.stringify({
      model: model,
      max_tokens: 2200,
      temperature: 0,
      messages: [{ role: 'user', content: repairPrompt }]
    })
  });

  const code = response.getResponseCode();
  const body = response.getContentText();

  if (code < 200 || code >= 300) {
    throw new Error('JSON repair failed: ' + body);
  }

  const data = JSON.parse(body);
  const content = Array.isArray(data.content) ? data.content : [];
  const repaired = content
    .filter(function(part) { return part.type === 'text'; })
    .map(function(part) { return part.text; })
    .join('\n');

  if (!repaired) {
    throw new Error('JSON repair returned empty output.');
  }

  return repaired;
}

function buildFallbackJson_(rowCtx, badText) {
  const criteria = getCriteriaForRow_(rowCtx);

  return {
    agent_name: 'Unknown',
    summary: 'Model did not return valid JSON. Row was preserved with fallback output.',
    evaluations: criteria.map(function(c) {
      return {
        id: c.id,
        verdict: c.applicable ? 'No' : 'N/A',
        evidence: c.applicable
          ? ('Model returned invalid output: ' + String(badText || '').slice(0, 120))
          : c.naReason
      };
    })
  };
}

function parseClt_(value) {
  const raw = String(value || '').trim();
  const m = raw.match(/(\d{4}-\d{2}-\d{2}|\d{2}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})/);
  return { date: m ? m[1] : '', time: m ? m[2] : '' };
}

function detectLanguage_(transcript) {
  const t = String(transcript || '').toLowerCase();
  const spanishHints = [
    'hola', 'gracias', 'por favor', 'buenos dias', 'buenas tardes', 'deuda', 'pago', 'cuenta',
    'necesito', 'puede', 'habla espanol', 'español', 'usted', 'llamada', 'grabada', 'cobrador',
    'intento de cobrar', 'fecha de nacimiento', 'últimos cuatro', 'seguro social', 'dirección',
    'préstamo personal', 'prestamo personal', 'tarjeta de crédito', 'tarjeta de credito', 'línea de crédito', 'linea de credito'
  ];

  let count = 0;
  for (let i = 0; i < spanishHints.length; i++) {
    if (t.indexOf(spanishHints[i]) !== -1) count++;
  }

  return count >= 2 ? 'Spanish' : 'English';
}

function isVoicemailTranscript_(transcript) {
  const t = String(transcript || '').toLowerCase();
  return /voicemail|limited content message|please call us back|leave a message|beep|tone after the beep|buzón de voz|deje su mensaje|después del tono|por favor devuélvanos la llamada/.test(t);
}

function detectAuthenticationSequence_(transcript) {
  const t = String(transcript || '').toLowerCase();

  const nameRegex = /(first name|last name|full name|nombre|apellido|nombre completo)/i;
  const ssnRegex = /(last four|last 4|ssn ending|social security ending|últimos cuatro|últimos 4|seguro social terminando)/i;
  const dobRegex = /(date of birth|dob|birthday|fecha de nacimiento)/i;
  const addressRegex = /(street address|mailing address|physical address|address on file|dirección física|dirección postal|dirección registrada|su dirección)/i;

  const nameMatch = t.search(nameRegex);
  const ssnMatch = t.search(ssnRegex);
  const dobMatch = t.search(dobRegex);
  const addressMatch = t.search(addressRegex);

  const piiPositions = [];
  if (ssnMatch >= 0) piiPositions.push(ssnMatch);
  if (dobMatch >= 0) piiPositions.push(dobMatch);
  if (addressMatch >= 0) piiPositions.push(addressMatch);

  const authSatisfied = nameMatch >= 0 && piiPositions.length >= 2;
  let authEndIndex = -1;

  if (authSatisfied) {
    piiPositions.sort(function(a, b) { return a - b; });
    authEndIndex = Math.max(nameMatch, piiPositions[1]);
  }

  return {
    authSatisfied: authSatisfied,
    authEndIndex: authEndIndex
  };
}

function detectRecordingDisclosure_(transcript) {
  const t = String(transcript || '');
  return /(recorded|recording|grabad|grabación|monitoread|monitoring)/i.test(t);
}

function detectEarlyRecordingDisclosure_(transcript) {
  const t = String(transcript || '');
  const earlySegment = t.slice(0, 1200);
  return /(recorded|recording|grabad|grabación|monitoread|monitoring)/i.test(earlySegment);
}

function detectMiniMirandaAfterAuthentication_(transcript, authEndIndex) {
  const t = String(transcript || '');
  if (authEndIndex < 0) return false;
  const afterAuth = t.slice(authEndIndex);
  return /(debt collector|debt collection|cobrador de deudas|cobranza de deudas)/i.test(afterAuth);
}

function detectSettlementDisclosure_(transcript) {
  const t = String(transcript || '');

  const automaticApprovalPattern = /(full negotiated payment amount|agreed timeframe|returned by your bank as unpaid|agreement will be void|new agreement must be reached|monto negociado completo|plazo acordado|devuelto por su banco como impago|el acuerdo quedará sin efecto|se debe llegar a un nuevo acuerdo)/i;
  const approvalRequiredPatternA = /(if your request is approved|request is approved|si su solicitud es aprobada|si se aprueba su solicitud)/i;
  const approvalRequiredPatternB = /(set up a payment within 30 days|payment within 30 days|programar un pago dentro de 30 días|pago dentro de 30 días)/i;
  const approvalRequiredPatternC = /(full negotiated payment amount|agreed timeframe|returned by your bank as unpaid|agreement will be void|new agreement must be reached|monto negociado completo|plazo acordado|devuelto por su banco como impago|el acuerdo quedará sin efecto|se debe llegar a un nuevo acuerdo)/i;

  const automaticApproval = automaticApprovalPattern.test(t);
  const approvalRequired = approvalRequiredPatternA.test(t) && approvalRequiredPatternB.test(t) && approvalRequiredPatternC.test(t);

  return automaticApproval || approvalRequired;
}

function detectBadgeNumberMention_(transcript) {
  const t = String(transcript || '');

  if (/(badge( number)?\s*[:#]?\s*\d{1,4}|agent( number)?\s*[:#]?\s*\d{1,4}|my number is\s*\d{1,4}|mi número es\s*\d{1,4}|número de agente\s*[:#]?\s*\d{1,4}|identification number\s*[:#]?\s*\d{1,4})/i.test(t)) {
    return true;
  }

  if (/(this is|my name is|i am|speaking|this is agent|mi nombre es|soy|le habla|habla)\s+[A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑa-záéíóúñ'’-]{1,30}(?:\s+[A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑa-záéíóúñ'’-]{1,30}){0,2}[\s,.\-]{1,6}\d{1,4}\b/i.test(t)) {
    return true;
  }

  return false;
}

function detectTranscriptCutoff_(transcript) {
  const t = String(transcript || '').toLowerCase();

  if (/(call dropped|disconnected|line went dead|recording ended abruptly|audio cuts off|se cortó la llamada|se desconectó|la línea se cayó|grabación se corta)/i.test(t)) {
    return true;
  }

  const trimmed = String(transcript || '').trim();
  if (!trimmed) return false;

  const tail = trimmed.slice(-120);
  if (!/[.!?]"?\)?\s*$/.test(tail) && tail.length > 40) {
    return true;
  }

  return false;
}

function detectGeneralDebtDescription_(transcript) {
  const t = String(transcript || '');
  return /(personal loan|loan|credit card|line of credit|installment loan|account|debt|medical bill|retail card|consumer loan|préstamo personal|prestamo personal|préstamo|prestamo|tarjeta de crédito|tarjeta de credito|línea de crédito|linea de credito|cuenta|deuda|préstamo a plazos|prestamo a plazos)/i.test(t);
}

function detectCreditorMention_(transcript, companyName) {
  const t = String(transcript || '');
  const company = String(companyName || '').trim();
  if (/original creditor|current creditor|creditor|acreedor original|acreedor actual|acreedor/i.test(t)) {
    return true;
  }
  if (company) {
    try {
      const escaped = company.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      return new RegExp(escaped, 'i').test(t);
    } catch (e) {}
  }
  return false;
}

function detectEmailContactInfo_(transcript) {
  const t = String(transcript || '');
  return /\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/i.test(t) ||
    /(email address|e-mail|email|correo electrónico|correo electronico)/i.test(t);
}

function detectSimplifiedRecap_(transcript) {
  const t = String(transcript || '');
  return /(to recap|just to recap|in summary|summary|recap|so today we|we reviewed|we discussed|you will receive|next steps|resumen|para resumir|en resumen|repaso|hoy revisamos|hablamos de|recibirá|vas a recibir|próximos pasos|proximos pasos)/i.test(t);
}

function detectThirdPartyAuthorizationConfirmed_(transcript) {
  const t = String(transcript || '');
  return /(authorized third party|authorization confirmed|you are authorized|I have authorization|permission to discuss|approved to speak|autorizado para hablar|autorización confirmada|tengo autorización|permiso para hablar|authorized representative|power of attorney on file|poa on file)/i.test(t);
}

function detectSharedAccountDetailsBeforeAuthorization_(transcript, authorizationConfirmed) {
  const t = String(transcript || '');

  if (!/(third party|third-party|on behalf of|de parte de|my husband|my wife|mi esposo|mi esposa|dsc|debt settlement company|settlement company|authorized representative|representative)/i.test(t)) {
    return false;
  }

  if (authorizationConfirmed) {
    return false;
  }

  return /(balance|current balance|account number|last four|original creditor|current creditor|debt|payment due|settlement offer|your account|su cuenta|saldo|número de cuenta|últimos cuatro|acreedor original|acreedor actual|deuda|oferta de liquidación)/i.test(t);
}

function createScorecards_(groups) {
  const folder = DriveApp.getFolderById(SCORECARD_FOLDER_ID);
  const out = [];

  Object.keys(groups).forEach(function(firstName) {
    const rows = groups[firstName] || [];
    if (!rows.length) return;

    const weekOf = deriveWeekOf_(rows[0][4] || rows[0][5] || '');
    const title = `${firstName} - week of ${weekOf}`;
    const ss = SpreadsheetApp.create(title);
    const file = DriveApp.getFileById(ss.getId());

    folder.addFile(file);
    try {
      DriveApp.getRootFolder().removeFile(file);
    } catch (e) {}

    const sheet = ss.getSheets()[0];

    const headers = [
      'UUID',
      'FIRST_NAME',
      'AGENT_UUID',
      'DIRECTION',
      'CREATED_AT',
      'CLT',
      'AP Portal',
      'Final Score',
      'Summary',
      'Errors',
      'Agent Comments'
    ];

    getUnifiedCriteriaList_().forEach(function(c) {
      headers.push(`C${c.id} Verdict`);
      headers.push(`C${c.id} Evidence`);
    });

    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
    sheet.getRange(2, 1, rows.length, headers.length).setValues(rows);
    sheet.setFrozenRows(1);

    if (sheet.getLastColumn() > 0) {
      sheet.autoResizeColumns(1, sheet.getLastColumn());
    }

    out.push({
      firstName: firstName,
      url: ss.getUrl(),
      title: title
    });
  });

  return out;
}

function updateScorecardLinks_(outputSheet, scorecardLinks) {
  if (!scorecardLinks || !scorecardLinks.length) return;

  const lastCol = outputSheet.getLastColumn();
  const headers = outputSheet.getRange(1, 1, 1, lastCol).getValues()[0];
  const scorecardColIndex = headers.indexOf('Scorecard') + 1;
  const firstNameColIndex = headers.indexOf('FIRST_NAME') + 1;

  if (!scorecardColIndex || !firstNameColIndex) return;

  const dataRowCount = outputSheet.getLastRow() - 1;
  if (dataRowCount <= 0) return;

  const scorecardMap = {};
  scorecardLinks.forEach(function(link) {
    scorecardMap[link.firstName] = link.url;
  });

  const firstNames = outputSheet.getRange(2, firstNameColIndex, dataRowCount, 1).getValues();
  const scorecardValues = outputSheet.getRange(2, scorecardColIndex, dataRowCount, 1).getValues();

  for (let i = 0; i < dataRowCount; i++) {
    const firstName = String(firstNames[i][0] || '').trim();
    if (firstName && scorecardMap[firstName]) {
      scorecardValues[i][0] = scorecardMap[firstName];
    } else if (String(scorecardValues[i][0] || '').indexOf('PENDING:') === 0) {
      scorecardValues[i][0] = '';
    }
  }

  outputSheet.getRange(2, scorecardColIndex, dataRowCount, 1).setValues(scorecardValues);
}

function deriveWeekOf_(value) {
  const raw = String(value || '');
  const m = raw.match(/(\d{4}-\d{2}-\d{2})/);
  return m ? m[1] : Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyy-MM-dd');
}

function indexMap_(headers) {
  const out = {};
  headers.forEach(function(h, i) {
    out[String(h).trim()] = i;
  });
  return out;
}

function validateHeaders_(idx) {
  const required = [
    'UUID', 'DIRECTION', 'COMM_CHANNEL', 'CREATED_AT', 'CLT', 'TRANSCRIPT', 'CALL_COMM_UUID',
    'ORIGINAL_CREDITOR', 'COMPANY_NAME', 'ADDRESS_STATE', 'IS_OOS', 'CALL_CODE',
    'FIRST_NAME', 'AGENT_UUID', 'ACCOUNT_NUMBER', 'STATUS'
  ];

  const missing = required.filter(function(k) {
    return idx[k] === undefined;
  });

  if (missing.length) {
    throw new Error('Missing required columns: ' + missing.join(', '));
  }
}

function normalizeDirection_(value) {
  return String(value || '').trim().toUpperCase();
}

function createProgressState_() {
  return {
    lastWriteMs: 0,
    lastProcessedWritten: -1
  };
}

function shouldWriteProgress_(state, progress, force) {
  if (force) return true;

  const now = Date.now();
  const processed = Number(progress.processed || 0);
  const total = Number(progress.total || 0);

  if (processed === 0 || processed === total) return true;
  if (processed === state.lastProcessedWritten) return false;
  if (processed % PROGRESS_WRITE_EVERY_N_ROWS !== 0) return false;
  if ((now - state.lastWriteMs) < PROGRESS_WRITE_MIN_INTERVAL_MS) return false;

  return true;
}

function setProgressThrottled_(state, progress) {
  progress = progress || {};
  const force = !!progress.force;

  if (!shouldWriteProgress_(state, progress, force)) {
    return;
  }

  const clean = {
    status: progress.status || 'idle',
    fileName: progress.fileName || '',
    total: Number(progress.total || 0),
    processed: Number(progress.processed || 0),
    written: Number(progress.written || 0),
    skipped: Number(progress.skipped || 0),
    errors: Array.isArray(progress.errors) ? progress.errors : [],
    message: String(progress.message || ''),
    updatedAt: new Date().toISOString()
  };

  PropertiesService.getDocumentProperties().setProperty(PROGRESS_KEY, JSON.stringify(clean));
  state.lastWriteMs = Date.now();
  state.lastProcessedWritten = clean.processed;
}