// Google Apps Script prototype for a Sheets sidebar that evaluates call transcripts
// from an uploaded CSV sent directly from the sidebar.
//
// Updated per Ashley comments:
// - Removes follow-up criterion
// - Removes settlement-entry criterion
// - Removes payment-plan setup/edit criterion
// - Removes settlement negotiation criterion
// - Contact info criterion is N/A for unauthorized third-party calls
// - Creditor reporting timeline only applies when debt is actually being fully resolved
// - Badge number gets credit if opening is cut off / recording starts mid-call
// - Company-name disclosure criterion will not fail if borrower explicitly asks who is calling
// - Call recap is N/A if borrower disconnects early
// - Scorecard links now contain ALL calls for each FIRST_NAME, not just one row

const OUTPUT_SHEET_NAME = 'QA Evaluations';
const PROGRESS_KEY = 'JAN_QA_PROGRESS';
const DEFAULT_MODEL = 'claude-sonnet-4-6';
const ROW_BATCH_WRITE_SIZE = 10;
const SLEEP_MS_BETWEEN_ROWS = 150;
const SCORECARD_FOLDER_ID = '1VV7lo0p0qLUgIdwWgqdZ57k-co8W0vXE';

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
      message: 'Ready.'
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

  setProgress_({
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

    setProgress_({
      status: 'running',
      fileName: options.fileName,
      total: total,
      processed: processed,
      written: written,
      skipped: skipped,
      errors: errors.slice(-10),
      message: `Evaluating row ${k + 1} of ${total}...`
    });

    try {
      const rowCtx = buildRowContext_(row, idx);
      const prompt = buildEvaluationPrompt_(rowCtx);
      const responseText = callClaudeSingle_(prompt);
      const parsed = parseModelJsonSafe_(responseText, prompt, rowCtx);
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

    setProgress_({
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

  setProgress_({
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

  const ashleyRules = [
    'Ashley QA rules:',
    '- Creditor reporting timeline is ONLY applicable if the borrower is making a full one-time resolution payment / debt is being fully resolved.',
    '- Do NOT fail the company-name disclosure criterion if the borrower explicitly asked who was calling.',
    '- Do NOT fail the badge-number criterion if the opening of the recording appears cut off or starts mid-call.',
    '- If the call is with an unauthorized third party, contact info confirmation/update can be N/A.',
    '- If the borrower disconnects early, recap/summary and later wrap-up items can be N/A.',
    '- Mini-Miranda / collection disclosure does not have to happen in one specific order unless the criterion explicitly says it must.',
    '- Removed criteria should not be invented or evaluated.'
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
    '- Be conservative.',
    '- If evidence is missing, mark No or N/A.',
    '- Evidence must come directly from the transcript.',
    '- Evidence must be short, maximum one sentence.',
    '- Use only the criterion numbers listed below.',
    '- Identify the agent name from the transcript if possible, otherwise use Unknown.',
    '- Do not evaluate deleted criteria.',
    '- For criteria marked not applicable, return N/A.',
    '',
    `Call direction: ${rowCtx.direction}`,
    `Borrower state: ${rowCtx.addressState || 'Unknown'}`,
    `IS_OOS: ${rowCtx.isOos ? 'OOS' : 'Not OOS / Unknown'}`,
    `CALL_CODE: ${rowCtx.callCode || 'Unknown'}`,
    `Company Name: ${rowCtx.companyName || 'Unknown'}`,
    `Language detected: ${rowCtx.language}`,
    stateRule,
    '',
    ashleyRules,
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
    const verdict = c.applicable ? normalizeVerdict_(raw.verdict || 'No') : 'N/A';
    const evidence = c.applicable ? String(raw.evidence || '') : c.naReason;

    return {
      id: c.id,
      criterion: c.criterion,
      verdict: verdict,
      evidence: evidence
    };
  });

  // Ashley override: if opening is cut off, give badge-number credit.
  const badgeId = rowCtx.direction === 'INBOUND' ? 29 : 31;
  const badgeEval = normalizedEvaluations.filter(function(ev) { return ev.id === badgeId; })[0];
  if (badgeEval && openingLooksCutOff_(rowCtx.transcript)) {
    badgeEval.verdict = 'Yes';
    badgeEval.evidence = 'Opening portion of recording appears cut off; badge number treated as satisfied per QA rule.';
  }

  // Ashley override: if borrower explicitly asked who was calling, do not fail company-name disclosure order.
  if (rowCtx.direction === 'OUTBOUND' && rowCtx.flags.askedWhoCalling) {
    const companyDisclosure = normalizedEvaluations.filter(function(ev) { return ev.id === 12; })[0];
    if (companyDisclosure) {
      companyDisclosure.verdict = 'Yes';
      companyDisclosure.evidence = 'Borrower explicitly asked who was calling, so company disclosure order exception applies.';
    }
  }

  const applicable = normalizedEvaluations.filter(function(ev) {
    return ev.verdict !== 'N/A';
  });

  const yesCount = applicable.filter(function(ev) {
    return ev.verdict === 'Yes';
  }).length;

  const finalScore = applicable.length
    ? Math.round((yesCount / applicable.length) * 100)
    : 0;

  return {
    agent_name: model.agent_name || 'Unknown',
    summary: model.summary || '',
    errors: buildErrorsText_(normalizedEvaluations),
    final_score: finalScore,
    evaluations: normalizedEvaluations
  };
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
    Number(model.final_score || 0),
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

function writeOutputHeader_(sheet) {
  const headers = [
    'UUID', 'FIRST_NAME', 'AGENT_UUID', 'Direction', 'Agent', 'Evaluator', 'Date of Survey', 'Date', 'Time',
    'Language', 'ACCOUNT_NUMBER', 'AP Portal', 'Comm Channel', 'Created At', 'Call Comm UUID', 'Original Creditor',
    'Company Name', 'Address State', 'Final Score', 'Summary', 'Errors', 'Scorecard'
  ];

  getUnifiedCriteriaList_().forEach(function(c) {
    headers.push(`C${c.id} Verdict`);
    headers.push(`C${c.id} Evidence`);
  });

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
  const t = String(transcript || '').toLowerCase();
  const code = String(callCode || '').toLowerCase();
  const isOos = String(isOosRaw || '').toUpperCase() === 'OOS';
  const isPaid = /\bpaid\b|paid in full|resolved|zero balance/i.test(status) || /paid in full|resolved your debt|zero balance/i.test(t);

  const hasThirdPartyJoin = /(new[, ]+third party|third party joined|joined the call|someone else('|’)s debit card|someone else('|’)s card|authorized to participate on a call)/i.test(t);
  const unauthorizedThirdParty = /(calling on behalf of|on behalf of someone else|not authorized|unauthorized third party|i am her husband|i am his wife|power of attorney|poa)/i.test(t);
  const paymentMade = /(payment (was )?successfully processed|processing that now|do i have your authorization to proceed with this payment|making a payment of|partial payment of|card ending in|authorization to proceed)/i.test(t);
  const paymentInfoGiven = /(card ending in|debit card|authorization to proceed with this payment|payment method|draft from|using your card ending in|routing number|account number)/i.test(t);
  const settlementRequested = /(settlement|settle|reduced amount|reduction|offer of|negotiat)/i.test(t) || code === 'settlement';
  const settlementOffer = /(offer of|approve an offer|counter offer|new balance is|settlement amount|apply that offer)/i.test(t) || code === 'settlement';
  const disputeDiscussed = /(dispute|fraud|identity theft|not responsible|do not recognize this debt)/i.test(t) || code === 'dispute';
  const oneTimePaymentDiscussed = /(one[- ]time payment|one time payment|pay today|make a payment today|single payment|lump sum)/i.test(t);
  const paymentPlanDiscussed = /(payment plan|monthly payment|draft each month|draft each week|reoccurring payments|recurring payments|schedule it out)/i.test(t);
  const companyRequiresClientSpecificDispute = companyRequiresSpecificCompanyRule_(companyName);
  const companyRequiresStipulation = companyRequiresSpecificCompanyRule_(companyName);
  const aboutSettlement = settlementRequested || settlementOffer;
  const resolvedPayment = /(paid in full|full negotiated payment amount|resolve the account|resolved account|settled in full|full payment|one-time resolution)/i.test(t);
  const transferOccurred = /(transfer you|connect you with|direct your call|specialist|transferred)/i.test(t);
  const askedWhoCalling = /(who is this|who's calling|who am i speaking with|what company is this|who called me)/i.test(t);
  const borrowerDisconnectedEarly = /(call dropped|disconnect|disconnected|hang up|hung up|line went dead)/i.test(t);

  return {
    hasThirdPartyJoin: hasThirdPartyJoin,
    unauthorizedThirdParty: unauthorizedThirdParty,
    paymentMade: paymentMade,
    paymentInfoGiven: paymentInfoGiven,
    settlementRequested: settlementRequested,
    settlementOffer: settlementOffer,
    disputeDiscussed: disputeDiscussed,
    oneTimePaymentDiscussed: oneTimePaymentDiscussed,
    paymentPlanDiscussed: paymentPlanDiscussed,
    companyRequiresClientSpecificDispute: companyRequiresClientSpecificDispute,
    companyRequiresStipulation: companyRequiresStipulation,
    aboutSettlement: aboutSettlement,
    isPaid: isPaid,
    isOos: isOos,
    resolvedPayment: resolvedPayment,
    transferOccurred: transferOccurred,
    askedWhoCalling: askedWhoCalling,
    borrowerDisconnectedEarly: borrowerDisconnectedEarly
  };
}

function companyRequiresSpecificCompanyRule_(companyName) {
  const normalized = normalizeCompanyName_(companyName);
  return STIPULATION_COMPANIES.map(normalizeCompanyName_).indexOf(normalized) !== -1;
}

function normalizeCompanyName_(value) {
  return String(value || '').trim().toLowerCase();
}

function getCriteriaForRow_(rowCtx) {
  const flags = rowCtx.flags;

  if (rowCtx.direction === 'INBOUND') {
    return [
      crit_(1, 'Agent authenticated the account by requesting at least two identifiers from the borrower (DOB, last 4 SSN, address, or login PIN).', true),
      crit_(2, 'Agent stated the original and current creditor, last 4 of account number, description of debt, and current balance.', true),
      crit_(3, 'Agent did not use discriminatory language or behavior.', true),
      crit_(4, 'Agent followed payment compliance guidelines, including required third-party authorization when applicable.', flags.paymentMade, 'Not evaluated because no payment activity occurred.'),
      crit_(5, 'Agent provided the Mini-Miranda / collection disclosure.', true),
      crit_(6, 'Agent provided the recording disclosure if a new third party joined the call.', flags.hasThirdPartyJoin, 'Not evaluated because no new third party joined the call.'),
      crit_(7, 'Agent provided the settlement disclosure.', flags.aboutSettlement, 'Not evaluated because the call was not about a settlement.'),
      crit_(8, 'Agent confirmed or updated the borrower preferred language for a New York resident.', rowCtx.addressState === 'NY', 'Not evaluated because borrower is not a New York resident.'),
      crit_(9, 'Agent provided the OOS disclosure.', rowCtx.isOos, 'Not evaluated because IS_OOS is not OOS.'),
      crit_(10, 'Agent addressed borrower questions and concerns fully and factually without UDAAP risk.', true),
      crit_(11, 'Agent confirmed or updated borrower contact info (phone, email, address).', !flags.unauthorizedThirdParty, 'Not evaluated because this was an unauthorized third-party call.'),
      crit_(12, 'Agent demonstrated call control.', true),
      crit_(13, 'Agent was personable and expressed empathy.', true),
      crit_(16, 'Agent filed a dispute under the correct category and advised borrower to send supporting documents when applicable.', flags.disputeDiscussed, 'Not evaluated because no dispute was discussed.'),
      crit_(21, 'Agent paused and resumed recording while taking payment information.', flags.paymentInfoGiven, 'Not evaluated because payment information was not taken on the call.'),
      crit_(22, 'Agent utilized probing questions for a one-time payment.', flags.oneTimePaymentDiscussed, 'Not evaluated because a one-time payment was not discussed.'),
      crit_(25, 'Agent provided January timetable for reporting a resolved account to the creditor.', flags.resolvedPayment, 'Not evaluated because the debt was not being fully resolved on this call.'),
      crit_(26, 'Agent provided a recap or summary of the call.', !flags.borrowerDisconnectedEarly, 'Not evaluated because borrower disconnected early.'),
      crit_(27, 'Agent correctly addressed a Klarna or Cavalry dispute.', flags.companyRequiresClientSpecificDispute && flags.disputeDiscussed, 'Not evaluated because this was not a Klarna/Cavalry dispute call.'),
      crit_(28, 'Agent informed borrower of BCU, RBFCU, or Flexible Finance stipulation prior to offering a settlement.', flags.companyRequiresStipulation && flags.aboutSettlement, 'Not evaluated because stipulation did not apply.'),
      crit_(29, 'Agent stated their badge number.', true),
      crit_(32, 'Agent transferred the call or account to the appropriate party when needed.', flags.transferOccurred, 'Not evaluated because no transfer occurred.')
    ];
  }

  return [
    crit_(1, 'Agent authenticated the account by requesting at least two identifiers from the borrower (DOB, last 4 SSN, address, or login PIN).', true),
    crit_(2, 'Agent stated the original and current creditor, last 4 of account number, description of debt, and current balance.', true),
    crit_(4, 'Agent did not use discriminatory language or behavior.', true),
    crit_(5, 'Agent recited Mini-Miranda and recording disclosure before providing account information.', true),
    crit_(6, 'Agent did not disclose the purpose of the call or leave a message with an unauthorized third party.', true),
    crit_(7, 'Agent followed payment compliance guidelines, including payment plan disclosure and EFTA compliance when applicable.', flags.paymentMade || flags.paymentPlanDiscussed, 'Not evaluated because no payment or payment-plan activity occurred.'),
    crit_(8, 'Agent provided the OOS disclosure.', rowCtx.isOos, 'Not evaluated because IS_OOS is not OOS.'),
    crit_(9, 'Agent provided the settlement disclosure.', flags.aboutSettlement, 'Not evaluated because the call was not about a settlement.'),
    crit_(10, 'Agent confirmed or updated the borrower preferred language for a New York resident.', rowCtx.addressState === 'NY', 'Not evaluated because borrower is not a New York resident.'),
    crit_(11, 'Agent provided recording disclosure if a new third party joined the phone call.', flags.hasThirdPartyJoin, 'Not evaluated because no new third party joined the call.'),
    crit_(12, 'Agent did not disclose company name before authentication unless explicitly asked by the borrower.', true),
    crit_(13, 'Agent addressed borrower questions and concerns fully and factually without UDAAP risk.', true),
    crit_(14, 'Agent confirmed or updated borrower contact info (phone, email, address).', !flags.unauthorizedThirdParty && !flags.isPaid && !flags.isOos, 'Not evaluated because contact update was not required for this call.'),
    crit_(15, 'Agent was personable and expressed empathy or professionalism.', true),
    crit_(18, 'Agent filed a dispute under the correct category and advised borrower to send supporting documents when applicable.', flags.disputeDiscussed, 'Not evaluated because no dispute was discussed.'),
    crit_(22, 'Agent properly ceased the account under the rightful category when required.', /(cease|cease and desist|stop calling|do not contact)/i.test(rowCtx.transcript), 'Not evaluated because no cease request occurred.'),
    crit_(28, 'Agent provided January timetable for reporting a resolved account to the creditor.', flags.resolvedPayment, 'Not evaluated because the debt was not being fully resolved on this call.'),
    crit_(29, 'Agent correctly addressed a Klarna or Cavalry dispute.', flags.companyRequiresClientSpecificDispute && flags.disputeDiscussed, 'Not evaluated because this was not a Klarna/Cavalry dispute call.'),
    crit_(30, 'Agent informed borrower of BCU, RBFCU, or Flexible Finance stipulation prior to offering a settlement.', flags.companyRequiresStipulation && flags.aboutSettlement, 'Not evaluated because stipulation did not apply.'),
    crit_(31, 'Agent stated their badge number.', true),
    crit_(34, 'Agent transferred the call or account to the appropriate party when needed.', flags.transferOccurred, 'Not evaluated because no transfer occurred.')
  ];
}

function buildErrorsText_(evaluations) {
  const deficiencies = evaluations
    .filter(function(ev) { return ev.verdict === 'No'; })
    .map(function(ev) {
      const evidence = String(ev.evidence || '').trim();
      return 'C' + ev.id + ' ' + ev.criterion + ': Not satisfied' + (
        evidence ? ' — ' + evidence : ' — No supporting evidence found in transcript.'
      );
    });

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
    { id: 21 }, { id: 22 }, { id: 25 }, { id: 26 }, { id: 27 }, { id: 28 }, { id: 29 }, { id: 30 },
    { id: 31 }, { id: 32 }, { id: 34 }
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

function parseModelJsonSafe_(text, originalPrompt, rowCtx) {
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
      const repaired = repairJsonResponse_(text, originalPrompt, rowCtx);
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

function repairJsonResponse_(badText, originalPrompt, rowCtx) {
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
    'necesito', 'puede', 'habla espanol', 'español', 'usted'
  ];

  let count = 0;
  for (let i = 0; i < spanishHints.length; i++) {
    if (t.indexOf(spanishHints[i]) !== -1) count++;
  }

  return count >= 2 ? 'Spanish' : 'English';
}

function isVoicemailTranscript_(transcript) {
  const t = String(transcript || '').toLowerCase();
  return /voicemail|limited content message|please call us back|leave a message|beep|tone after the beep/.test(t);
}

function openingLooksCutOff_(transcript) {
  const t = String(transcript || '').trim();
  const start = t.slice(0, 250).toLowerCase();

  if (!start) return false;

  if (/thank you for calling|this call will be recorded|my name is|january|debt collection agency|attempt to collect a debt/.test(start)) {
    return false;
  }

  return true;
}

function buildScorecardRecord_(model, meta) {
  return [
    meta.uuid || '',
    meta.firstName || '',
    meta.agentUuid || '',
    meta.direction || '',
    meta.createdAt || '',
    meta.clt || '',
    meta.apPortal || '',
    Number(meta.finalScore || 0),
    meta.summary || '',
    meta.errors || '',
    ''
  ];
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
    const headers = ['UUID', 'FIRST_NAME', 'AGENT_UUID', 'DIRECTION', 'CREATED_AT', 'CLT', 'AP Portal', 'Final Score', 'Summary', 'Errors', 'Agent Comments'];

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
    'FIRST_NAME', 'AGENT_UUID', 'ACCOUNT_NUMBER'
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

function setProgress_(progress) {
  PropertiesService.getDocumentProperties().setProperty(PROGRESS_KEY, JSON.stringify(progress));
}