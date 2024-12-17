/**
 * Performs custom validation on a URL, with access to details such as the URL,
 * the response from the server, configuration options and entity Details.
 *
 * To use, the "Use Custom Validation" option in the configuration spreadsheet
 * must be set to "Yes", and your custom validation code implemented within the
 * below function.
 *
 * See the documentation for this solution for further details.
 *    
 * @param {string} url The URL being checked.
 * @param {!HTTPResponse} response The response object for the request.
 * @param {!Object} options Configuration options.
 * @param {!Object} entityDetails Details of the associated Ad / Keywords etc.
 * @return {boolean} Return true if the URL and response are deemed valid.
 */
function isValidResponse(url, response, options, entityDetails) {
  /*
     Some examples of data that can be used in determining the validity of this
     URL. This is not exhaustive and there are further properties available.
  */

  // The HTTP status code, e.g. 200, 404
  // const responseCode = response.getResponseCode();

  // The HTTP response body, e.g. HTML for web pages:
  // const responseText = response.getContentText();

  // The failure strings from the configuration spreadsheet, as an array:
  // const failureStrings = options.failureStrings;

  // The type of the entity associated with the URL, e.g. Ad, Keyword, Sitelink.
  // const entityType = entityDetails.entityType;

  // The campaign name
  // const campaignName = entityDetails.campaign;

  // The ad group name, if applicable
  // const adGroupName = entityDetails.adGroup;

  // The ad text, if applicable
  // const adText = entityDetails.ad;

  // The keyword text, if applicable
  // const keywordText = entityDetails.keyword;

  // The sitelink link text, if applicable
  // const sitelinkText = entityDetails.sitelink;

  /*
   Remove comments and insert custom logic to determine whether this URL and
   response are valid, using the data obtained above.

   If valid, return true. If invalid, return false.
  */

  // Placeholder implementation treats all URLs as valid
  return true;
}

const MILLISECONDS_PER_SECOND = 1000;
const QUOTA_CONFIG = CONFIG.advanced_options.quota_config;

const SPREADSHEET_URL = CONFIG.spreadsheet_url;
const RECIPIENT_EMAILS = CONFIG.recipient_emails;
const LABEL = CONFIG.label;
const THROTTLE_SECONDS = CONFIG.throttle_seconds;
const TIMEOUT_BUFFER_SECONDS = CONFIG.timeout_buffer_seconds;
const ACCOUNT_CONDITIONS = CONFIG.account_conditions;

/**
 * Exceptions that prevent the script from finishing checking all URLs in an
 * account but allow it to resume next time.
 */
const EXCEPTIONS = {
  QPS: 'Reached UrlFetchApp QPS limit',
  LIMIT: 'Reached UrlFetchApp daily quota',
  TIMEOUT: 'Approached script execution time limit'
};

/**
 * Named ranges in the spreadsheet.
 */
const NAMES = {
  CHECK_AD_URLS: 'checkAdUrls',
  CHECK_KEYWORD_URLS: 'checkKeywordUrls',
  CHECK_SITELINK_URLS: 'checkSitelinkUrls',
  CHECK_PAUSED_ADS: 'checkPausedAds',
  CHECK_PAUSED_KEYWORDS: 'checkPausedKeywords',
  CHECK_PAUSED_SITELINKS: 'checkPausedSitelinks',
  VALID_CODES: 'validCodes',
  EMAIL_EACH_RUN: 'emailEachRun',
  EMAIL_NON_ERRORS: 'emailNonErrors',
  EMAIL_ON_COMPLETION: 'emailOnCompletion',
  SAVE_ALL_URLS: 'saveAllUrls',
  FAILURE_STRINGS: 'failureStrings',
  FREQUENCY: 'frequency',
  DATE_STARTED: 'dateStarted',
  DATE_COMPLETED: 'dateCompleted',
  DATE_EMAILED: 'dateEmailed',
  NUM_ERRORS: 'numErrors',
  RESULT_HEADERS: 'resultHeaders',
  ARCHIVE_HEADERS: 'archiveHeaders',
  USE_SIMPLE_FAILURE_STRINGS: 'useSimpleFailureStrings',
  USE_CUSTOM_VALIDATION: 'useCustomValidation'
};

function main() {
  const spreadsheet = validateAndGetSpreadsheet(SPREADSHEET_URL);
  validateEmailAddresses();
  spreadsheet.setSpreadsheetTimeZone(AdsApp.currentAccount().getTimeZone());

  const options = loadOptions(spreadsheet);
  const status = loadStatus(spreadsheet);

  if (!status.dateStarted) {
    // This is the very first execution of the script.
    startNewAnalysis(spreadsheet);
  } else if (status.dateStarted > status.dateCompleted) {
    console.log(`Resuming work from a previous execution.`);
  } else if (dayDifference(status.dateStarted, new Date()) <
             options.frequency) {
    console.log(`Waiting until ${options.frequency} days have elapsed since ` +
                `the start of the last analysis.`);
    return;
  } else {
    // Enough time has passed since the last analysis to start a new one.
    removeLabelsInAccounts();
    removeAccountLabels([LABEL]);
    startNewAnalysis(spreadsheet);
  }

  ensureAccountLabels([LABEL]);

  // Get up to 50 accounts that have not yet had all of their URLs checked.
  const accountSelector = getAccounts(false).withLimit(50);
  if (accountSelector.get().hasNext()) {
    accountSelector
        .executeInParallel('processAccount', 'processResults', JSON.stringify(options));
  } else {
    processResults([]);
  }
}

/**
 * Retrieves all accounts that either have had their URLs checked or need to
 * have their URLs checked.
 *
 * @param {boolean} isChecked True to get accounts that have been checked
 *     already, false to get accounts that have not have been checked already.
 *     Ignored if the label does not exist.
 * @return {Object} An account selector.
 */
function getAccounts(isChecked) {
  let accountSelector = AdsManagerApp.accounts();
  
  const accountLabel = getAccountLabel(LABEL);

  if (accountLabel) {
    accountSelector = accountSelector
      .withCondition(`customer_client.applied_labels ${(isChecked ? `CONTAINS ANY` : `CONTAINS NONE`)}` +
          ` ("${accountLabel.getResourceName()}")`);
  }

  if (ACCOUNT_CONDITIONS) {
    for (const condition of ACCOUNT_CONDITIONS) {
      accountSelector =
          accountSelector.withCondition(condition);
    }
  }

  return accountSelector;
}

/**
 * Removes the tracking in each account that was previously analyzed, thereby
 * clearing that account for a new analysis.
 */
function removeLabelsInAccounts() {
  const managerAccount = AdsApp.currentAccount();
  const accounts = getAccounts(true).get();

  for (const account of accounts) {
    AdsManagerApp.select(account);
    removeLabels([LABEL]);
  }

  AdsManagerApp.select(managerAccount);
}

/**
 * Performs the link checking analysis on the current account.
 *
 * @param {string} options Options from the spreadsheet as JSON.
 * @return {string} JSON stringified results of the analysis.
 */
function processAccount(options) {
  return JSON.stringify(analyzeAccount(JSON.parse(options)));
}

/**
 * Consolidates results from each account and outputs them.
 *
 * @param {Array.<Object>} executionResults A list of ExecutionResult objects.
 */
function processResults(executionResults) {
  const spreadsheet = SpreadsheetApp.openByUrl(SPREADSHEET_URL);
  const options = loadOptions(spreadsheet);
  const results = {
    urlChecks: [],
    didComplete: true
  };

  for (const result of executionResults) {
    if (!result.getError()) {
      const accountResult = JSON.parse(result.getReturnValue());
      results.urlChecks = results.urlChecks.concat(accountResult.urlChecks);
      results.didComplete = results.didComplete && accountResult.didComplete;

      if (accountResult.didComplete) {
        AdsManagerApp.accounts()
            .withIds([result.getCustomerId()])
            .get()
            .next()
            .applyLabel(LABEL);
      }
    } else {
      console.log(`Processing for ${result.getCustomerId()} failed.`);
    }
  }

  // The entire analysis is not complete if there are any accounts that have
  // not been labeled (i.e., the account was not started, or not all URLs in
  // the account have been checked).
  results.didComplete = results.didComplete &&
      !getAccounts(false).get().hasNext();

  outputResults(results, options);
}

/**
 * Checks as many new URLs as possible that have not previously been checked,
 * subject to quota and time limits.
 *
 * @param {Object} options Dictionary of options.
 * @return {Object} An object with fields for the URLs checked and an indication
 *     if the analysis was completed (no remaining URLs to check).
 */
function analyzeAccount(options) {
  // Ensure the label exists before attempting to retrieve already checked URLs.
  ensureLabels([LABEL]);

  const checkedUrls = getAlreadyCheckedUrls(options);
  const urlChecks = [];
  let didComplete = false;

  try {
    // If the script throws an exception, didComplete will remain false.
    didComplete = checkUrls(checkedUrls, urlChecks, options);
  } catch(e) {
    if (e == EXCEPTIONS.QPS ||
        e == EXCEPTIONS.LIMIT ||
        e == EXCEPTIONS.TIMEOUT) {
      console.log(`Stopped checking URLs early because: ${e}`);
      console.log(`Checked URLs will still be output.`);
    } else {
      throw e;
    }
  }

  return {
    urlChecks: urlChecks,
    didComplete: didComplete
  };
}

/**
 * Outputs the results to a spreadsheet and sends emails if appropriate.
 *
 * @param {Object} results An object with fields for the URLs checked and an
 *     indication if the analysis was completed (no remaining URLs to check).
 * @param {Object} options Dictionary of options.
 */
function outputResults(results, options) {
  const spreadsheet = SpreadsheetApp.openByUrl(SPREADSHEET_URL);

  const numErrors = countErrors(results.urlChecks, options);
  console.log(`Found ${numErrors} errors this execution.`);

  saveUrlsToSpreadsheet(spreadsheet, results.urlChecks, options);

  // Reload the status to get the total number of errors for the entire
  // analysis, which is calculated by the spreadsheet.
  status = loadStatus(spreadsheet);

  if (results.didComplete) {
    spreadsheet.getRangeByName(NAMES.DATE_COMPLETED).setValue(new Date());
    console.log(`Found ${status.numErrors} errors across the entire analysis.`);
  }

  if (RECIPIENT_EMAILS) {
    if (!results.didComplete && options.emailEachRun &&
        (options.emailNonErrors || numErrors > 0)) {
      sendIntermediateEmail(spreadsheet, numErrors);
    }

    if (results.didComplete &&
        (options.emailEachRun || options.emailOnCompletion) &&
        (options.emailNonErrors || status.numErrors > 0)) {
      sendFinalEmail(spreadsheet, status.numErrors);
    }
  }
}

/**
 * Loads data from a spreadsheet based on named ranges. Strings 'Yes' and 'No'
 * are converted to booleans. One-dimensional ranges are converted to arrays
 * with blank cells omitted. Assumes each named range exists.
 *
 * @param {Object} spreadsheet The spreadsheet object.
 * @param {Array.<string>} names A list of named ranges that should be loaded.
 * @return {Object} A dictionary with the names as keys and the values
 *     as the cell values from the spreadsheet.
 */
function loadDatabyName(spreadsheet, names) {
  const data = {};

  for (const name of names) {
    const range = spreadsheet.getRangeByName(name);

    if (range.getNumRows() > 1 && range.getNumColumns() > 1) {
      // Name refers to a 2d range, so load it as a 2d array.
      data[name] = range.getValues();
    } else if (range.getNumRows() == 1 && range.getNumColumns() == 1) {
      // Name refers to a single cell, so load it as a value and replace
      // Yes/No with boolean true/false.
      data[name] = range.getValue();
      data[name] = data[name] === 'Yes' ? true : data[name];
      data[name] = data[name] === 'No' ? false : data[name];
    } else {
      // Name refers to a 1d range, so load it as an array (regardless of
      // whether the 1d range is oriented horizontally or vertically).
      const isByRow = range.getNumRows() > 1;
      const limit = isByRow ? range.getNumRows() : range.getNumColumns();
      const cellValues = range.getValues();

      data[name] = [];
      for (let j = 0; j < limit; j++) {
        const cellValue = isByRow ? cellValues[j][0] : cellValues[0][j];
        if (cellValue) {
          data[name].push(cellValue);
        }
      }
    }
  }

  return data;
}

/**
 * Loads options from the spreadsheet.
 *
 * @param {Object} spreadsheet The spreadsheet object.
 * @return {Object} A dictionary of options.
 */
function loadOptions(spreadsheet) {
  return loadDatabyName(spreadsheet,
      [NAMES.CHECK_AD_URLS, NAMES.CHECK_KEYWORD_URLS,
       NAMES.CHECK_SITELINK_URLS, NAMES.CHECK_PAUSED_ADS,
       NAMES.CHECK_PAUSED_KEYWORDS, NAMES.CHECK_PAUSED_SITELINKS,
       NAMES.VALID_CODES, NAMES.EMAIL_EACH_RUN,
       NAMES.EMAIL_NON_ERRORS, NAMES.EMAIL_ON_COMPLETION,
       NAMES.SAVE_ALL_URLS, NAMES.FREQUENCY,
       NAMES.FAILURE_STRINGS, NAMES.USE_SIMPLE_FAILURE_STRINGS,
       NAMES.USE_CUSTOM_VALIDATION]);
}

/**
 * Loads state information from the spreadsheet.
 *
 * @param {Object} spreadsheet The spreadsheet object.
 * @return {Object} A dictionary of status information.
 */
function loadStatus(spreadsheet) {
  return loadDatabyName(spreadsheet,
      [NAMES.DATE_STARTED, NAMES.DATE_COMPLETED,
       NAMES.DATE_EMAILED, NAMES.NUM_ERRORS]);
}

/**
 * Saves the start date to the spreadsheet and archives results of the last
 * analysis to a separate sheet.
 *
 * @param {Object} spreadsheet The spreadsheet object.
 */
function startNewAnalysis(spreadsheet) {
  console.log(`Starting a new analysis.`);

  spreadsheet.getRangeByName(NAMES.DATE_STARTED).setValue(new Date());

  // Helper method to get the output area on the results or archive sheets.
  function getOutputRange(rangeName) {
    const headers = spreadsheet.getRangeByName(rangeName);
    return headers.offset(1, 0, headers.getSheet().getDataRange().getLastRow());
  }

  getOutputRange(NAMES.ARCHIVE_HEADERS).clearContent();

  const results = getOutputRange(NAMES.RESULT_HEADERS);
  results.copyTo(getOutputRange(NAMES.ARCHIVE_HEADERS));

  getOutputRange(NAMES.RESULT_HEADERS).clearContent();
}

/**
 * Counts the number of errors in the results.
 *
 * @param {Array.<Object>} urlChecks A list of URL check results.
 * @param {Object} options Dictionary of options.
 * @return {number} The number of errors in the results.
 */
function countErrors(urlChecks, options) {
  let numErrors = 0;

  for (const urlCheck of urlChecks) {
    if (options.validCodes.indexOf(urlCheck.responseCode) == -1) {
      numErrors++;
    }
  }

  return numErrors;
}

/**
 * Saves URLs for a particular account to the spreadsheet starting at the first
 * unused row.
 *
 * @param {Object} spreadsheet The spreadsheet object.
 * @param {Array.<Object>} urlChecks A list of URL check results.
 * @param {Object} options Dictionary of options.
 */
function saveUrlsToSpreadsheet(spreadsheet, urlChecks, options) {
  // Build each row of output values in the order of the columns.
  const outputValues = [];
  for (const urlCheck of urlChecks) {

    if (options.saveAllUrls ||
        options.validCodes.indexOf(urlCheck.responseCode) == -1) {
      outputValues.push([
        urlCheck.customerId,
        new Date(urlCheck.timestamp),
        urlCheck.url,
        urlCheck.responseCode,
        urlCheck.entityType,
        urlCheck.campaign,
        urlCheck.adGroup,
        urlCheck.ad,
        urlCheck.keyword,
        urlCheck.sitelink
      ]);
    }
  }

  if (outputValues.length > 0) {
    // Find the first open row on the Results tab below the headers and create a
    // range large enough to hold all of the output, one per row.
    const headers = spreadsheet.getRangeByName(NAMES.RESULT_HEADERS);
    const lastRow = headers.getSheet().getDataRange().getLastRow();
    const outputRange = headers.offset(lastRow - headers.getRow() + 1,
                                     0, outputValues.length);
    outputRange.setValues(outputValues);
  }

  for (const email of RECIPIENT_EMAILS) {
    spreadsheet.addEditor(email);
  }
}

/**
 * Sends an email to a list of email addresses with a link to the spreadsheet
 * and the results of this execution of the script.
 *
 * @param {Object} spreadsheet The spreadsheet object.
 * @param {boolean} numErrors The number of errors found in this execution.
 */
function sendIntermediateEmail(spreadsheet, numErrors) {
  spreadsheet.getRangeByName(NAMES.DATE_EMAILED).setValue(new Date());

  MailApp.sendEmail(
      RECIPIENT_EMAILS.join(','), `Link Checker Results`,
      `The Link Checker script found ${numErrors} URLs with errors in ` +
          `an execution that just finished. See ${
              spreadsheet.getUrl()} for details.`);
}

/**
 * Sends an email to a list of email addresses with a link to the spreadsheet
 * and the results across the entire account.
 *
 * @param {Object} spreadsheet The spreadsheet object.
 * @param {boolean} numErrors The number of errors found in the entire account.
 */
function sendFinalEmail(spreadsheet, numErrors) {
  spreadsheet.getRangeByName(NAMES.DATE_EMAILED).setValue(new Date());

  MailApp.sendEmail(
      RECIPIENT_EMAILS.join(','), `Link Checker Results`,
      `The Link Checker script found ${numErrors} URLs with errors ` +
          `across its entire analysis. See ${
              spreadsheet.getUrl()} for details.`);
}

/**
 * Retrieves all final URLs and mobile final URLs in the account across ads,
 * keywords, and sitelinks that were checked in a previous run, as indicated by
 * them having been labeled.
 *
 * @param {Object} options Dictionary of options.
 * @return {Object} A map of previously checked URLs with the URL as the key.
 */
function getAlreadyCheckedUrls(options) {
  const urlMap = {};

  function addToMap(items) {
    for (const item of items) {
      for(const url of expandUrlModifiers(item)){
        urlMap[url] = true;
      }
    }
  }

  if (options.checkAdUrls) {
    addToMap(getUrlsBySelector(AdsApp.ads()
                               .withCondition(labelCondition(true))));
  }

  if (options.checkKeywordUrls) {
    addToMap(getUrlsBySelector(AdsApp.keywords()
                               .withCondition(labelCondition(true))));
  }

  if (options.checkSitelinkUrls) {
    addToMap(getAlreadyCheckedSitelinkUrls());
  }

  return urlMap;
}

/**
 * Retrieves all final URLs and mobile final URLs for campaign and ad group
 * sitelinks.
 *
 * @return {Array.<string>} An array of URLs.
 */
function getAlreadyCheckedSitelinkUrls() {
  const urls = [];

  // Helper method to get campaign or ad group sitelink URLs.
  function addSitelinkUrls(selector) {
    const iterator = selector.withCondition(labelCondition(true)).get();

    for (const entity of iterator) {
      const sitelinks = entity.extensions().sitelinks();
      urls.push(...getUrlsBySelector(sitelinks));
    }
  }

  addSitelinkUrls(AdsApp.campaigns());
  addSitelinkUrls(AdsApp.adGroups());

  return urls;
}

/**
 * Retrieves all URLs in the entities specified by a selector.
 *
 * @param {Object} selector The selector specifying the entities to use.
 *     The entities should be of a type that has a urls() method.
 * @return {Array.<string>} An array of URLs.
 */
function getUrlsBySelector(selector) {
  const urls = [];
  const entities = selector.get();

  // Helper method to add the url to the list if it exists.
  function addToList(url) {
    if (url) {
      urls.push(url);
    }
  }

  for (const entity of entities) {
    addToList(entity.urls().getFinalUrl());
    addToList(entity.urls().getMobileFinalUrl());
  }

  return urls;
}

/**
 * Retrieves all final URLs and mobile final URLs in the account across ads,
 * keywords, and sitelinks, and checks their response code. Does not check
 * previously checked URLs.
 *
 * @param {Object} checkedUrls A map of previously checked URLs with the URL as
 *     the key.
 * @param {Array.<Object>} urlChecks An array into which the results of each URL
 *     check will be inserted.
 * @param {Object} options Dictionary of options.
 * @return {boolean} True if all URLs were checked.
 */
function checkUrls(checkedUrls, urlChecks, options) {
  let didComplete = true;

  // Helper method to add common conditions to ad group and keyword selectors.
  function addConditionsForAds(selector, includePaused) {
    const statuses = ['ENABLED'];
    if (includePaused) {
      statuses.push('PAUSED');
    }

    const predicate = ` IN (${statuses.join(',')})`;
    return selector.withCondition(labelCondition(false))
        .withCondition(`ad_group_ad.status${predicate}`)
        .withCondition(`campaign.status${predicate}`)
        .withCondition(`ad_group.status${predicate}`);
  }

  function addConditionsForKeywords(selector, includePaused) {
    const statuses = ['ENABLED'];
    if (includePaused) {
      statuses.push('PAUSED');
    }

    const predicate = ` IN (${statuses.join(',')})`;
    return selector.withCondition(labelCondition(false))
        .withCondition(`ad_group_criterion.status${predicate}`)
        .withCondition(`campaign.status${predicate}`)
        .withCondition(`ad_group.status${predicate}`);
  }

  if (options.checkAdUrls) {
    didComplete = didComplete && checkUrlsBySelector(checkedUrls, urlChecks,
        addConditionsForAds(AdsApp.ads()
                            .withCondition('CreativeFinalUrls != ""'),
                            options.checkPausedAds), options);
  }

  if (options.checkKeywordUrls) {
    didComplete = didComplete && checkUrlsBySelector(checkedUrls, urlChecks,
        addConditionsForKeywords(AdsApp.keywords()
                                 .withCondition('FinalUrls != ""'),
                                 options.checkPausedKeywords), options);
  }

  if (options.checkSitelinkUrls) {
    didComplete = didComplete && checkSitelinkUrls(checkedUrls, urlChecks,
        options);
  }

  return didComplete;
}

/**
 * Retrieves all final URLs and mobile final URLs in a selector and checks them
 * for a valid response code. Does not check previously checked URLs. Labels the
 * entity that it was checked, if possible.
 *
 * @param {Object} checkedUrls A map of previously checked URLs with the URL as
 *     the key.
 * @param {Array.<Object>} urlChecks An array into which the results of each URL
 *     check will be inserted.
 * @param {Object} selector The selector specifying the entities to use.
 *     The entities should be of a type that has a urls() method.
 * @param {!Object} options Dictionary of options.
 * @return {boolean} True if all URLs were checked.
 */
function checkUrlsBySelector(checkedUrls, urlChecks, selector, options) {
  const customerId = AdsApp.currentAccount().getCustomerId();
  const iterator = selector.get();
  const entities = [];

  // Helper method to check a URL.
  function checkUrl(entity, url) {
    if (!url) {
      return;
    }

    const urlsToCheck = expandUrlModifiers(url);

    for (const expandedUrl of urlsToCheck) {
      if (checkedUrls[expandedUrl]) {
        continue;
      }

      const entityType = entity.getEntityType();
      const entityDetails = {
        entityType: entityType,
        campaign: entity.getCampaign ? entity.getCampaign().getName() : '',
        adGroup: entity.getAdGroup ? entity.getAdGroup().getName() : '',
        ad: entityType == 'Ad' ? getAdAsText(entity) : '',
        keyword: entityType == 'Keyword' ? entity.getText() : '',
        sitelink: entityType.indexOf('Sitelink') != -1 ?
            entity.getLinkText() : ''
      };

      const responseCode = requestUrl(expandedUrl, options, entityDetails);

      urlChecks.push({
        customerId: customerId,
        timestamp: new Date(),
        url: expandedUrl,
        responseCode: responseCode,
        entityType: entityDetails.entityType,
        campaign: entityDetails.campaign,
        adGroup: entityDetails.adGroup,
        ad: entityDetails.ad,
        keyword: entityDetails.keyword,
        sitelink: entityDetails.sitelink
      });

      checkedUrls[expandedUrl] = true;
    }
  }

  entities.push(...iterator);

  for (const entity of entities) {
    checkUrl(entity, entity.urls().getFinalUrl());
    checkUrl(entity, entity.urls().getMobileFinalUrl());

    // Sitelinks do not have labels.
    if (entity.applyLabel) {
      entity.applyLabel(LABEL);
      checkTimeout();
    }
  }

  // True only if we did not breach an iterator limit.
  return entities.length === iterator.totalNumEntities();
}
/**
 * Retrieves a text representation of an ad, casting the ad to the appropriate
 * type if necessary.
 *
 * @param {Ad} ad The ad object.
 * @return {string} The text representation.
 */
function getAdAsText(ad) {
  // There is no AdTypeSpace method for textAd
  if (ad.getType() === 'TEXT_AD') {
    return ad.getHeadline();
  } else if (ad.isType().expandedTextAd()) {
    const eta = ad.asType().expandedTextAd();
    return eta.getHeadlinePart1() + ' - ' + eta.getHeadlinePart2();
  } else if (ad.isType().gmailImageAd()) {
    return ad.asType().gmailImageAd().getName();
  } else if (ad.isType().gmailMultiProductAd()) {
    return ad.asType().gmailMultiProductAd().getHeadline();
  } else if (ad.isType().gmailSinglePromotionAd()) {
    return ad.asType().gmailSinglePromotionAd().getHeadline();
  } else if (ad.isType().html5Ad()) {
    return ad.asType().html5Ad().getName();
  } else if (ad.isType().imageAd()) {
    return ad.asType().imageAd().getName();
  } else if (ad.isType().responsiveDisplayAd()) {
    return ad.asType().responsiveDisplayAd().getLongHeadline();
  } else if (ad.isType().responsiveSearchAd()) {
    return ad.asType()
        .responsiveSearchAd()
        .getHeadlines()
        .map(h => h.text)
        .join(', ');
  }
  return 'N/A';
}

/**
 * Retrieves all final URLs and mobile final URLs for campaign and ad group
 * sitelinks and checks them for a valid response code. Does not check
 * previously checked URLs. Labels the containing campaign or ad group that it
 * has been checked.
 *
 * @param {Object} checkedUrls A map of previously checked URLs with the URL as
 *     the key.
 * @param {Array.<Object>} urlChecks An array into which the results of each URL
 *     check will be inserted.
 * @param {Object} options Dictionary of options.
 * @return {boolean} True if all URLs were checked.
 */
function checkSitelinkUrls(checkedUrls, urlChecks, options) {
  let didComplete = true;

  // Helper method to check URLs for sitelinks in a campaign or ad group
  // selector.
  function checkSitelinkUrls(selector) {
    const iterator = selector.withCondition(labelCondition(false)).get();
    const entities = [];

    for (const iteratorElement of iterator) {
      entities.push(iteratorElement);
    }

    for (const entity of entities) {
      const sitelinks = entity.extensions().sitelinks();

      if (sitelinks.get().hasNext()) {
        didComplete = didComplete &&
            checkUrlsBySelector(checkedUrls, urlChecks, sitelinks, options);
        entity.applyLabel(LABEL);
        checkTimeout();
      }
    }

    // True only if we did not breach an iterator limit.
    didComplete = didComplete &&
        entities.length === iterator.totalNumEntities();
  }

  const statuses = ['ENABLED'];
  if (options.checkPausedSitelinks) {
    statuses.push('PAUSED');
  }

  const predicate = ` IN (${statuses.join(',')})`;
  checkSitelinkUrls(AdsApp.campaigns()
                    .withCondition(`campaign.status${predicate}`));
  checkSitelinkUrls(AdsApp.adGroups()
                    .withCondition(`ad_group.status${predicate}`)
                    .withCondition(`campaign.status${predicate}`));

  return didComplete;
}

/**
 * Expands a URL that contains ValueTrack parameters such as {ifmobile:mobile}
 * to all the combinations, and returns as an array. The following pairs of
 * ValueTrack parameters are currently expanded:
 *     1. {ifmobile:<...>} and {ifnotmobile:<...>} to produce URLs simulating
 *        clicks from either mobile or non-mobile devices.
 *     2. {ifsearch:<...>} and {ifcontent:<...>} to produce URLs simulating
 *        clicks on either the search or display networks.
 * Any other ValueTrack parameters or customer parameters are stripped out from
 * the URL entirely.
 *
 * @param {string} url The URL which may contain ValueTrack parameters.
 * @return {!Array.<string>} An array of one or more expanded URLs.
 */
function expandUrlModifiers(url) {
  const ifRegex = /({(if\w+):([^}]+)})/gi;
  const modifiers = {};
  let matches;
  let modifiedUrls;
  while (matches = ifRegex.exec(url)) {
    // Tags are case-insensitive, e.g. IfMobile is valid.
    modifiers[matches[2].toLowerCase()] = {
      substitute: matches[0],
      replacement: matches[3]
    };
  }
  if (Object.keys(modifiers).length) {
    let mobileCombinations;
    if (modifiers.ifmobile || modifiers.ifnotmobile) {
      mobileCombinations =
          pairedUrlModifierReplace(modifiers, 'ifmobile', 'ifnotmobile', url);
    } else {
      mobileCombinations = [url];
    }

    // Store in a map on the offchance that there are duplicates.
    const combinations = {};
    for (const url of mobileCombinations) {
      if (modifiers.ifsearch || modifiers.ifcontent) {
        for (const modifiedUrl of
            pairedUrlModifierReplace(modifiers, 'ifsearch', 'ifcontent', url)) {
          combinations[modifiedUrl] = true;
        }
      } else {
        combinations[url] = true;
      }
    }
    modifiedUrls = Object.keys(combinations);
  } else {
    modifiedUrls = [url];
  }
  // Remove any custom parameters
  return modifiedUrls.map(url => url.replace(/{[\w_+:]+}/g, ''));
}

/**
 * Return a pair of URLs, where each of the two modifiers is mutually exclusive,
 * one for each combination. e.g. Evaluating ifmobile and ifnotmobile for a
 * mobile and a non-mobile scenario.
 *
 * @param {Object} modifiers A map of ValueTrack modifiers.
 * @param {string} modifier1 The modifier to honour in the URL.
 * @param {string} modifier2 The modifier to remove from the URL.
 * @param {string} url The URL potentially containing ValueTrack parameters.
 * @return {Array.<string>} A pair of URLs, as a list.
 */
function pairedUrlModifierReplace(modifiers, modifier1, modifier2, url) {
  return [
    urlModifierReplace(modifiers, modifier1, modifier2, url),
    urlModifierReplace(modifiers, modifier2, modifier1, url)
  ];
}

/**
 * Produces a URL where the first {if...} modifier is set, and the second is
 * deleted.
 *
 * @param {Object} mods A map of ValueTrack modifiers.
 * @param {string} mod1 The modifier to honour in the URL.
 * @param {string} mod2 The modifier to remove from the URL.
 * @param {string} url The URL potentially containing ValueTrack parameters.
 * @return {string} The resulting URL with substitions.
 */
function urlModifierReplace(mods, mod1, mod2, url) {
  const modUrl = mods[mod1] ?
      url.replace(mods[mod1].substitute, mods[mod1].replacement) :
      url;
  return mods[mod2] ? modUrl.replace(mods[mod2].substitute, '') : modUrl;
}

/**
 * Requests a given URL. Retries if the UrlFetchApp QPS limit was reached,
 * exponentially backing off on each retry. Throws an exception if it reaches
 * the maximum number of retries. Throws an exception if the UrlFetchApp daily
 * quota limit was reached.
 *
 * @param {string} url The URL to test.
 * @param {!Object} options The options loaded from the configuration sheet.
 * @param {!Object} entityDetails Details of the entity, e.g. type, name etc.
 * @return {number|string} The response code received when requesting the URL,
 *     or an error message.
 */
function requestUrl(url, options, entityDetails) {
  let responseCode;
  let sleepTimeMillis = QUOTA_CONFIG.INIT_SLEEP_TIME_MILLIS;
  let numTries = 0;

  while (numTries < QUOTA_CONFIG.MAX_TRIES && !responseCode) {
    try {
      // If UrlFetchApp.fetch() throws an exception, responseCode will remain
      // undefined.
      const response = UrlFetchApp.fetch(url, {muteHttpExceptions: true});
      responseCode = response.getResponseCode();

      if (options.validCodes.indexOf(responseCode) !== -1) {
        if (options.useSimpleFailureStrings &&
            bodyContainsFailureStrings(response, options.failureStrings)) {
          responseCode = 'Failure string detected';
        } else if (options.useCustomValidation && !isValidResponse(url,
            response, options, entityDetails)) {
          responseCode = "Custom validation failed";
        }
      }

      if (THROTTLE_SECONDS > 0) {
        Utilities.sleep(THROTTLE_SECONDS * MILLISECONDS_PER_SECOND);
      }
    } catch(e) {
      if (e.message.indexOf('Service invoked too many times in a short time:')
          != -1) {
        Utilities.sleep(sleepTimeMillis);
        sleepTimeMillis *= QUOTA_CONFIG.BACKOFF_FACTOR;
      } else if (e.message.indexOf('Service invoked too many times:') != -1) {
        throw EXCEPTIONS.LIMIT;
      } else {
        return e.message;
      }
    }

    numTries++;
  }

  if (!responseCode) {
    throw EXCEPTIONS.QPS;
  } else {
    return responseCode;
  }
}

/**
 * Searches the body of a HTTP response for any occurrence of a "failure string"
 * as defined in the configuration spreadsheet. For example, "Out of stock".
 *
 * @param {!HTTPResponse} response The response from the UrlFetchApp request.
 * @param {!Array.<string>} failureStrings A list of failure strings.
 * @return {boolean} Returns true if at least one failure string found.
 */
function bodyContainsFailureStrings(response, failureStrings) {
  const contentText = response.getContentText() || '';
  // Whilst searching for each separate failure string across the body text
  // separately may not be the most efficient, it is simple, and tests suggest
  // it is not overly poor performance-wise.
  return failureStrings.some(
    failureString => contentText.indexOf(failureString) !== -1);
}

/**
 * Throws an exception if the script is close to timing out.
 */
function checkTimeout() {
  if (AdsApp.getExecutionInfo().getRemainingTime() < TIMEOUT_BUFFER_SECONDS) {
    throw EXCEPTIONS.TIMEOUT;
  }
}

/**
 * Returns the number of days between two dates.
 *
 * @param {Object} from The older Date object.
 * @param {Object} to The newer (more recent) Date object.
 * @return {number} The number of days between the given dates (possibly
 *     fractional).
 */
function dayDifference(from, to) {
  return (to.getTime() - from.getTime()) / (24 * 3600 * 1000);
}

/**
 * Builds a string to be used for withCondition() filtering for whether the
 * label is present or not.
 *
 * @param {boolean} hasLabel True if the label should be present, false if the
 *     label should not be present.
 * @return {string} A condition that can be used in withCondition().
 */
function labelCondition(hasLabel) {
  return `LabelNames ${(hasLabel ? `CONTAINS_ANY` : `CONTAINS_NONE`)}` +
      ` ["${LABEL}"]`;
}

/**
 * Retrieves an entity by name.
 *
 * @param {Object} selector A selector for an entity type with a Name field.
 * @param {string} name The name to retrieve the entity by.
 * @return {Object} The entity, if it exists, or null otherwise.
 */
function getEntityByName(selector, name) {
  const entities = selector.withCondition(`Name = "${name}"`).get();

  if (entities.hasNext()) {
    return entities.next();
  } else {
    return null;
  }
}

/**
 * Retrieves a Label object by name.
 *
 * @param {string} labelName The label name to retrieve.
 * @return {Object} The Label object, if it exists, or null otherwise.
 */
function getLabel(labelName) {
  return getEntityByName(AdsApp.labels(), labelName);
}

/**
 * Retrieves an AccountLabel object by name.
 *
 * @param {string} labelName The label name to retrieve.
 * @return {Object} The AccountLabel object, if it exists, or null otherwise.
 */
function getAccountLabel(labelName) {
  return getEntityByName(AdsManagerApp.accountLabels(), labelName);
}

/**
 * Checks that the account has all provided labels and creates any that are
 * missing. Since labels cannot be created in preview mode, throws an exception
 * if a label is missing.
 *
 * @param {Array.<string>} labelNames An array of label names.
 */
function ensureLabels(labelNames) {
  for (const labelName of labelNames) {
    const label = getLabel(labelName);

    if (!label) {
      if (!AdsApp.getExecutionInfo().isPreview()) {
        AdsApp.createLabel(labelName);
      } else {
        throw `Label ${labelName} is missing and cannot be created in ` +
            `preview mode. Please run the script or create the label manually.`;
      }
    }
  }
}

/**
 * Checks that the account has all provided account labels and creates any that
 * are missing. Since labels cannot be created in preview mode, throws an
 * exception if a label is missing.
 *
 * @param {Array.<string>} labelNames An array of label names.
 */
function ensureAccountLabels(labelNames) {
  for (const labelName of labelNames) {
    const label = getLabel(labelName);

    if (!label) {
      if (!AdsApp.getExecutionInfo().isPreview()) {
        AdsManagerApp.createAccountLabel(labelName);
      } else {
        throw `Account label ${labelName} is missing and cannot be ` +
            `created in preview mode. Please run the script or create the ` +
            `label manually.`;
      }
    }
  }
}

/**
 * Removes all provided labels from the account. Since labels cannot be removed
 * in preview mode, throws an exception in preview mode.
 *
 * @param {Array.<string>} labelNames An array of label names.
 */
function removeLabels(labelNames) {
  if (AdsApp.getExecutionInfo().isPreview()) {
    throw 'Cannot remove labels in preview mode. Please run the script or ' +
        'remove the labels manually.';
  }

  for (const labelName of labelNames) {
    const label = getLabel(labelName);

    if (label) {
      label.remove();
    }
  }
}

/**
 * Removes all provided account labels from the account. Since labels cannot be
 * removed in preview mode, throws an exception in preview mode.
 *
 * @param {Array.<string>} labelNames An array of label names.
 */
function removeAccountLabels(labelNames) {
  if (AdsApp.getExecutionInfo().isPreview()) {
    throw 'Cannot remove account labels in preview mode. Please run the ' +
        'script or remove the labels manually.';
  }

  for (const labelName of labelNames) {
    const label = getLabel(labelName);

    if (label) {
      label.remove();
    }
  }
}

/**
 * Validates the provided spreadsheet URL to make sure that it's set up
 * properly. Throws a descriptive error message if validation fails.
 *
 * @param {string} spreadsheeturl The URL of the spreadsheet to open.
 * @return {Spreadsheet} The spreadsheet object itself, fetched from the URL.
 * @throws {Error} If the spreadsheet URL hasn't been set
 */
function validateAndGetSpreadsheet(spreadsheeturl) {
  if (spreadsheeturl == 'YOUR_SPREADSHEET_URL') {
    throw new Error('Please specify a valid Spreadsheet URL. You can find' +
        ' a link to a template in the associated guide for this script.');
  }
  return SpreadsheetApp.openByUrl(spreadsheeturl);
}

/**
 * Validates the provided email address to make sure it's not the default.
 * Throws a descriptive error message if validation fails.
 *
 * @throws {Error} If the list of email addresses is still the default
 */
function validateEmailAddresses() {
  if (RECIPIENT_EMAILS && RECIPIENT_EMAILS[0] == 'YOUR_EMAIL_HERE') {
    throw new Error('Please either specify a valid email address or clear' +
        ' the RECIPIENT_EMAILS field.');
  }
}
