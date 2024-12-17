/**
 * Utility functions for account and email handling
 */

function getAccountEmailAddresses(account) {
  const emailLabels = account.labels()
    .withCondition(`LabelName CONTAINS "${CONFIG.email_label_pattern}"`)
    .get();
  
  const emails = [];
  while (emailLabels.hasNext()) {
    const label = emailLabels.next();
    const labelName = label.getName();
    // Extract email from label name
    emails.push(labelName);
  }
  
  return emails;
}

function shouldProcessAccount(account) {
  return account.labels()
    .withCondition(`LabelName = "${CONFIG.account_filter_label}"`)
    .get()
    .hasNext();
}

function sendAccountSpecificEmail(account, numErrors, spreadsheet, isFinal) {
  const emails = getAccountEmailAddresses(account);
  
  if (emails.length === 0) {
    Logger.log(`No email addresses found for account ${account.getName()}`);
    return;
  }
  
  const subject = `Link Checker Results - ${account.getName()}`;
  const body = isFinal ?
    `The Link Checker script found ${numErrors} URLs with errors in account ${account.getName()}. ` +
    `See ${spreadsheet.getUrl()} for details.` :
    `The Link Checker script found ${numErrors} URLs with errors in an execution that just finished ` +
    `for account ${account.getName()}. See ${spreadsheet.getUrl()} for details.`;
  
  MailApp.sendEmail(emails.join(','), subject, body);
}
