/**
 * Configuration to be used for the Link Checker.
 */
CONFIG = {
  // URL of the spreadsheet template.
  // This should be a copy of
  // https://docs.google.com/spreadsheets/d/1iO1iEGwlbe510qo3Li-j4KgyCeVSmodxU6J7M756ppk/copy.
  'spreadsheet_url': 'YOUR_SPREADSHEET_URL',
  
  // Label to use when a link has been checked.
  'label': 'LinkChecker_Done',
  
  // Label that identifies accounts to be processed
  'account_filter_label': 'TB_Script',
  
  // Email label identifier
  'email_label_pattern': '@traffic-builders.com',
  
  // Number of seconds to sleep after each URL request.
  'throttle_seconds': 0,
  
  // Number of seconds before timeout
  'timeout_buffer_seconds': 120,
  
  'advanced_options': {
    'quota_config': {
      'INIT_SLEEP_TIME_MILLIS': 250,
      'BACKOFF_FACTOR': 2,
      'MAX_TRIES': 5
    }
  }
};