# MCC Link Checker

This is a modified version of the Google Ads Link Checker script with the following enhancements:

1. Account Filtering: Only processes accounts with the 'TB_Script' label
2. Email Routing: Sends error notifications to email addresses found in account labels containing '@traffic-builders.com'
3. Account-Specific Reporting: Each account's errors are reported only to its associated email addresses

## Setup Instructions

1. Create a copy of the template spreadsheet
2. Update the 'spreadsheet_url' in config.gs
3. Add the 'TB_Script' label to accounts you want to monitor
4. Add email labels to accounts (e.g., 'john@traffic-builders.com')
5. Deploy the script in your Google Ads account

## Files

- info.gs: Script information and version history
- config.gs: Configuration settings
- account-utils.gs: Account and email handling utilities
- code.gs: Main implementation

## Configuration

See config.gs for all available configuration options.
