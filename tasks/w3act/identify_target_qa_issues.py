#!/usr/bin/env python
# coding: utf-8

"""
Identifies Targets that:
1) Potentially infringe the Regulations (scoping in non-UK websites as "legal deposit") and,
2) Indicate unnecessary crawling (too deep, too frequent) which has implications for the crawlers and for storage, and which has a cost associated with it.

Params
-d lookback days (default 7)
-m mail recipient(s); if multiple, enclose in quotes (default needs manually coding if in public repo)

Usage examples:
[python3 identify_target_qa_issues.py...]

identify_target_qa_issues.py    >>> default email address & look back days (7)
identify_target_qa_issues.py -d 30   >>> default email address, look back 30 days
identify_target_qa_issues.py -m person@x.com   >>> single recipient
identify_target_qa_issues.py -d 30 -m "person1@x.com, person2@y.com"   >>> multiple recipients, look back 30 days

"""

import sys
import pandas as pd
import json
import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import argparse
import re
import logging

w3act_target_url_prefix = 'https://www.webarchive.org.uk/act/targets/'

# maybe not for public repos
default_email_list = 'contact1@x.com, contact2@x.com, contact3@y.com'
data_dir = './'
#

output_file = './target_issues.csv'
log_file = "./target_issues.log"
log_level = 'INFO'

if log_level == 'INFO':
    logging.basicConfig(filename=log_file, format='%(message)s', level=log_level)
else:
    logging.basicConfig(filename=log_file, format='%(asctime)s [%(levelname)s] %(message)s', level=log_level)

logging.info("=====================")
logging.info("Started identify_target_qa_issues.py")


def invalid_URL(urls):
    # true returned if:
    # url doesn't end in a slash or an extension
    try:
        primary_seed = urls[0]
        _, *_, last = primary_seed.split('/')  # last entry in url

        if last == '':  # ends /
            return False  # ends / = ok
        if '.' in last:
            return False  # extension = ok
    except:
        return True  # corrupt url - flag it

    # no extension, no trailing /
    return True


# initialise from args
parser = argparse.ArgumentParser('Report Targets That May Have QA Issues.')
parser.add_argument('-d', '--days', dest='lookback_days', type=int, default=7, required=False,
                    help="Days to look back [default: %(default)s]")
parser.add_argument('-m', '--mailto', dest='email_list', type=str, default=default_email_list, required=False,
                    help='List of email recipients. [default: %(default)s]')
args = parser.parse_args()

email_to = args.email_list
lookback_days = args.lookback_days

# Load the targets from the daily json dump
today = datetime.date.today()
data_file = data_dir + str(today)[0:7] + '/' + str(today) + '-w3act-csv-all.json'

try:
    with open(data_file) as f:
        all = json.load(f)
except IOError:
    logging.error("Unable to access today's w3act data file: " + data_file)
    sys.exit(0)

targets = all['targets']

df = pd.DataFrame(targets)
df = df.transpose()

# Targets looking back n days
df_scope = df[(pd.to_datetime(df.created_at) > (pd.to_datetime('today') - pd.DateOffset(days=lookback_days + 1)))]

# Frequent Crawl issues - targets assigned a daily/weekly schedule with no end date
df_issue_frequent = df_scope[df_scope.crawl_frequency.isin(['DAILY', 'WEEKLY']) & (df_scope.crawl_end_date == '')].copy()
df_issue_frequent["issue_reason"] = "No End Date"
df_issue_frequent["issue_info"] = df_scope.crawl_frequency

# Uncapped targets
df_issue_capped = df_scope[~df_scope.depth.isin(['CAPPED', 'CAPPED_LARGE'])].copy()
df_issue_capped["issue_reason"] = "Uncapped"
df_issue_capped["issue_info"] = df_scope.depth

# Potentially non-UK
df_issue_uk_scope = df_scope[df_scope.professional_judgement].copy()
df_issue_uk_scope["issue_reason"] = "Professional Judgement"
df_issue_uk_scope["issue_info"] = df_scope.professional_judgement_exp

# Temp df used because I couldn't get apply() to work on the original
df_temp = df_scope.copy()

# Create a column that flags invalid URLS
df_temp['invalid'] = df_temp['urls'].apply(invalid_URL)

# A trailing slash is not included at the end of the starting seed (unless it ends in a tld or file name extension)
df_issue_urls = df_temp[df_temp['invalid']].copy()
df_issue_urls["issue_reason"] = "URL Should End /"
df_issue_urls["issue_info"] = df_temp.urls

# Bring the separate issues together
df_target_issues = pd.concat([df_issue_frequent, df_issue_capped, df_issue_uk_scope, df_issue_urls])

# Get curator info...
curators=pd.DataFrame(all['curators']).transpose()
df_target_issues = df_target_issues.join(curators[['name', 'email']], on='author_id', how='inner')

# ...and organisation
organisations=pd.DataFrame(all['organisations']).transpose()
df_target_issues = df_target_issues.join(organisations[['title']], on='organisation_id', rsuffix='_organisation', lsuffix='_target', how='inner')


# Add a link to the problem record
df_target_issues['W3ACT URL'] = w3act_target_url_prefix + df_target_issues.id.astype(str)

# Get rid of the columns we aren't reporting on
df_target_issues = df_target_issues[['title_organisation', 'name', 'email', 'id', 'title_target','issue_reason', 'issue_info', 'depth', 'crawl_end_date', 'W3ACT URL']]

# Rename for presentation
df_target_issues.columns = ['Organisation', 'User', 'Email', 'Target ID', 'Title', 'Issue', 'Info', 'Depth', 'Crawl End Date', "Target URL"]

df_target_issues.index.name = 'Target ID'

# Output ready to email
df_target_issues.to_csv(output_file)

# Initialise the email
msg = MIMEMultipart()
msg['From'] = 'TargetIssues@bl.uk'
msg['Subject'] = 'Targets Flagged as Having QA Issues'

# Attach the csv
try:
    with open(output_file, 'r', encoding="utf8") as f:
        attachment = MIMEText(f.read())
        attachment.add_header('Content-Disposition', 'attachment', filename="issues.csv")
        msg.attach(attachment)   
except IOError:
    logging.error("Unable to attach csv file: " + output_file)
    sys.exit(0)

s = smtplib.SMTP('juno.bl.uk')

# I couldn't find a workable single call with multiple recipients and attachments, so loop instead
for contact in email_to.split(','):
    email = contact.strip()
    if not re.match(r"[^@]+@[^@]+\.[^@]+", email): # very basic validation on email address
        logging.error("Invalid email address: " + email)
        break
    msg['To'] = email
    s.send_message(msg)
    del msg['to'] # required; setting msg['To'] above adds a new email header - we want to overwrite it

s.quit()

logging.info("Finished")
logging.info("...")


