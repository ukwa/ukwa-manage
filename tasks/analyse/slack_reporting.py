import os
import json
import luigi
import requests

from tasks.analyse.crawl_logs.log_analysis_hadoop import CountStatusCodes, ListDeadSeeds

SLACK_WEBHOOK_URL = os.environ.get('SLACK_WEBHOOK_URL','https://hooks.slack.com/services/Token1/Token2/Token3')

class ReportToSlackStatusCodes(luigi.Task):
    """
    Wrapper for the task it is reporting; forward output to Slack Webhook
    """

    log_paths = luigi.ListParameter()
    job = luigi.Parameter()
    launch_id = luigi.Parameter()
    on_hdfs = luigi.BoolParameter(default=False)

    task_namespace = 'analyse'

    def requires(self):
        return CountStatusCodes(self.log_paths, self.job, self.launch_id, self.on_hdfs)

    def run(self):
        with self.input().open() as report_file:
            content = report_file.read()
        SlackHook("Status Codes Aggregated", content)


class ReportToSlackDeadSeeds(luigi.Task):
    """
    Wrapper for the task it is reporting; forward output to Slack Webhook
    """

    log_paths = luigi.ListParameter()
    job = luigi.Parameter()
    launch_id = luigi.Parameter()
    on_hdfs = luigi.BoolParameter(default=False)

    task_namespace = 'analyse'

    def requires(self):
        return ListDeadSeeds(self.log_paths, self.job, self.launch_id, self.on_hdfs)

    def run(self):
        with self.input().open() as report_file:
            content = report_file.read()
        SlackHook("Latest Dead Seeds", content)


def SlackHook(title, content):
    if title == "": title = "Crawl Log Analytics"

    slack_data = {'text': title + "\nNothing reported."}
    if content:
        slack_data["text"] = title + "\n" + content

    response = requests.post(
        SLACK_WEBHOOK_URL, data=json.dumps(slack_data),
        headers={'Content-Type': 'application/json'}
    )
    if response.status_code != 200:
        raise ValueError(
            'Request to slack returned an error %s, the response is:\n%s'
            % (response.status_code, response.text)
        )

