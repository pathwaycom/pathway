import requests

from pathway.internals.column import ColumnReference
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.trace import trace_user_frame
from pathway.io._subscribe import subscribe


@check_arg_types
@trace_user_frame
def send_alerts(alerts: ColumnReference, slack_channel_id: str, slack_token: str):
    """Sends content of a given column to the Slack channel. Each row in a column is
    a distinct message in the Slack channel.

    Args:
        alerts: ColumnReference with alerts to be sent.
        slack_channel_id: id of the channel to which alerts are to be sent.
        slack_token: token used for authenticating to Slack API.

    Example:

    >>> import os
    >>> import pathway as pw
    >>> slack_channel_id = os.environ["SLACK_CHANNEL_ID"]
    >>> slack_token = os.environ["SLACK_TOKEN"]
    >>> t = pw.debug.table_from_markdown('''
    ... alert
    ... This_is_Slack_alert
    ... ''')
    >>> pw.io.slack.send_alerts(t.alert, slack_channel_id, slack_token)
    """

    def send_slack_alert(key, row, time, is_addition):
        if not is_addition:
            return
        alert_message = row[alerts.name]
        requests.post(
            "https://slack.com/api/chat.postMessage",
            data="text={}&channel={}".format(alert_message, slack_channel_id),
            headers={
                "Authorization": "Bearer {}".format(slack_token),
                "Content-Type": "application/x-www-form-urlencoded",
            },
        ).raise_for_status()

    subscribe(alerts._table, send_slack_alert)
