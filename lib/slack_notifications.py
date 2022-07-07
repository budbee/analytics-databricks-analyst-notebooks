import requests
import json


def _post_message_to_slack(msg):
    data = {'text': msg}
    headers = {'Content-type': 'application/json'}
    r = requests.post('https://hooks.slack.com/services/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', data=json.dumps(data), headers=headers)

def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    msg = f''':alert:*SLA Miss*\n\n
*DAG:* {dag.dag_id}\n
*Task:* {slas[0].task_id}\n'''
    _post_message_to_slack(msg)

def dag_failure(context):
    msg = f''':alert:*Dag failure*\n\n
*DAG:* {context["dag"].dag_id}\n
*Description:* {context["dag"].description}\n
*Task:* {context["task_instance_key_str"]}\n
*Run id:* {context["run_id"]}\n
*Exception:* {str(context["exception"])}'''
    _post_message_to_slack(msg)
