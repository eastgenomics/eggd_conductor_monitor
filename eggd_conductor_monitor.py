"""
Script to monitor state of jobs launched by eggd_conductor, and notify
via Slack for any fails or when all successfully complete
"""
import concurrent
import logging
import os
import re
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

import dxpy as dx

log = logging.getLogger("monitor log")
log.setLevel(logging.DEBUG)

handler = logging.handlers.TimedRotatingFileHandler(
    'eggd_conductor_monitor.log',
    when="midnight",
    interval=1,
    backupCount=5
)

log.addHandler(handler)


def dx_login(token) -> bool:

    """
    Function to check dxpy login

    Parameters
    ----------
    token : str
        DNAnexus authentication token

    Returns
    -------
    bool
        If login to DNAnexus was successful
    """

    try:
        DX_SECURITY_CONTEXT = {
            "auth_token_type": "Bearer",
            "auth_token": str(token)
        }

        dx.set_security_context(DX_SECURITY_CONTEXT)
        dx.api.system_whoami()

        return True

    except dx.exceptions.InvalidAuthentication as err:
        log.error(err.error_message())

        return False


def find_jobs() -> list:
    """
    Find eggd_conductor jobs that have run in the given project
    in the last 72 hours

    Returns
    -------
    jobs : list
        list of describe objects for each job
    """
    jobs = list(dx.bindings.search.find_executions(
        project=os.environ.get('DX_PROJECT'),
        state='done',
        created_after='-1d',
        describe=True
    ))

    jobs = [
        x for x in jobs
        if x.get('describe', {}).get('name') == 'eggd_conductor'
    ]

    return jobs


def filter_notified_jobs(jobs) -> list:
    """
    Filter out job IDs of runs already notified

    Parameters
    ----------
    jobs : list
        list of job describe objects

    Returns
    -------
    list
        list of job describe objects where no Slack notification has been sent
    """
    with open('monitor_job_ids_notified.log', 'r') as fh:
        notified_jobs = fh.read().splitlines()
    
    return [x for x in jobs if x['id'] not in notified_jobs]


def get_run_ids(jobs) -> list:
    """
    Get run ID for each job to know the run being processed.

    This is either parsed from the sentinel record if used, or from the
    run_id input or RunInfo.xml file

    Parameters
    ----------
    jobs : list
        list of job describe objects

    Returns
    -------
    list
        list of job describe objects, including run IDs
    """
    updated_jobs = []

    for job in jobs:
        job_input = job.get('describe', {}).get('originalInput', {})

        sentinel = job_input.get('SENTINEL_FILE')
        run_id = job_input.get('RUN_ID')
        run_info_xml = job_input.get('RUN_INFO_XML')

        if run_id:
            continue
        elif sentinel:
            run_id = dx.describe(sentinel).get('name', '')
            run_id = run_id.replace('run.', '').replace('.lane.all.upload_sentinel', '')
        elif run_info_xml:
            file_contents = dx.bindings.dxfile.DXFile(run_info_xml).read()
            run_id = re.search(r'Run Id=\"[A-Z0-9_]*\"', file_contents)
            if run_id:
                run_id = run_id.group(0).replace("Run Id=", "").strip('"')

        if not run_id:
            # failed to correctly get run id
            run_id = "unknown"

        job['run_id'] = run_id
        updated_jobs.append(job)

    return updated_jobs


def get_launched_jobs(jobs) -> list:
    """
    Parse out job IDs of launched jobs from eggd_conductor output

    Parameters
    ----------
    jobs : list
        list of job describe objects

    Returns
    -------
    list
        _description_
    """
    updated_jobs = []

    for job in jobs:
        output = job.get('describe').get('output').get('job_ids', '')
        job['output'] = [x for x in output.split(',') if x]

        updated_jobs.append(job)

    return updated_jobs


def get_all_job_states(jobs) -> dict:
    """
    Get the state of all launched jobs

    Parameters
    ----------
    jobs : list
        list of job describe objects

    Returns
    -------
    all_states_counts : dict
        mapping of state to total jobs
    """
    all_states = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
        # submit query to get state of job / analysis
        concurrent_jobs = {
            executor.submit(dx.describe, id): id for id in jobs['output']
        }
        for future in concurrent.futures.as_completed(concurrent_jobs):
            # access returned output as each is returned in any order
            try:
                job_state = future.result()
                all_states.append(job_state.get('state'))
            except Exception as exc:
                # catch any errors that might get raised during querying
                print(
                    f"Error getting data for {concurrent_jobs[future]}: {exc}"
                )

    # get a count of each state
    all_states_count = {}
    for state in set(all_states):
        all_states_count[state] = all_states.count(state)

    return all_states_count


def slack_notify(channel, message, job_id=None) -> None:
    """
    Send notification to given Slack channel

    Parameters
    ----------
    channel : str
        channel to send message to
    message : str
        message to send to Slack
    job_id : str
        DNAnexus ID of eggd_conductor job
    """
    slack_token = os.environ.get('SLACK_TOKEN')
    http = Session()
    retries = Retry(total=5, backoff_factor=10, method_whitelist=['POST'])
    http.mount("https://", HTTPAdapter(max_retries=retries))

    try:
        response = http.post(
            'https://slack.com/api/chat.postMessage', {
                'token': slack_token,
                'channel': f"#{channel}",
                'text': message
            }).json()

        if not response['ok']:
            # error in sending slack notification
            log.error(
                f"Error in sending slack notification: {response.get('error')}"
            )
        else:
            # log job ID to know we sent an alert for it and not send another
            if job_id:
                with open('monitor_job_ids_notified.log', 'a') as fh:
                    fh.write(f"{job_id}\n")
    except Exception as err:
        log.error(
            f"Error in sending post request for slack notification: {err}"
        )


def monitor():
    """
    Main function for monitoring eggd_conductor jobs in a given project
    """
    if not dx_login(os.environ.get('AUTH_TOKEN')):
        # error connecting to DNAnexus => notify on Slack
        slack_notify(
            channel=os.environ.get('SLACK_ALERT_CHANNEL'),
            message=(
                ":warning: eggd_conductor_monitor: Failed to connect to "
                "DNAnexus with supplied authentication token."
            )
        )

    conductor_jobs = find_jobs()
    conductor_jobs = filter_notified_jobs(conductor_jobs)
    conductor_jobs = get_run_ids(conductor_jobs)
    conductor_jobs = get_launched_jobs(conductor_jobs)

    for job in conductor_jobs:
        # get the state of all launched analysis jobs
        all_states = get_all_job_states(job)

        # get url to downstream analysis added as tag to job
        # filtering by beginning of url in case of multiple tags
        url = ''.join([
            x for x in job['describe']['tags']
            if x.startswith('platform.dnanexus.com')
        ])


        if all_states.get('failed') or all_states.get('partially failed'):
            # something has failed => send an alert
            channel = os.environ.get('SLACK_ALERT_CHANNEL')
            message = (
                ":warning: eggd_conductor_monitoring: Jobs failed processing "
                f"run *{job.get('run_id')}* in job {job.get('id')}.\n"
                f"Analysis project: {url}"
            )
        elif (
            not all_states.get('in progress') and
            not all_states.get('waiting') and
            not all_states.get('running') and
            not all_states.get('terminated')
        ):
            # everything completed successfully
            channel = os.environ.get('SLACK_LOG_CHANNEL')
            message = (
                ":white_check_mark: eggd_conductor_monitoring: All jobs "
                f"completed successfully processing {job.get('run_id')}.\n"
                f"Analysis project: {url}"
            )
        else:
            # jobs still in progress
            continue

        # send message to slack
        slack_notify(channel=channel, message=message, job_id=job['id'])


if __name__ == "__main__":
    monitor()
