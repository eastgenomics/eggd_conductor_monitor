"""
Script to monitor state of jobs launched by eggd_conductor, and notify
via Slack for any fails or when all successfully complete
"""

import concurrent
import concurrent.futures
from datetime import timedelta
import logging
import logging.handlers
import os
import re
from requests import Session
from requests.adapters import HTTPAdapter
import sys
from urllib3.util import Retry

import dxpy as dx

from utils.jira_functions import Jira

log = logging.getLogger("monitor log")
log.setLevel(logging.DEBUG)

log_format = logging.StreamHandler(sys.stdout)
log_format.setFormatter(
    logging.Formatter("%(asctime)s:%(module)s:%(levelname)s: %(message)s")
)

log.addHandler(log_format)

handler = logging.handlers.TimedRotatingFileHandler(
    "logs/eggd_conductor_monitor.log",
    when="midnight",
    interval=1,
    backupCount=5,
)

handler.setFormatter(
    logging.Formatter("%(asctime)s:%(module)s:%(levelname)s: %(message)s")
)

log.addHandler(handler)


def dx_login(token):
    """
    Function to check authenticating to DNAneuxs

    Parameters
    ----------
    token : str
        DNAnexus authentication token
    """
    try:
        DX_SECURITY_CONTEXT = {
            "auth_token_type": "Bearer",
            "auth_token": str(token),
        }

        dx.set_security_context(DX_SECURITY_CONTEXT)
        dx.api.system_whoami()
    except dx.exceptions.InvalidAuthentication as err:
        log.error(err.error_message())

        # error connecting to DNAnexus => notify on Slack
        slack_notify(
            channel=os.environ.get("SLACK_ALERT_CHANNEL"),
            message=(
                ":warning: eggd_conductor_monitor: Failed to connect to "
                "DNAnexus with supplied authentication token."
            ),
        )


def find_jobs(testing_conductor_job) -> list:
    """
    Find eggd_conductor jobs that have run in the given project
    in the last 48 hours

    Parameters
    ----------
    testing_conductor_job : str
        Testing conductor job

    Returns
    -------
    jobs : list
        list of describe objects for each job
    """

    if testing_conductor_job:
        job = dx.DXJob(testing_conductor_job)
        jobs = [{"id": job.id, "describe": job.describe()}]
    else:
        jobs = list(
            dx.bindings.search.find_executions(
                project=os.environ.get("DX_PROJECT"),
                state="done",
                created_after="-48h",
                describe=True,
            )
        )

    jobs = [
        x
        for x in jobs
        if x.get("describe", {}).get("executableName") == "eggd_conductor"
    ]

    log.info(
        f"Found the following {len(jobs)} eggd_conductor jobs: "
        f"{', '.join([x['id'] for x in jobs])}"
    )

    return jobs


def filter_notified_projects(job, projects) -> list:
    """
    Filter out project IDs of runs already notified

    Parameters
    ----------
    job : dict
        job describe object
    projects : list
        list of project IDs

    Returns
    -------
    list
        list of project IDs where no Slack notification has been sent
    """
    with open("logs/monitor_project_ids_notified.log", "a+") as fh:
        fh.seek(0)
        notified = fh.read().splitlines()

    if notified:
        log.info(
            "Projects already notified via Slack or not to notify: "
            f"{os.linesep}{notified}"
        )

    return [x for x in projects if f"{job['id']}:{x}" not in notified]


def get_run_ids(jobs) -> list:
    """
    Get run ID and assay(s) for each job to know the run being processed.

    This is either parsed from the sentinel record if used, or from the
    run_id input or RunInfo.xml file

    Parameters
    ----------
    jobs : list
        list of job describe objects

    Returns
    -------
    list
        list of job describe objects, including run IDs and assay(s)
    """
    updated_jobs = []

    for job in jobs:
        job_input = job.get("describe", {}).get("originalInput", {})
        run_id = ""

        run_id_matches = [
            re.search(r"run_id", ele, re.IGNORECASE) for ele in job_input
        ]
        sentinel = job_input.get("upload_sentinel_record")

        if sentinel:
            run_id = dx.describe(sentinel).get("name", "")
            run_id = run_id.replace("run.", "").replace(
                ".lane.all.upload_sentinel", ""
            )
        elif any(run_id_matches):
            run_id = job_input.get(
                [match.group(0) for match in run_id_matches if match][0]
            )

        if not run_id:
            # failed to correctly get run id
            run_id = "unknown"

        log.info(f"Found run ID {run_id} for job {job['id']}")

        job["run_id"] = run_id

        describe = job.get("describe", {})
        output = describe.get("output", {})
        assay = output.get("assay_config_file_ids", "unknown")

        log.info(f"Found assay(s) {assay} for {job['id']}")

        parsed_assays = re.findall(
            r'file-\w+:\s*(\w+)\s*-.*?->\s*(project-\w+)', assay)
        job["assay"] = parsed_assays if parsed_assays else [("unknown", "")]
        updated_jobs.append(job)

    return updated_jobs


def get_launched_jobs(jobs) -> tuple[list, dict]:
    """
    Parse out job IDs of launched jobs from eggd_conductor output

    Parameters
    ----------
    jobs : list
        list of job describe objects

    Returns
    -------
    list
        list of job describe objects with launched jobs set to output
    dict
        dict of job describe objects grouped by analysis project
    """
    updated_jobs = []
    jobs_by_project = {}

    for job in jobs:

        describe = job.get("describe", {})
        output_data = describe.get("output", {})
        output = output_data.get("job_ids", "")

        job["output"] = [x for x in re.split(
            "project-[a-zA-Z0-9]+:|,", output) if x]

        updated_jobs.append(job)
        # group jobs by project id
        for match in re.finditer(r'(project-[a-zA-Z0-9]+):([^,]+)', output):
            project, job_id = match.groups()
            jobs_by_project.setdefault(project, []).append(job_id)

    return updated_jobs, jobs_by_project


def get_all_job_states(jobs_by_project) -> dict:
    """
    Get the state of all launched jobs

    Parameters
    ----------
    jobs_by_project : dict
        mapping of project IDs to job IDs

    Returns
    -------
    project_states : dict
        mapping of project to states, executables, and times
    """
    project_states = {}
    total_states = {}

    for project, job_ids in jobs_by_project.items():
        all_states = []
        all_executables = []
        started = []
        stopped = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
            # submit query to get state of job / analysis
            concurrent_jobs = {
                executor.submit(dx.describe, id): id for id in job_ids
            }
            for future in concurrent.futures.as_completed(concurrent_jobs):
                # access returned output as each is returned in any order
                try:
                    describe = future.result()
                    all_states.append(describe.get("state"))
                    all_executables.append(describe.get("executableName"))
                    created = describe.get("created")
                    modified = describe.get("modified")
                    if created is not None:
                        started.append(created)
                    if modified is not None:
                        stopped.append(modified)
                except Exception as exc:
                    # catch any errors that might get raised during querying
                    log.error(
                        f"Error getting data for "
                        f"{concurrent_jobs[future]}: {exc}"
                    )

        # get a count of each state
        all_states_count = {}
        for state in set(all_states):
            all_states_count[state] = all_states.count(state)

        # get a count of each executable
        all_executables_count = {}
        for exe in set(all_executables):
            all_executables_count[exe] = all_executables.count(exe)

        # get earliest job start time and end time of latest running job
        if started and stopped:
            times = (min(started) / 1000, max(stopped) / 1000)
        else:
            # if querying is immediately after launching jobs (or the
            # eggd_conductor job did not launch any jobs) then the created
            # and modified metadata fields may be null => only calculate if
            # something is present, else just return zeros
            times = (0, 0)

        project_states[project] = {
            "all_states_count": all_states_count,
            "all_executables_count": all_executables_count,
            "times": times,
        }

        for state, count in all_states_count.items():
            total_states[state] = total_states.get(state, 0) + count

    return project_states, total_states


def jira_comment(
    run_id, assay, jira_message, job_id, conductor_message, project_url
) -> None:
    """
    Add comment to Jira ticket linked to the run ID

    Parameters
    ----------
    run_id : str
        run ID to match to Jira ticket
    assay : str
        assay to match to Jira ticket
    jira_message : str
        message to add to Jira comment
    job_id : str
        job ID to link to Jira ticket
    conductor_message : str
        eggd_conductor processing message
    project_url : str
        url to the analysis project
    """
    # setup the Jira client
    try:
        jira = Jira(
            os.environ.get("JIRA_QUEUE_URL"),
            os.environ.get("JIRA_ISSUE_URL"),
            os.environ.get("JIRA_TOKEN"),
            os.environ.get("JIRA_EMAIL"),
        )
        # get all tickets in the specified helpdesk
        all_tickets = jira.query_all_tickets()
        filtered_tickets = jira.filter_tickets_by_run(run_id, all_tickets)

        # filter by assay code if multiple tickets

        filtered_tickets = jira.filter_tickets_by_assay(
            assay, filtered_tickets)

        project_id = os.environ.get("DX_PROJECT")
        job_url = (
            "https://platform.dnanexus.com/panx/projects/"
            f"{project_id.replace('project-', '')}/monitor/"
            f"job/{job_id.replace('job-', '')}"
        )

        if len(filtered_tickets) != 1:
            # send Slack notification
            channel = os.environ.get("SLACK_ALERT_CHANNEL")
            message = (
                ":x: eggd_conductor_monitor: Error finding Jira ticket "
                f"for run *{run_id}* and *{assay}* assay: "
                f"found {len(filtered_tickets)} matching tickets.")
            slack_notify(channel=channel, message=message)

        # add comment to Jira ticket for run to link to
        # this eggd_conductor job
        else:
            for ticket in filtered_tickets:
                log.info(
                    f"Adding comment to Jira ticket {ticket['id']} "
                    f"for run {run_id}"
                )
                jira.add_comment(
                    comment=jira_message,
                    project_url=project_url,
                    conductor_message=conductor_message,
                    url=job_url,
                    ticket=ticket["id"],
                 )

    except Exception as err:
        log.error(f"Error in adding Jira comment for {run_id}: {err}")


def slack_notify(channel, message, job_id=None, project=None) -> None:
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
    project : str
        DNAnexus ID of analysis project
    """
    log.info(f"Sending message to {channel}")
    slack_token = os.environ.get("SLACK_TOKEN")

    http = Session()
    retries = Retry(total=5, backoff_factor=10, allowed_methods=["POST"])
    http.mount("https://", HTTPAdapter(max_retries=retries))
    try:
        response = http.post(
            "https://slack.com/api/chat.postMessage",
            {"token": slack_token, "channel": f"#{channel}", "text": message},
        ).json()

        if not response["ok"]:
            # error in sending slack notification
            log.error(
                f"Error in sending slack notification: {response.get('error')}"
            )
        else:
            # log job ID to know we sent an alert for it and not send another
            if job_id:
                with open("logs/monitor_project_ids_notified.log", "a+") as fh:
                    fh.write(f"{job_id}:{project}\n")
    except Exception as err:
        log.error(
            f"Error in sending post request for slack notification: {err}"
        )


def failed_run(run, project) -> None:
    """
    Build message and sent Slack notification to alert of failed job(s)

    Parameters
    ----------
    run : dict
        dx describe object of given run
    project : str
        analysis project ID
    """

    assay = next(
        (assay_type for assay_type, project_name in run['assay']
         if project_name == project), "unknown"
         )

    log.info(f"Found failed jobs for {assay} "
             f"in {project} for run {run['run_id']}")

    url = (
        "https://platform.dnanexus.com/panx/projects/"
        f"{project.replace('project-', '')}/monitor/"
    )
    # send Slack notification
    channel = os.environ.get("SLACK_ALERT_CHANNEL")
    message = (
        ":x: eggd_conductor_monitor: Automated job(s) failed processing "
        f"for *{assay}* in run *{run.get('run_id')}* from `{run.get('id')}`.\n"
        f"Analysis project: {url}?state.values=failed"
    )

    slack_notify(channel=channel, message=message)

    # add Jira comment
    jira_message = (
        "Eggd_conductor_monitor: Automated job(s) failed processing "
        f"for {assay} in run {run.get('run_id')} from {run.get('id')}.\n"
        f"Analysis project: "
    )

    project_url = f"{url}?state.values=failed"
    conductor_message = (
        "This run was processed automatically by eggd_conductor: "
    )

    jira_comment(
        run_id=run["run_id"],
        assay=assay,
        jira_message=jira_message,
        project_url=project_url,
        conductor_message=conductor_message,
        job_id=run["id"]
    )


def completed_run(run, executables, times, project) -> None:
    """
    Build message and sent Slack notification for completed run

    Parameters
    ----------
    run : dict
        dx describe object of given run

    executables : dict
        mapping of executables run and total count of each

    times : tuple
        first job start time and last job finished time

    project : str
        analysis project ID
    """

    assay = next(
        (assay_type for assay_type, project_name in run['assay']
         if project_name == project), "unknown"
         )

    log.info(f"All jobs completed for {assay} "
             f"in {project} for run {run['run_id']}")

    url = (
        "https://platform.dnanexus.com/panx/projects/"
        f"{project.replace('project-', '')}/monitor/"
    )

    # calculate run time of pipeline and including conductor job
    pipeline = timedelta(seconds=times[1]) - timedelta(seconds=times[0])
    total = timedelta(seconds=times[1]) - timedelta(
        seconds=run["describe"]["created"] / 1000
    )

    times = []

    for time in [pipeline, total]:
        duration = time.total_seconds()

        if duration < 3600:
            minutes, _ = divmod(duration, 60)
            reformatted_time = f"{int(minutes)}m"
        else:
            hours, remainder = divmod(duration, 3600)
            minutes, _ = divmod(remainder, 60)
            reformatted_time = f"{int(hours)}h{int(minutes)}m"

        times.append(reformatted_time)

    pipeline, total = times

    # build list of what has been run
    executables = "".join(
        [f":black_small_square: {v}x {k}\n" for k, v in executables.items()]
    )
    # send Slack message
    channel = os.environ.get("SLACK_LOG_CHANNEL")
    message = (
        ":white_check_mark: eggd_conductor_monitor: All jobs "
        f"completed successfully processing "
        f"for *{assay}* in run *{run.get('run_id')}*.\n"
        f"Total elapsed time: *{total}*\nPipeline runtime: *{pipeline}*\n"
        f"Apps / workflows run: \n{executables}\n"
        f"Analysis project: {url}"
    )

    slack_notify(channel=channel, message=message)

    # add Jira comment
    jira_executables = executables.replace(":black_small_square:", "-")

    project_url = f"{url}"

    jira_message = (
        "Eggd_conductor_monitor: All jobs "
        f"completed successfully processing "
        f"for {assay} in run {run.get('run_id')}.\n"
        f"Total elapsed time: {total}\nPipeline runtime: {pipeline}\n"
        f"Apps / workflows run: \n{jira_executables}"
        f"Analysis project: "
    )

    conductor_message = (
        "This run was processed automatically by eggd_conductor: "
    )

    jira_comment(
        run_id=run["run_id"],
        assay=assay,
        jira_message=jira_message,
        project_url=project_url,
        conductor_message=conductor_message,
        job_id=run["id"]
    )


def monitor():
    """
    Main function for monitoring eggd_conductor jobs in a given project
    """
    log.info("Starting monitoring")

    required = [
        "AUTH_TOKEN",
        "DX_PROJECT",
        "SLACK_TOKEN",
        "SLACK_LOG_CHANNEL",
        "SLACK_ALERT_CHANNEL",
        "JIRA_QUEUE_URL",
        "JIRA_ISSUE_URL",
        "JIRA_TOKEN",
        "JIRA_EMAIL",
    ]

    testing_job = os.environ.get("DX_CONDUCTOR_JOB")

    missing = [x for x in required if not os.environ.get(x)]

    if missing:
        # one or more required env variables not set
        log.error(f"Required env variable(s) not set: {missing}. Exiting now.")
        sys.exit()

    # test can connect to DNAnexus
    dx_login(os.environ.get("AUTH_TOKEN"))

    conductor_jobs = find_jobs(testing_job)
    conductor_jobs = get_run_ids(conductor_jobs)

    finished_states = {"done", "failed", "partially failed", "terminated"}

    for job in conductor_jobs:
        # get the state of all launched analysis jobs for given conductor job
        _, jobs_by_project = get_launched_jobs([job])
        project_states, total_states = get_all_job_states(jobs_by_project)

        log.info(f"Current state(s) for {job['id']}: {total_states}")

        unnotified = filter_notified_projects(
            job, project_states.keys())

        if not total_states:
            # no job states => no launched jobs => stop monitoring
            log.info(
                f"No launched jobs for {job['id']} => stopping monitoring"
            )
            continue

        # check the state of each project with launched jobs and notify
        for project, states in project_states.items():
            if project not in unnotified:
                log.info(
                    f"Already sent notification for {project} "
                    f"=> skipping project"
                )
                continue

            state = states["all_states_count"]
            executables = states["all_executables_count"]
            times = states["times"]
            log.info(f"Current state for {project} in {job['id']}: {state}")

            # something has failed => send an alert
            if state.get("failed") or state.get("partially failed"):
                failed_run(job, project)
                with open("logs/monitor_project_ids_notified.log", "a+") as fh:
                    fh.write(f"{job['id']}:{project}\n")

            # everything completed with no failed jobs => send notification
            elif set(state.keys()) == {"done"}:
                completed_run(job, executables, times, project)
                with open("logs/monitor_project_ids_notified.log", "a+") as fh:
                    fh.write(f"{job['id']}:{project}\n")

            # everything has been terminated for that project =>
            # stop monitoring
            elif set(state.keys()) == {"terminated"}:
                log.info(f"All jobs terminated for {project} "
                         f"=> stopping monitoring")
                with open("logs/monitor_project_ids_notified.log", "a+") as fh:
                    fh.write(f"{job['id']}:{project}\n")

        if set(total_states.keys()).issubset(finished_states):
            # everything has finished for the job => add to log
            if set(total_states.keys()) == {"terminated"}:
                log.info(
                    f"All jobs terminated for {job['id']}"
                    f"=> stopping monitoring"
                )
            else:
                log.info(
                    f"All jobs finished for {job['id']} => stopping monitoring"
                )

        else:
            # jobs still in progress => continue monitoring
            log.info(
                f"Jobs launched from {job['id']} "
                "have not failed or all completed"
            )
            continue

    log.info("Finished monitoring\n")


if __name__ == "__main__":
    monitor()
