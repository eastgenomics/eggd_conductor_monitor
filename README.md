# eggd_conductor_monitor
Monitoring script for checking and notifying status of jobs launched with eggd_conductor.

The given `DX_PROJECT` is monitored for `eggd_conductor` jobs run in the past 48 hours, for which if they have completed all analysis jobs launched from this job will be checked for their state.
- If all are `done`, a success notification will be sent to the `SLACK_LOG_CHANNEL` and the job ID logged to not send further notifications for
- If any have the state `failed` or `partially failed`, an alert is sent to the `SLACK_ALERT_CHANNEL` to notify of a failed analysis job and the job ID logged to not send further notifications for
- If neither of the above conditions are met then the job(s) will be checked again the next time the monitor script is run

This should be set to run on a frequent cron job so that any running analyses are frequently checked for failed jobs and all completing, to send out timely notifications.


## Requirements

Required environment variables:
- `AUTH_TOKEN` - DNAnexus token
- `DX_PROJECT` - DNAneuxs project to monitor for eggd_conductor jobs
- `SLACK_TOKEN` - Slack API token
- `SLACK_LOG_CHANNEL` - Slack channel to send succes notifications to
- `SLACK_ALERT_CHANNEL` - Slack channel to send fail job alerts to

## Usage

- Build Docker image: `docker build . -t eggd_conductor_monitor

- Run Docker image: `docker run -d {image}`
  - environment variables either should be passed with Docker run as `--env`, in a file as `--env-file` or set in the running detached container