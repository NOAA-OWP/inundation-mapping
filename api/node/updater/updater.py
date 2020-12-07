import os
import re
import math
import time
import logging

import socketio

current_jobs = {}

# Get all the current running jobs from the list of docker containers, store that data in a dictionary
# along with any other needed metadata (like if it's still running, doing post processing, copying outputs
# to its destination, etc), and then update the websocket server of the status of the jobs.
def update_loop():
    while True:
        # Get list of current docker containers that are fim run jobs
        job_names = os.popen("docker container ls --filter=name=apijob --format '{{.Names}}'").read().splitlines()
        for name in job_names:
            if name not in current_jobs:
                # If it's a new job, add it to the dictionary
                current_jobs[name] = {
                    'container_name': name,
                    'name': re.search(r"apijob_(.+)_apijob.+", name).group(1),
                    'status': 'In Progress',
                    'time_started': time.time(),
                    'time_elapsed': 0,
                }

            if current_jobs[name]['status'] == 'In Progress':
                # Update the time elapsed for all jobs that are currently in progress
                current_jobs[name]['time_elapsed'] = math.ceil(time.time() - current_jobs[name]['time_started'])

        jobs_to_delete = []
        for name in current_jobs.keys():
            # TODO: change this to be completed after the outputs are copied over
            if current_jobs[name]['status'] == 'In Progress' and name not in job_names:
                current_jobs[name]['status'] = 'Saving Outputs'
                # Insert Slack notification here for finished job
                # TODO: Possible check the completed job's log for its exit code
                # This status will also trigger the connector to delete the temp cloned repo
            
            if current_jobs[name]['status'] == 'Saving Outputs' and name not in job_names:
                current_jobs[name]['status'] = 'Saving Outputs'

            if current_jobs[name]['status'] == 'Completed' and \
                time.time() >= current_jobs[name]['time_started'] + current_jobs[name]['time_elapsed'] + 900:
                # Remove job from list after it's been completed for more than 15 minutes
                jobs_to_delete.append(name)

        for job in jobs_to_delete:
            del current_jobs[job]

        # Send updates to the connector
        sio.emit('update', current_jobs)
        time.sleep(2)

sio = socketio.Client()

@sio.event
def connect():
    print("Update Loop Connected!")
    update_loop()

@sio.event
def disconnect():
    print('disconnected from server')

sio.connect('http://fim_node_connector:6000/')
sio.wait()