import os
import re
import math
import time
import json
import shutil
import logging

import socketio

DATA_PATH = os.environ.get('DATA_PATH')

connected = False
shared_data = {
    'connected': False
}

current_jobs = {}
if os.path.exists('/data/outputs/current_jobs.json'):
    with open('/data/outputs/current_jobs.json') as f:
        current_jobs = json.load(f)

# Get all the current running jobs from the list of docker containers, store that data in a dictionary
# along with any other needed metadata (like if it's still running, doing post processing, copying outputs
# to its destination, etc), and then update the websocket server of the status of the jobs.
def update_loop():
    while True:
        # Get list of current docker containers that are fim run jobs
        job_names = os.popen("docker container ls --filter=name=apijob --format '{{.Names}}'").read().splitlines()
        for job_name in job_names:
            if job_name not in current_jobs:
                # If it's a new job, add it to the dictionary
                current_jobs[job_name] = {
                    'job_name': job_name,
                    'nice_name': re.search(r"apijob_(.+)_apijob.+", job_name).group(1),
                    'status': 'In Progress',
                    'time_started': time.time(),
                    'time_elapsed': 0,
                    'output_files_saved': {}
                }

        jobs_to_delete = []
        for job_name in current_jobs.keys():
            # Update the time elapsed for all jobs that are currently in progress or saving outputs
            if current_jobs[job_name]['status'] == 'In Progress' or current_jobs[job_name]['status'] == 'Ready to Save File'\
                or current_jobs[job_name]['status'] == 'Saving File':
                current_jobs[job_name]['time_elapsed'] = math.ceil(time.time() - current_jobs[job_name]['time_started'])

            # TODO: While job is in progress, keep track of how many hucs are done and overall progress %

            # Once the Docker container is done, set the job as ready to save output
            if current_jobs[job_name]['status'] == 'In Progress' and job_name not in job_names:
                for path, folders, files in os.walk(f"/data/outputs/{job_name}"):
                    for file in files:
                        current_jobs[job_name]['output_files_saved'][os.path.join(path, file)] = False

                current_jobs[job_name]['status'] = 'Ready to Save File'
                # TODO: Possible check the completed job's log for its exit code
    
            # Trigger connector to transmit the outputs to the output_handler
            # If the output_handler is offline, it will keep retrying until the output_handler is online
            if current_jobs[job_name]['status'] == 'Ready to Save File':
                print(f"{job_name} ready for output handler")
                outputs_to_save = []
                for path in current_jobs[job_name]['output_files_saved']:
                    if current_jobs[job_name]['output_files_saved'][path] == False:
                        outputs_to_save.append(path)

                if len(outputs_to_save) > 0:
                    if shared_data['connected']: 
                        sio.emit('ready_for_output_handler', {
                            'nice_name': current_jobs[job_name]['nice_name'],
                            'job_name': job_name,
                            'path': outputs_to_save[0]
                        })
                current_jobs[job_name]['status'] = 'Saving File'
            
            # Once the output_handler is done getting the outputs and the connector deletes the temp repo source,
            # mark as completed
            if current_jobs[job_name]['status'] == 'Saving File':
                is_done = True
                for path in current_jobs[job_name]['output_files_saved']:
                    if current_jobs[job_name]['output_files_saved'][path] == False:
                        is_done = False
                        break

                if is_done:
                    print("output_handler finished, deleted temp source files and output files")
                    temp_path = f"/data/temp/{job_name}"
                    if os.path.isdir(temp_path):
                        shutil.rmtree(temp_path)

                    outputs_path = f"/data/outputs/{job_name}"
                    if os.path.isdir(outputs_path):
                        shutil.rmtree(outputs_path)

                    current_jobs[job_name]['status'] = 'Completed'
                    print(f"{job_name} completed")
                    # TODO: Insert Slack notification here for finished job

            # Remove job from list after it's been completed for more than 15 minutes
            if current_jobs[job_name]['status'] == 'Completed' and \
                time.time() >= current_jobs[job_name]['time_started'] + current_jobs[job_name]['time_elapsed'] + 900:
                print(f"{job_name} removed from job list")
                jobs_to_delete.append(job_name)

        for job in jobs_to_delete:
            del current_jobs[job]

        # Send updates to the connector and write job progress to file
        if shared_data['connected']: sio.emit('update', current_jobs)
        with open('/data/outputs/current_jobs.json', 'w') as f:
            json.dump(current_jobs, f)

        time.sleep(1)

sio = socketio.Client()

@sio.event
def connect():
    print("Update Loop Connected!")
    sio.emit('updater_connected')
    shared_data['connected'] = True

@sio.event
def disconnect():
    print('disconnected from server')
    shared_data['connected'] = False

# If the output_handler is offline, try the saving process again
@sio.on('retry_saving_files')
def ws_retry_saving_files():
    print('saving files failed, retrying')
    for job_name in current_jobs:
        if current_jobs[job_name]['status'] == "Saving File":
            current_jobs[job_name]['status'] = 'Ready to Save File'

@sio.on('file_saved')
def ws_file_saved(data):
    job_name = data['job_name']
    file_path = data['file_path']

    current_jobs[job_name]['output_files_saved'][file_path] = True
    current_jobs[job_name]['status'] = 'Ready to Save File'

sio.connect('http://fim_node_connector:6000/')
update_loop()