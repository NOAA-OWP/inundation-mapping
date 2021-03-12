import os
import re
import math
import time
import json
import shutil
import logging
import subprocess

import socketio

DATA_PATH = os.environ.get('DATA_PATH')
DOCKER_IMAGE_PATH = os.environ.get('DOCKER_IMAGE_PATH')
GITHUB_REPO = os.environ.get('GITHUB_REPO')
MAX_ALLOWED_CPU_CORES = int(os.environ.get('MAX_ALLOWED_CPU_CORES'))

shared_data = {
    'connected': False,
    'current_saving_job': ''
}

buffer_jobs = []
buffer_remove_jobs = []
current_jobs = {}
if os.path.exists('/data/outputs/current_jobs.json'):
    with open('/data/outputs/current_jobs.json') as f:
        current_jobs = json.load(f)
        for job_name in current_jobs.keys():
            if 'is_actively_saving' in current_jobs[job_name] and current_jobs[job_name]['is_actively_saving'] == True:
                shared_data['current_saving_job'] = current_jobs[job_name]


# Get all the current running jobs from the list of docker containers, store that data in a dictionary
# along with any other needed metadata (like if it's still running, doing post processing, copying outputs
# to its destination, etc), and then update the websocket server of the status of the jobs.
def update_loop():
    while True:
        # If there are no current jobs, just check every 10 seconds till there is
        if len(current_jobs.keys()) == 0: sio.sleep(10)

        while len(buffer_jobs) > 0:
            new_job = buffer_jobs.pop()
            current_jobs[new_job['job_name']] = new_job

        while len(buffer_remove_jobs) > 0:
            job_to_remove = buffer_remove_jobs.pop()
            current_jobs[job_to_remove['job_name']]['status'] = 'Cancelled'

        # Get list of current docker containers that are fim run jobs
                                #    docker ps --all --filter=name=apijob --format='{{.Names}} {{.State}}'
        containers_raw = os.popen("docker ps --all --filter=name=apijob --format='{{.Names}} {{.State}}'").read().splitlines()
        containers_split = [ line.split() for line in containers_raw ]
        container_states = { name: state for (name, state) in containers_split }

        jobs_to_delete = []
        for job_name in current_jobs.keys():
            sio.sleep(0)
            if job_name in container_states:
                current_jobs[job_name]['container_state'] = container_states[job_name]

            # If the user chooses to cancel the job early
            if current_jobs[job_name]['status'] == 'Cancelled':
                # If the docker container is running, stop and remove it
                if current_jobs[job_name]['time_elapsed'] > 0 and current_jobs[job_name]['container_state'] != 'exited':
                    subprocess.call(f"docker container stop {job_name}", shell=True)
                    subprocess.call(f"docker container rm {job_name}", shell=True)

                print("output_handler finished, deleted temp source files and output files")
                temp_path = f"/data/temp/{job_name}"
                if os.path.isdir(temp_path):
                    shutil.rmtree(temp_path)

                outputs_path = f"/data/outputs/{job_name}"
                if os.path.isdir(outputs_path):
                    shutil.rmtree(outputs_path)
                
                jobs_to_delete.append(job_name)


            # Update the time elapsed for all jobs that are currently in progress or saving outputs
            if current_jobs[job_name]['status'] == 'In Progress' or current_jobs[job_name]['status'] == 'Ready to Save File'\
                or current_jobs[job_name]['status'] == 'Saving File':
                current_jobs[job_name]['time_elapsed'] = math.ceil(time.time() - current_jobs[job_name]['time_started'])

            # TODO: While job is in progress, keep track of how many hucs are done and overall progress %

            # Once resources recome available, start a new job that is in queue
            if current_jobs[job_name]['status'] == 'In Queue':
                # TODO: Start Dcoker containers here and set start time.
                current_jobs[job_name]['time_started'] = time.time()

                total_active_cores = 0
                for j in current_jobs.keys():
                    if current_jobs[j]['status'] == 'In Progress':
                        # This is to account for the fact that HUC6's take a lot more resources to run.
                        # (not necessarily cpu cores but rather RAM, so this artificially reduces how many jobs can run when HUC6's
                        #  are running)
                        # HACK: this is more of a temporary solution until we no longer need to run HUC6's
                        if current_jobs[j]['hucs_type'] == '6':
                            total_active_cores += current_jobs[j]['parallel_jobs'] * 5
                        else:
                            total_active_cores += current_jobs[j]['parallel_jobs']
                
                # Machine has enough resources to run a new job
                potential_active_cores = 0
                if current_jobs[job_name]['hucs_type'] == '6':
                    potential_active_cores = current_jobs[job_name]['parallel_jobs'] * 5 + total_active_cores
                else:
                    potential_active_cores = current_jobs[job_name]['parallel_jobs'] + total_active_cores

                # print(f"Checking whether a new job can start {potential_active_cores} <= {MAX_ALLOWED_CPU_CORES}")
                # print(potential_active_cores <= MAX_ALLOWED_CPU_CORES)
                if potential_active_cores <= MAX_ALLOWED_CPU_CORES:
                    job_name = current_jobs[job_name]['job_name']
                    branch = current_jobs[job_name]['branch']
                    hucs = current_jobs[job_name]['hucs']
                    parallel_jobs = current_jobs[job_name]['parallel_jobs']
                    extent = current_jobs[job_name]['extent']
                    config_path = current_jobs[job_name]['config_path']
                    dev_run = current_jobs[job_name]['dev_run']
                    viz_run = current_jobs[job_name]['viz_run']

                    # Clone github repo, with specific branch, to a temp folder
                    print(f'cd /data/temp && git clone -b {branch} {GITHUB_REPO} {job_name}')
                    subprocess.call(f'cd /data/temp && git clone -b {branch} {GITHUB_REPO} {job_name}', shell=True)

                    # Kick off the new job as a docker container with the new cloned repo as the volume
                    print(f"docker run -d --name {job_name} -v {DATA_PATH}:/data/ -v {DATA_PATH}temp/{job_name}/:/foss_fim {DOCKER_IMAGE_PATH} fim_run.sh -u \"{hucs}\" -e {extent} -c {config_path} -n {job_name} -o {'' if dev_run else '-p'} {'-v' if viz_run else ''} -j {parallel_jobs}")
                    subprocess.call(f"docker run -d --name {job_name} -v {DATA_PATH}:/data/ -v {DATA_PATH}temp/{job_name}/:/foss_fim {DOCKER_IMAGE_PATH} fim_run.sh -u \"{hucs}\" -e {extent} -c {config_path} -n {job_name} -o {'' if dev_run else '-p'} {'-v' if viz_run else ''} -j {parallel_jobs}", shell=True)
                    current_jobs[job_name]['status'] = 'In Progress'

            # Once the Docker container is done, set the job as ready to save output
            if current_jobs[job_name]['status'] == 'In Progress' and current_jobs[job_name]['container_state'] == 'exited':

                # Get container exit code, get the docker log, and then remove container
                exit_code_raw = os.popen(f"docker inspect {job_name}" + " --format='{{.State.ExitCode}}'").read().splitlines()

                print("Exit code")
                print(exit_code_raw)
                print(exit_code_raw[0])
                try:
                    print(int(exit_code_raw[0]))
                except:
                    pass

                exit_code = int(exit_code_raw[0])
                current_jobs[job_name]['exit_code'] = exit_code
                subprocess.call(f"docker logs {job_name} >& /data/outputs/{job_name}/logs/docker.log", shell=True)
                subprocess.call(f"docker container rm {job_name}", shell=True)

                for path, folders, files in os.walk(f"/data/outputs/{job_name}"):
                    for file in files:
                        current_jobs[job_name]['output_files_saved'][os.path.join(path, file)] = 0

                current_jobs[job_name]['total_output_files_length'] = len(current_jobs[job_name]['output_files_saved'].keys())
                current_jobs[job_name]['status'] = 'Ready to Save File'
    
            # Trigger connector to transmit the outputs to the output_handler
            # If the output_handler is offline, it will keep retrying until the output_handler is online
            if current_jobs[job_name]['status'] == 'Ready to Save File' and (shared_data['current_saving_job'] == '' or shared_data['current_saving_job'] == current_jobs[job_name]):
                print(f"{job_name} ready for output handler")

                shared_data['current_saving_job'] = current_jobs[job_name]
                current_jobs[job_name]['is_actively_saving'] = True
                output_to_save = {}
                for path in current_jobs[job_name]['output_files_saved']:
                    if current_jobs[job_name]['output_files_saved'][path] != -1:
                        output_to_save = {'path': path, 'chunk_index': current_jobs[job_name]['output_files_saved'][path]}

                if output_to_save != {}:
                    if shared_data['connected']: 
                        sio.emit('ready_for_output_handler', {
                            'nice_name': current_jobs[job_name]['nice_name'],
                            'job_name': job_name,
                            'path': output_to_save['path'],
                            'chunk_index': output_to_save['chunk_index']
                        })
                current_jobs[job_name]['status'] = 'Saving File'
            
            # Once the output_handler is done getting the outputs and the connector deletes the temp repo source,
            # mark as completed
            if current_jobs[job_name]['status'] == 'Saving File':
                is_done = True
                for path in current_jobs[job_name]['output_files_saved']:
                    if current_jobs[job_name]['output_files_saved'][path] != -1:
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

                    current_jobs[job_name]['status'] = 'Completed' if current_jobs[job_name]['exit_code'] == 0 else 'Error'
                    shared_data['current_saving_job'] = ''
                    current_jobs[job_name]['is_actively_saving'] = False
                    print(f"{job_name} completed")
                    # TODO: Insert Slack notification here for finished job

            # Remove job from list after it's been completed for more than 15 minutes
            if (current_jobs[job_name]['status'] == 'Completed' or current_jobs[job_name]['status'] == 'Error') and \
                time.time() >= current_jobs[job_name]['time_started'] + current_jobs[job_name]['time_elapsed'] + 900:
                print(f"{job_name} removed from job list")
                jobs_to_delete.append(job_name)

        for job in jobs_to_delete:
            del current_jobs[job]

        # TODO: Get Preset Lists here
        presets_list = []
        for path, folders, files in os.walk(f"/data/inputs/huc_lists"):
            for file in files:
                presets_list.append(file)

        # Send updates to the connector and write job progress to file
        job_updates = [ {
            'job_name': job['job_name'],
            'nice_name': job['nice_name'],
            'status': job['status'],
            'exit_code': job['exit_code'],
            'time_elapsed': job['time_elapsed'],
            'total_output_files_length': job['total_output_files_length'],
            'current_output_files_saved_length': job['current_output_files_saved_length'],
        } for job in current_jobs.values()]

        if shared_data['connected']: sio.emit('update', {'jobUpdates': job_updates, 'presetsList': presets_list})
        with open('/data/outputs/current_jobs.json', 'w') as f:
            json.dump(current_jobs, f)

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

@sio.on('add_job_to_queue')
def ws_add_job_to_queue(data):
    job_name = data['job_name']
    branch = data['branch']
    hucs = data['hucs']
    parallel_jobs = data['parallel_jobs']
    hucs_type = data['hucs_type']
    extent = data['extent']
    config_path = data['config_path']
    dev_run = data['dev_run']
    viz_run = data['viz_run']

    # This is a preset list instead of a custom list of hucs
    if hucs_type == 0:
        if os.path.exists(hucs):
            with open(hucs, "r") as preset_file:
                hucs_raw = preset_file.read().splitlines()
                parallel_jobs = len(hucs_raw)
                hucs_type = len(hucs_raw[0])
    print(f"{hucs} {parallel_jobs} {hucs_type}")


    parallel_jobs = parallel_jobs if parallel_jobs <= MAX_ALLOWED_CPU_CORES else MAX_ALLOWED_CPU_CORES

    buffer_jobs.append({
        'job_name': job_name,
        'branch': branch,
        'hucs': hucs,
        'parallel_jobs': parallel_jobs,
        'hucs_type': hucs_type,
        'extent': extent,
        'config_path': config_path,
        'dev_run': dev_run,
        'viz_run': viz_run,
        'nice_name': re.search(r"apijob_(.+)_apijob.+", job_name).group(1),
        'status': 'In Queue',
        'time_started': 0,
        'time_elapsed': 0,
        'output_files_saved': {},
        'total_output_files_length': 0,
        'current_output_files_saved_length': 0,
        'output_files_saved': {},
        'container_state': 'running',
        'exit_code': 0,
        'is_actively_saving': False
    })

@sio.on('remove_job_from_queue')
def ws_remove_job_from_queue(data):
    job_name = data['job_name']
    buffer_remove_jobs.append({'job_name': job_name})

# If the output_handler is offline, try the saving process again
@sio.on('retry_saving_files')
def ws_retry_saving_files():
    print('saving files failed, retrying')
    for job_name in current_jobs:
        if current_jobs[job_name]['status'] == "Saving File":
            for path in current_jobs[job_name]['output_files_saved']:
                if current_jobs[job_name]['output_files_saved'][path] != -1:
                    current_jobs[job_name]['output_files_saved'][path] = 0

            current_jobs[job_name]['status'] = 'Ready to Save File'

@sio.on('file_chunk_saved')
def ws_file_chunk_saved(data):
    job_name = data['job_name']
    file_path = data['file_path']

    current_jobs[job_name]['output_files_saved'][file_path] += 1
    current_jobs[job_name]['status'] = 'Ready to Save File'

@sio.on('file_saved')
def ws_file_saved(data):
    job_name = data['job_name']
    file_path = data['file_path']

    current_jobs[job_name]['output_files_saved'][file_path] = -1
    current_jobs[job_name]['current_output_files_saved_length'] += 1
    current_jobs[job_name]['status'] = 'Ready to Save File'

sio.connect('http://fim_node_connector:6000/')
update_loop()