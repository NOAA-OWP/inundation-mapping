import eventlet
eventlet.monkey_patch()

import os
import re
import random
import logging
import subprocess
from datetime import date

from flask import Flask, request
from flask_socketio import SocketIO, emit

DATA_PATH = os.environ.get('DATA_PATH')
DOCKER_IMAGE_PATH = os.environ.get('DOCKER_IMAGE_PATH')
SOCKET_URL = os.environ.get('SOCKET_URL')
FRONTEND_URL = os.environ.get('FRONTEND_URL')
GITHUB_REPO = os.environ.get('GITHUB_REPO')

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins=[SOCKET_URL, FRONTEND_URL, "http://fim_node_connector:6000"])

shared_data = {
    'handler_sid': None,
    'updater_sid': None
}

@app.route('/')
def main():
    return '<h1>Nothing to see here....</h1>'

@socketio.on('connect')
def ws_conn():
    print('user connected!')
    emit('is_connected', True)

@socketio.on('disconnect')
def ws_disconn():
    print('user disconnected!')
    emit('is_connected', False)

@socketio.on('update')
def ws_update(current_jobs):
    emit('client_update', current_jobs, broadcast=True)

@socketio.on('output_handler_connected')
def ws_output_handler_connected():
    print('handler_sid: ', request.sid)
    shared_data['handler_sid'] = request.sid
    emit('retry_saving_files', room=shared_data['updater_sid'])

@socketio.on('updater_connected')
def ws_updater_connected():
    print('updater_sid: ', request.sid)
    shared_data['updater_sid'] = request.sid
    emit('retry_saving_files', room=shared_data['updater_sid'])

@socketio.on('ready_for_output_handler')
def ws_ready_for_output_handler(data):
    nice_name = data['nice_name']
    job_name = data['job_name']
    path = data['path']

    print(f"handler_sid: {shared_data['handler_sid']}")

    if shared_data['handler_sid'] == None:
        print("output handler not connected!")
        emit('retry_saving_files')
        return

    # Split up path into parts for the output handler
    path_parts = re.search(rf"/data/outputs/{job_name}/(.+)/(.+)", path)
    directory_path = path_parts.group(1)
    file_name = path_parts.group(2)

    with open(path, "rb") as binary_file:
        print("Sending to output handler", path)
        
        # Read and emit file chunk by chunk (50MB at a time)
        chunk_index = 0
        file_chunk = binary_file.read(52428800)
        # file_chunk = binary_file.read(104857600)
        while file_chunk:
            print("Sending to output handler", path, "Chunk:", chunk_index)
            emit('new_job_outputs', {
                'nice_name': nice_name,
                'job_name': job_name,
                'directory_path': directory_path,
                'file_name': file_name,
                'file_chunk': file_chunk,
                'chunk_index': chunk_index
            }, room=shared_data['handler_sid'])

            chunk_index += 1
            file_chunk = binary_file.read(52428800)
            # file_chunk = binary_file.read(104857600)

    # Send None to indicate end of file
    print("Sending to output handler", path, "Chunk:", chunk_index, "EOF")
    emit('new_job_outputs', {
        'nice_name': nice_name,
        'job_name': job_name,
        'directory_path': directory_path,
        'file_name': file_name,
        'file_chunk': None,
        'chunk_index': chunk_index
    }, room=shared_data['handler_sid'])

@socketio.on('output_handler_finished_file')
def ws_output_handler_finished_file(data):
    job_name = data['job_name']
    file_path = data['file_path']

    print('done saving', job_name, file_path)
    emit('file_saved', {
        'job_name': job_name,
        'file_path': f"/data/outputs/{job_name}/{file_path}"
    }, room=shared_data['updater_sid'])

@socketio.on('new_job')
def ws_new_job(job_params):
    validation_errors = []

    # Validate Hucs Name Option
    hucs = ' '.join(job_params['hucs'].replace(',', ' ').split())
    invalid_hucs = re.search('[a-zA-Z]', hucs)
    if invalid_hucs: validation_errors.append('Invalid Huc(s)')

    # Validate Git Branch Option
    branch = ''
    branch_exists = subprocess.run(['git', 'ls-remote', '--heads', GITHUB_REPO, job_params['git_branch'].replace(' ', '_')], stdout=subprocess.PIPE).stdout.decode('utf-8')
    if branch_exists: branch = job_params['git_branch'].replace(' ', '_')
    else: validation_errors.append('Git Branch Does Not Exist')

    # Validate Job Name Option
    job_name = f"apijob_{job_params['job_name'].replace(' ', '_')[0:50]}_apijob_{branch}_{date.today().strftime('%d%m%Y')}_{random.randint(0, 99999)}"

    # Validate Extent Option
    extent = ''
    if job_params['extent'] == 'FR': extent = 'FR'
    elif job_params['extent'] == 'MS': extent = 'MS'
    else: validation_errors.append('Invalid Extent Option')

    # Validate Configuration Option
    config_path = ''
    if job_params['configuration'] == 'default': config_path = './foss_fim/config/params_template.env'
    elif job_params['configuration'] == 'calibrated': config_path = './foss_fim/config/params_calibrated.env'
    else: validation_errors.append('Invalid Configuration Option')
    
    # Validate Dev Run Option
    if job_params['dev_run'] : dev_run = True
    else: dev_run = False

    if len(validation_errors) == 0:
        # Clone github repo, with specific branch, to a temp folder
        print(f'cd /data/temp && git clone -b {branch} {GITHUB_REPO} {job_name}')
        subprocess.call(f'cd /data/temp && git clone -b {branch} {GITHUB_REPO} {job_name}', shell=True)

        # TODO: instead of starting the job right away, add it to a queue until there are enough resources to run it. Also track things like huc count and huc type (6 or 8)

        # Kick off the new job as a docker container with the new cloned repo as the volume
        print(f"docker run -d --rm --name {job_name} -v {DATA_PATH}:/data/ -v {DATA_PATH}temp/{job_name}/:/foss_fim {DOCKER_IMAGE_PATH} fim_run.sh -u \"{hucs}\" -e {extent} -c {config_path} -n {job_name} -o {'' if dev_run else '-p'}")
        subprocess.call(f"docker run -d --rm --name {job_name} -v {DATA_PATH}:/data/ -v {DATA_PATH}temp/{job_name}/:/foss_fim {DOCKER_IMAGE_PATH} fim_run.sh -u \"{hucs}\" -e {extent} -c {config_path} -n {job_name} -o {'' if dev_run else '-p'}", shell=True)
        emit('job_started', 'fim_run')
    else:
        emit('validation_errors', validation_errors)

if __name__ == '__main__':
    socketio.run(app, host="0.0.0.0", port="6000")
