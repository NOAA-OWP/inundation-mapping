import eventlet
eventlet.monkey_patch()

import os
import re
import time
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
def ws_update(data):
    emit('client_update', data, broadcast=True)

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
    job_name = data['job_name']
    path = data['path']
    chunk_index = data['chunk_index']

    if shared_data['handler_sid'] == None:
        print("output handler not connected!")
        emit('retry_saving_files')
        return

    # Split up path into parts for the output handler
    path_parts = re.search(rf"(.+)/(.+)", path)
    directory_path = path_parts.group(1)
    file_name = path_parts.group(2)

    file_read_start = time.time()
    with open(path, "rb") as binary_file:
        # Read and emit file chunk by chunk (50MB at a time)
        binary_file.seek(chunk_index * 52428800)
        file_chunk = binary_file.read(52428800)

        if len(file_chunk) == 0:
            print('End of File')
            emit('file_saved', {
                'job_name': job_name,
                'file_path': path
            }, room=shared_data['updater_sid'])
            return

        print("Sending to output handler", path, "Chunk:", chunk_index)
        emit('new_job_outputs', {
            'job_name': job_name,
            'directory_path': directory_path,
            'file_name': file_name,
            'file_chunk': file_chunk,
            'chunk_index': chunk_index
        }, room=shared_data['handler_sid'])

@socketio.on('output_handler_finished_file_chunk')
def output_handler_finished_file_chunk(data):
    job_name = data['job_name']
    file_path = data['file_path']

    print('done saving chunk', job_name, file_path)
    emit('file_chunk_saved', {
        'job_name': job_name,
        'file_path': file_path,
    }, room=shared_data['updater_sid'])

@socketio.on('new_job')
def ws_new_job(job_params):
    job_type = job_params['job_type']

    if job_type == 'fim_run':
        validation_errors = []

        # Get Preset Option
        preset = job_params['preset']

        # Validate Hucs Name Option
        if preset == 'custom':
            hucs_raw = job_params['hucs'].replace(',', ' ').split()
            parallel_jobs = len(hucs_raw)
            hucs_type = len(hucs_raw[0])
            hucs = ' '.join(hucs_raw)
            invalid_hucs = re.search('[^0-9 ]', hucs)
            if invalid_hucs: validation_errors.append('Invalid Huc(s)')
        else:
            hucs = f"/data/inputs/huc_lists/{preset}"
            parallel_jobs = 0
            hucs_type = 0

        # Validate Git Branch Option
        branch = ''
        branch_exists = subprocess.run(['git', 'ls-remote', '--heads', GITHUB_REPO, job_params['git_branch'].replace(' ', '_')], stdout=subprocess.PIPE).stdout.decode('utf-8')
        if branch_exists: branch = job_params['git_branch'].replace(' ', '_')
        else: validation_errors.append('Git Branch Does Not Exist')

        # Validate Extent Option
        valid_extents = ['FR', 'MS']
        extents = []
        for extent in job_params['extents']:
            if extent in valid_extents:
                extents.append(extent)
            else:
                validation_errors.append('Invalid Extent Option')

        # Validate Configuration Option
        config_path = ''
        if job_params['configuration'] == 'default': config_path = './foss_fim/config/params_template.env'
        elif job_params['configuration'] == 'calibrated': config_path = './foss_fim/config/params_calibrated.env'
        else: validation_errors.append('Invalid Configuration Option')
        
        # Validate Dev Run Option
        if job_params['dev_run'] : dev_run = True
        else: dev_run = False

        # Validate Viz Run Option
        if job_params['viz_run'] : viz_run = True
        else: viz_run = False

        if len(validation_errors) == 0:
            for extent in extents:
                # Validate Job Name Option
                job_name = f"apijob_{job_params['job_name'].replace(' ', '_')[0:50]}_fim_run_{extent.lower()}{'_c' if job_params['configuration'] == 'calibrated' else ''}{'_v' if viz_run == True else ''}_apijob_{branch}_{date.today().strftime('%d%m%Y')}_{random.randint(0, 99999)}"
                print(f"adding job {job_name} {branch} {preset} {hucs} {parallel_jobs} {hucs_type} {extent.lower()} {config_path} {dev_run} {viz_run}")
                emit('add_job_to_queue', {
                    'job_type': 'fim_run',
                    'job_name': job_name,
                    'branch': branch,
                    'hucs': hucs,
                    'parallel_jobs': parallel_jobs,
                    'hucs_type': hucs_type,
                    'extent': extent,
                    'config_path': config_path,
                    'dev_run': dev_run,
                    'viz_run': viz_run,
                }, room=shared_data['updater_sid'])
                print('fim_run job added')
                emit('job_added', 'fim_run')
        else:
            emit('validation_errors', validation_errors)

    elif job_type == 'release':
        job_version_major = job_params['job_version_major']
        job_version_minor = job_params['job_version_minor']
        job_version_patch = job_params['job_version_patch']

        # TODO: validate version number

        job_name_base = f"fim_3_{job_version_major}_{job_version_minor}_{job_version_patch}"

        prev_job_version_major = job_params['prev_job_version_major']
        prev_job_version_minor = job_params['prev_job_version_minor']
        prev_job_version_patch = job_params['prev_job_version_patch']

        prev_version_base = f"fim_3_{prev_job_version_major}_{prev_job_version_minor}_{prev_job_version_patch}"

        huc_lists = ['/data/inputs/huc_lists/included_huc8.lst', '/data/inputs/huc_lists/included_huc8_ms.lst']
        extents = ['FR', 'MS']

        for hucs, extent in zip(huc_lists, extents):
            # Validate Job Name Option
            prev_version = f"{prev_version_base}_{extent.lower()}_c"
            job_name = f"apijob_{job_name_base}_{extent.lower()}_c_apijob_dev_{date.today().strftime('%d%m%Y')}_{random.randint(0, 99999)}"
            print(f"adding job {job_name} {hucs} {extent.lower()}")
            emit('add_job_to_queue', {
                'job_type': 'release',
                'job_name': job_name,
                'hucs': hucs,
                'extent': extent,
                'previous_major_fim_version': prev_version
            }, room=shared_data['updater_sid'])
            print('release job added')
            emit('job_added', 'release')
    
    @socketio.on('cancel_job')
    def ws_cancel_job(job_params):
        # Validate Job Name Option
        job_name = job_params['job_name']

        emit('remove_job_from_queue', {'job_name': job_name}, room=shared_data['updater_sid'])
        print('job canceled')
        emit('job_canceled', 'fim_run')
   

if __name__ == '__main__':
    socketio.run(app, host="0.0.0.0", port="6000")
