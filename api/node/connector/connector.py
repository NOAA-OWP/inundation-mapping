import os
import random
import logging
import subprocess
from datetime import date

from flask import Flask
from flask_socketio import SocketIO, emit

DATA_PATH = os.environ.get('DATA_PATH')
DOCKER_IMAGE_PATH = os.environ.get('DOCKER_IMAGE_PATH')
GITHUB_REPO = os.environ.get('GITHUB_REPO')

app = Flask(__name__)
socketio = SocketIO(app)

handler_sid=None

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
    # TODO: For jobs with "Saving Outputs" status, start piping the outputs to the output handler, then set to 
    # TODO: For jobs with "Completed" status, the temp cloned repo and outputs should be deleted
    emit('client_update', current_jobs, broadcast=True)

@socketio.on('output_handler_connected')
def ws_output_handler_connected(new_handler_sid):
    handler_sid = new_handler_sid

@socketio.on('new_job')
def ws_new_job(job_params):
    print(job_params)
    # Validate the job parameters
    branch=job_params['git_branch']
    name=f"apijob_{job_params['job_name'].replace(' ', '_')}_apijob_{branch}_{date.today().strftime('%d%m%Y')}_{random.randint(0, 99999)}"
    hucs=job_params['huc']
    extent='FR'
    config_path='./foss_fim/config/params_template.env'

    # Clone github repo, with specific branch, to a temp folder
    print(f'cd /data/temp && git clone -b {branch} {GITHUB_REPO} {name}')
    subprocess.call(f'cd /data/temp && git clone -b {branch} {GITHUB_REPO} {name}', shell=True)

    # Kick off the new job as a docker container with the new cloned repo as the volume
    print(f'docker run -d --rm --name {name} -v {DATA_PATH}:/data/ -v {DATA_PATH}temp/{name}/:/foss_fim {DOCKER_IMAGE_PATH} fim_run.sh -u {hucs} -e {extent} -c {config_path} -n {name} -o')
    subprocess.call(f'docker run -d --rm --name {name} -v {DATA_PATH}:/data/ -v {DATA_PATH}temp/{name}/:/foss_fim {DOCKER_IMAGE_PATH} fim_run.sh -u {hucs} -e {extent} -c {config_path} -n {name} -o', shell=True)

if __name__ != '__main__':
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
