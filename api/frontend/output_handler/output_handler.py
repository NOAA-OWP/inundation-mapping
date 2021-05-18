import os
import time

import socketio

SOCKET_URL = os.environ.get('SOCKET_URL')

def handle_outputs(data):
    job_name = data['job_name']
    directory_path = data['directory_path']
    file_name = data['file_name']
    file_chunk = data['file_chunk']
    chunk_index = data['chunk_index']

    # Create folder if it doesn't yet exist and set writing mode
    mode = 'ab'
    if chunk_index == 0:
        mode = 'wb'
        try:
            os.makedirs(directory_path)
        except:
            pass
        
    # Write binary data to file
    with open(f"{directory_path}/{file_name}", mode) as binary_file:
        print(f"Writing chunk {chunk_index} for file {directory_path}/{file_name}")
        binary_file.write(file_chunk)

    sio.emit('output_handler_finished_file_chunk', {'job_name': job_name, 'file_path': f"{directory_path}/{file_name}"})

sio = socketio.Client()

@sio.event
def connect():
    print("Output Handler Connected!")
    sio.emit('output_handler_connected')

@sio.event
def disconnect():
    print('disconnected from server')

@sio.on('new_job_outputs')
def ws_new_job_outputs(data):
    handle_outputs(data)

sio.connect(SOCKET_URL)