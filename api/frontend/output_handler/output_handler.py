import os
import time

import socketio

SOCKET_URL = os.environ.get('SOCKET_URL')

pending_files = {}

def handle_outputs(data):
    name = f"{data['job_name']}_{data['file_name']}"
    if name not in pending_files:
        pending_files[name] = {
            'locked': False,
            'current_index': 0,
            'nice_name': data['nice_name'],
            'job_name': data['job_name'],
            'directory_path': data['directory_path'],
            'file_name': data['file_name']
        }

    pending_files[name][data['chunk_index']] = data['file_chunk']


    work_to_do = True
    while work_to_do:
        work_to_do = False

        nice_name = pending_files[name]['nice_name']
        job_name = pending_files[name]['job_name']
        directory_path = pending_files[name]['directory_path']
        file_name = pending_files[name]['file_name']

        # If the last chunk just got added, waiting to write any potentially missing data to file
        if data['file_chunk'] == None and pending_files[name]['locked']:
            while name in pending_files and pending_files[name]['locked']:
                print("EOF, waiting till not locked")
                sio.sleep(0.5)
            if not name in pending_files:
                return

        # To ensure that the files are being written in the correct order, use current_index
        # to write the correct file chunk.
        if not pending_files[name]['locked'] and pending_files[name]['current_index'] in pending_files[name]:
            pending_files[name]['locked'] = True
            file_chunk = pending_files[name].pop(pending_files[name]['current_index'])

            # End of file
            if file_chunk == None:
                if sio.connected:
                    sio.emit('output_handler_finished_file', {'job_name': job_name, 'file_path': f"{directory_path}/{file_name}"})
                    print("finished with file", name, directory_path, file_name)
                    # files_to_delete.append(name)
                    pending_files.pop(name)
                continue
            else:
                # Not end of file, keep looping till you can't do more work
                work_to_do = True

            # Create folder if it doesn't yet exist and set writing mode
            mode = 'ab'
            if pending_files[name]['current_index'] == 0:
                mode = 'wb'
                try:
                    os.makedirs(f"/data/outputs/{nice_name}/{directory_path}")
                except:
                    pass
                
            # Write binary data to file
            with open(f"/data/outputs/{nice_name}/{directory_path}/{file_name}", mode) as binary_file:
                print(f"Writing chunk {pending_files[name]['current_index']} for file {directory_path}/{file_name}")
                binary_file.write(file_chunk)

            # Remove current chunk from list
            pending_files[name]['current_index'] += 1
            pending_files[name]['locked'] = False
    
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