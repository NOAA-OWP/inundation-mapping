import os

import socketio

SOCKET_URL = os.environ.get('SOCKET_URL')

def handle_outputs():
    pass

sio = socketio.Client()

@sio.event
def connect():
    print("Output Handler Connected!")
    sio.emit('output_handler_connected', sio.sid)

@sio.event
def disconnect():
    print('disconnected from server')

sio.connect(SOCKET_URL)
sio.wait()