import os
from gevent import monkey
monkey.patch_all()

from flask import Flask, render_template, request

SOCKET_URL = os.environ.get('SOCKET_URL')

app = Flask(__name__)

@app.route('/')
def main():
    return render_template('index.html', socket_url=SOCKET_URL)

if __name__ == '__main__':
    app.run("0.0.0.0", port=5000)