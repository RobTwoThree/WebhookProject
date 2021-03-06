import json
import MySQLdb
import os
from datetime import datetime, timedelta
from flask import Flask, request, jsonify
from config import HOST, PORT, DB_HOST, DB_USER, DB_PASSWORD, DATABASE

def temp_token():
    import binascii
    temp_token = binascii.hexlify(os.urandom(24))
    return temp_token.decode('utf-8')

WEBHOOK_VERIFY_TOKEN = os.getenv('WEBHOOK_VERIFY_TOKEN')
CLIENT_AUTH_TIMEOUT = 24 # in Hours

app = Flask(__name__)

database = MySQLdb.connect(DB_HOST, DB_USER, DB_PASSWORD, DATABASE)

cursor = database.cursor()

authorized_clients = {}

@app.route('/submit', methods=['GET', 'POST'])
def webhook():
    if request.method == 'GET':
        verify_token = request.args.get('verify_token')
        if verify_token == WEBHOOK_VERIFY_TOKEN:
            authorized_clients[request.remote_addr] = datetime.now()
            return jsonify({'status':'success'}), 200
        else:
            return jsonify({'status':'bad token'}), 401

    elif request.method == 'POST':
        client = request.remote_addr
        if client in authorized_clients:
            if datetime.now() - authorized_clients.get(client) > timedelta(hours=CLIENT_AUTH_TIMEOUT):
                authorized_clients.pop(client)
                return jsonify({'status':'authorization timeout'}), 401
            else:
                print(request.json)
                return jsonify({'status':'success'}), 200
        else:
            return jsonify({'status':'not authorized'}), 401

    else:
        return '', 400

if __name__ == '__main__':
    if WEBHOOK_VERIFY_TOKEN is None:
        print('WEBHOOK_VERIFY_TOKEN has not been set in the environment.\nGenerating random token...')
        token = temp_token()
        print('Token: %s' % token)
        WEBHOOK_VERIFY_TOKEN = token
    app.run(host=HOST,port=PORT)
