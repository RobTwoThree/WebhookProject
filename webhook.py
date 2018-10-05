import json
import MySQLdb
from flask import Flask, request, abort
from config import HOST, PORT, DB_HOST, DB_USER, DB_PASSWORD, DATABASE

app = Flask(__name__)

database = MySQLdb.connect(DB_HOST, DB_USER, DB_PASSWORD, DATABASE)

cursor = database.cursor()

@app.route('/webhook', methods=['POST'])
def webhook():
    if request.method == 'POST':
        data = json.loads(request.data)
        print(request.json)
        #print(data['message']['name'])
        #print(data['type'])
        #if data['type'] == "raid":
        #    print("ITS A RAID")
        return 'Success', 200

    else:
        abort(400)


if __name__ == '__main__':
    app.run(host=HOST,port=PORT)
