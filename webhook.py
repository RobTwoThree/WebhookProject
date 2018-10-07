import json
import MySQLdb
from flask import Flask, request, abort
from config import HOST, PORT, DB_HOST, DB_USER, DB_PASSWORD, DATABASE

app = Flask(__name__)

database = MySQLdb.connect(DB_HOST, DB_USER, DB_PASSWORD, DATABASE)

cursor = database.cursor()

@app.route('/submit', methods=['POST'])
def webhook():
    if request.method == 'POST':
        data = json.loads(request.data)
        #print(request.json)
        #print(data['message']['name'])
        #print(data['type'])
        message_type = data['type']
        gym_name = data['message']['name']
        gym_id = data['message']['gym_id']
        gym_lat = data['message']['latitude']
        gym_lon = data['message']['longitude']
        gym_team = data['message']['team']
        raid_level = data['message']['level']
        boss_id = data['message']['pokemon_id']
        boss_cp = data['message']['cp']
        boss_move_1 = data['message']['move_1']
        boss_move_2 = data['message']['move_2']
        raid_begin = data['message']['raid_begin']
        raid_end = data['message']['raid_end']
        
        if message_type == "raid":
            insert_query = "INSERT INTO raids(id, external_id, fort_id, level, pokemon_id, move_1, move_2, time_spawn, time_battle, time_end, cp) VALUES (null, null, " + str(gym_id) + ", " + str(raid_level) + ", " + str(boss_id) + ", null, null, null, " + str(raid_begin) + ", " + str(raid_end) + ", null);"
            
            try:
                cursor.execute(insert_query)
                database.commit()
                print("INSERT EXECUTED")
            except:
                database.rollback()
                print("INSERT FAILED")
        
            print(insert_query)
        return 'Webhook message sent successfully.', 200

    else:
        abort(400)


if __name__ == '__main__':
    app.run(host=HOST,port=PORT)
