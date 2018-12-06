import json
import MySQLdb
import datetime
import calendar
import time
from flask import Flask, request, abort
from config import HOST, PORT, DB_HOST, DB_USER, DB_PASSWORD, DATABASE

app = Flask(__name__)

database = MySQLdb.connect(DB_HOST, DB_USER, DB_PASSWORD, DATABASE)

cursor = database.cursor()

@app.route('/submit', methods=['POST'])
def webhook():
    if request.method == 'POST':
        data = json.loads(request.data)
        print(request.json)
        
        print(data[0]['type'])
        print(data[0]['message']['name'])
        print(data[0]['message']['pokemon_id'])
        
        message_type = data[0]['type']
        gym_name = data[0]['message']['name']
        gym_id = data[0]['message']['gym_id']
        gym_lat = data[0]['message']['latitude']
        gym_lon = data[0]['message']['longitude']
        gym_team = data[0]['message']['team']
        raid_level = data[0]['message']['level']
        raid_begin = data[0]['message']['raid_begin']
        raid_end = data[0]['message']['raid_end']
        
        #Check if message has pokemon_id sent
        if ( data[0]['message']['pokemon_id'] ):
            boss_id = data[0]['message']['pokemon_id']
            boss_cp = data[0]['message']['cp']
            boss_move_1 = data[0]['message']['move_1']
            boss_move_2 = data[0]['message']['move_2']
        else:
            boss_id = 0
        
        current_time = datetime.datetime.utcnow()
        
        if message_type == "raid":

            gym_id_query = "SELECT id FROM forts WHERE external_id='" + str(gym_id) + "';"
            cursor.execute(gym_id_query)
            gym_ids = cursor.fetchall()
            gym_id_count = cursor.rowcount
            if ( gym_id_count ):
                gym_id = gym_ids[0][0]
                insert_query = "INSERT INTO raids(id, external_id, fort_id, level, pokemon_id, move_1, move_2, time_spawn, time_battle, time_end, cp) VALUES (null, null, " + str(gym_id) + ", " + str(raid_level) + ", " + str(boss_id) + ", null, null, null, " + str(raid_begin) + ", " + str(raid_end) + ", null);"
                
                update_query = "UPDATE raids SET pokemon_id='" + str(boss_id) + "' WHERE fort_id='" + str(gym_id)+ "' AND time_end>'" + str(calendar.timegm(current_time.timetuple())) + "';"
                
                existing_raid_check_query = "SELECT id, fort_id, pokemon_id, time_end FROM raids WHERE fort_id='" + str(gym_id) + "' AND time_end>'" + str(calendar.timegm(current_time.timetuple())) + "';"
                
                print(insert_query)
                print(update_query)
                print(existing_raid_check_query)
                
                try:
                    cursor.execute(existing_raid_check_query)
                    raid_data = cursor.fetchall()
                    raid_count = cursor.rowcount
            
                    #If raid entry already exists and is not an egg, update the boss_id
                    if ( raid_count ):
                        current_boss_id = raid_data[0][2]
                        
                        if ( current_boss_id == 0 ): #Need to determine what value this is
                            try:
                                cursor.execute(update_query)
                                database.commit()
                                print("RAID UPDATED")
                            except:
                                database.rollback()
                                print("RAID UPDATE FAILED")
                        else:
                            pass
                        return 'Duplicate webhook message was ignored.', 200
                    else:
                        try:
                            cursor.execute(insert_query)
                            database.commit()
                            print("INSERT EXECUTED")
                        except:
                            database.rollback()
                            print("INSERT FAILED")
                        return 'Webhook message sent successfully.', 200
                except:
                    database.rollback()
                    print("EXISTING RAID QUERY FAILED")
            else:
                print("Gym ID Not Found.")
                return 'Gym ID was not found.', 500

    else:
        abort(400)


if __name__ == '__main__':
    app.run(host=HOST,port=PORT)
