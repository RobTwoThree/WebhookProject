import json
import MySQLdb
import datetime
import calendar
import time
import logging
from flask import Flask, request, abort
from config import HOST, PORT, DB_HOST, DB_USER, DB_PASSWORD, DATABASE, DEBUG

logging.basicConfig(filename='debug_webhook.log',level=logging.DEBUG)

app = Flask(__name__)

database = MySQLdb.connect(DB_HOST, DB_USER, DB_PASSWORD, DATABASE)

cursor = database.cursor()

print('Webhook Started at ' + str(time.strftime('%I:%M %p on %m.%d.%y',  time.localtime(calendar.timegm(datetime.datetime.utcnow().timetuple())))))
logging.info('Webhook Started at ' + str(time.strftime('%I:%M %p on %m.%d.%y',  time.localtime(calendar.timegm(datetime.datetime.utcnow().timetuple())))))

def proces_raid(data):
    current_time = datetime.datetime.utcnow()
    
    #Load payload data into variables
    gym_name = data[0]['message']['name']
    gym_id = data[0]['message']['gym_id']
    gym_lat = data[0]['message']['latitude']
    gym_lon = data[0]['message']['longitude']
    gym_url = data[0]['message']['gym_url']
    gym_team = data[0]['message']['team']
    raid_level = data[0]['message']['level']
    raid_begin = data[0]['message']['raid_begin']
    raid_end = data[0]['message']['raid_end']
    
    #Check if message has pokemon_id sent. If not, its an egg
    if 'pokemon_id' in data[0]['message']:
        boss_id = data[0]['message']['pokemon_id']
        boss_cp = data[0]['message']['cp']
        boss_move_1 = data[0]['message']['move_1']
        boss_move_2 = data[0]['message']['move_2']
    else:
        boss_id = 0

    gym_id_query = "SELECT id FROM forts WHERE external_id='" + str(gym_id) + "';"
    database.ping(True)
    cursor.execute(gym_id_query)
    gym_ids = cursor.fetchall()
    gym_id_count = cursor.rowcount
    if ( gym_id_count ):
        gym_id = gym_ids[0][0]
        insert_query = "INSERT INTO raids(id, external_id, fort_id, level, pokemon_id, move_1, move_2, time_spawn, time_battle, time_end, cp) VALUES (null, null, " + str(gym_id) + ", " + str(raid_level) + ", " + str(boss_id) + ", null, null, null, " + str(raid_begin) + ", " + str(raid_end) + ", null);"
        
        update_query = "UPDATE raids SET pokemon_id='" + str(boss_id) + "' WHERE fort_id='" + str(gym_id)+ "' AND time_end>'" + str(calendar.timegm(current_time.timetuple())) + "';"
        
        existing_raid_check_query = "SELECT id, fort_id, pokemon_id, time_end FROM raids WHERE fort_id='" + str(gym_id) + "' AND time_end>'" + str(calendar.timegm(current_time.timetuple())) + "';"
        
        fort_sightings_query = "SELECT id, fort_id, team FROM fort_sightings WHERE fort_id='" + str(gym_id) + "';"
        
        if ( DEBUG ):
            print("DEBUG: " + insert_query)
            print("DEBUG: " + update_query)
            print("DEBUG: " + existing_raid_check_query)
            logging.debug(insert_query)
            logging.debug(update_query)
            logging.debug(existing_raid_check_query)
        
        try:
            database.ping(True)
            cursor.execute(existing_raid_check_query)
            raid_data = cursor.fetchall()
            raid_count = cursor.rowcount
            
            #If raid entry already exists and current boss_id is provided in message, update entry
            if ( raid_count and boss_id != 0 ):
                if ( DEBUG ):
                    print("DEBUG: raid_data[0][2] = " + str(raid_data[0][2]))
                    logging.debug("raid_data[0][2] = " + str(raid_data[0][2]))
            
                #If exisiting pokemon_id in table is an egg, update with new boss_id
                if ( raid_data[0][2] == 0 ):
                    try:
                        database.ping(True)
                        cursor.execute(update_query)
                        database.commit()
                        print("RAID UPDATED. Old Boss:" + str(raid_data[0][2]) + " New Boss:" + str(boss_id))
                        logging.info("RAID UPDATED. Old Boss:" + str(raid_data[0][2]) + " New Boss:" + str(boss_id))
                    except:
                        database.rollback()
                        print("RAID UPDATE FAILED.")
                        logging.info("RAID UPDATE FAILED.")
                else:
                    pass
                return 'Duplicate webhook message was ignored.\n', 200
            else:
                try:
                    database.ping(True)
                    cursor.execute(insert_query)
                    database.commit()
                    print("INSERT EXECUTED. Gym:" + str(gym_id) + " Raid:" + str(raid_level) + " Boss:" + str(boss_id))
                    logging.info("INSERT EXECUTED. Gym:" + str(gym_id) + " Raid:" + str(raid_level) + " Boss:" + str(boss_id))
                except:
                    database.rollback()
                    print("INSERT FAILED.")
                    logging.info("INSERT FAILED.")
                
                #Need to check if fort_id is in fort_sightings. If not, insert as new entry, otherwise update.
                database.ping(True)
                cursor.execute(fort_sightings_query)
                fs_count = cursor.rowcount
                
                if ( fs_count ):
                    fort_sightings_update = "UPDATE fort_sightings SET team='" + str(gym_team) + "', guard_pokemon_id='" + str(boss_id) + "' WHERE fort_id='" + str(gym_id) + "';"
                
                    try:
                        database.ping(True)
                        cursor.execute(fort_sightings_update)
                        database.commit()
                        print("UPDATED FORT_SIGHTINGS. Gym:" + str(gym_id) + " Boss:" + str(boss_id) + " Team:" + str(gym_team))
                        logging.info("UPDATED FORT_SIGHTINGS. Gym:" + str(gym_id) + " Boss:" + str(boss_id) + " Team:" + str(gym_team))
                    except:
                        database.rollback()
                        print("UPDATE TO FORT_SIGHTINGS FAILED.")
                        logging.info("UPDATE TO FORT_SIGHTINGS FAILED.")

                else:
                    fort_sightings_insert = "INSERT INTO fort_sightings(fort_id, team, last_modified, guard_pokemon_id) VALUES (" + str(gym_id) + ", " + str(gym_team) + ", " + str(calendar.timegm(current_time.timetuple())) + ", " + str(boss_id) + ");"
                    
                    try:
                        database.ping(True)
                        cursor.execute(fort_sightings_insert)
                        database.commit()
                        print("INSERTED INTO FORT_SIGHTINGS. Gym:" + str(gym_id) + " Boss:" + str(boss_id) + " Team:" + str(gym_team))
                        logging.info("INSERTED INTO FORT_SIGHTINGS. Gym:" + str(gym_id) + " Boss:" + str(boss_id) + " Team:" + str(gym_team))
                    except:
                        database.rollback()
                        print("INSERT INTO FORT_SIGHTINGS FAILED.")
                        logging.info("INSERT INTO FORT_SIGHTINGS FAILED.")

                return 'Webhook message sent successfully.\n', 200
        except:
            database.rollback()
            print("EXISTING RAID QUERY FAILED")
    else:
        print("Gym ID Not Found.")
        logging.info("Gym ID Not Found.")
        
        add_gym_query = "INSERT INTO forts(external_id, lat, lon, name, url) VALUES('" + str(gym_id) + "', " +  str(gym_lat) + ", " + str(gym_lon) + ", '" + str(gym_name) + "', '" + str(gym_url) + "');"
        
        if ( DEBUG ):
           print("DEBUG: " + str(add_gym_query))
           logging.debug(str(add_gym_query))
        
        try:
            database.ping(True)
            cursor.execute(add_gym_query)
            database.commit()
            print("GYM ADDED. Gym:" + str(gym_id) + " Lat:" + str(gym_lat) + " Lon:" + str(gym_lon) + " Name:" + str(gym_name) + " URL:" + str(gym_url))
            logging.info("GYM ADDED. Gym:" + str(gym_id) + " Lat:" + str(gym_lat) + " Lon:" + str(gym_lon) + " Name:" + str(gym_name) + " URL:" + str(gym_url))
            return 'Unknown gym. Insert successful.\n', 200
        except:
            database.rollback()
            print("GYM INSERT FAILED.")
            logging.info("GYM INSERT FAILED.")
            return 'Unknown gym. Insert failed.\n', 500

def process_pokemon(data):
    current_time = datetime.datetime.utcnow()
    
    print("POKEMON MESSAGE: " + str(data))
    return 'Pokemon type processed.\n', 200

@app.route('/submit', methods=['POST'])
def webhook():
    if request.method == 'POST':
        data = json.loads(request.data)
        print("MESSAGE: " + str(request.json))
        logging.info("MESSAGE: " + str(request.json))
        
        if ( DEBUG ):
            print("DEBUG: type=" + str(data[0]['type']))
            print("DEBUG: name=" + str(data[0]['message']['name']))
            logging.debug("type=" + str(data[0]['type']))
            logging.debug("name=" + str(data[0]['message']['name']))
        
        message_type = data[0]['type']
        
        print("Message is type: " + str(message_type))
        logging.info("Message is type: "  + str(message_type))
        if message_type == "raid":
            result = proces_raid(data)
            return result

        if message_type == "pokemon":
            result = process_pokemon(data)
            return result
    else:
        abort(400)


if __name__ == '__main__':
    app.run(host=HOST,port=PORT)
