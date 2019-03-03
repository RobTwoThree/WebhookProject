import json
import MySQLdb
import datetime
import pytz
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
    gym_url = data[0]['message']['url']
    gym_team = data[0]['message']['team_id']
    raid_level = data[0]['message']['level']
    raid_begin = data[0]['message']['start']
    raid_end = data[0]['message']['end']
        
    #Check if message has pokemon_id sent. If not, its an egg
    if 'pokemon_id' in data[0]['message']:
        boss_id = data[0]['message']['pokemon_id']
        if boss_id != 0:
            boss_cp = data[0]['message']['cp']
            boss_move_1 = data[0]['message']['move_1']
            boss_move_2 = data[0]['message']['move_2']
        else:
            boss_cp = "null"
            boss_move_1 = "null"
            boss_move_2 = "null"
    else:
        boss_id = 0
        boss_cp = "null"
        boss_move_1 = "null"
        boss_move_2 = "null"

    gym_id_query = "SELECT id FROM forts WHERE external_id='" + str(gym_id) + "';"
    database.ping(True)
    cursor.execute(gym_id_query)
    gym_ids = cursor.fetchall()
    gym_id_count = cursor.rowcount
    if ( gym_id_count ):
        gym_id = gym_ids[0][0]
        insert_query = "INSERT INTO raids(id, external_id, fort_id, level, pokemon_id, move_1, move_2, time_spawn, time_battle, time_end, cp) VALUES (null, null, " + str(gym_id) + ", " + str(raid_level) + ", " + str(boss_id) + ", " + str(boss_move_1) + ", " + str(boss_move_2) + ", null, " + str(raid_begin) + ", " + str(raid_end) + ", " + str(boss_cp) + ");"
                
        update_query = "UPDATE raids SET pokemon_id='" + str(boss_id) + "', move_1='" + str(boss_move_1) + "', move_2='" + str(boss_move_2) + "', cp='" + str(boss_cp) + "' WHERE fort_id='" + str(gym_id)+ "' AND time_end>'" + str(calendar.timegm(current_time.timetuple())) + "';"
                
        existing_raid_check_query = "SELECT id, fort_id, pokemon_id, time_end FROM raids WHERE fort_id='" + str(gym_id) + "' AND time_end>'" + str(calendar.timegm(current_time.timetuple())) + "';"
                
        fort_sightings_query = "SELECT id, fort_id, team FROM fort_sightings WHERE fort_id='" + str(gym_id) + "';"
                
        if ( DEBUG ):
            print("RAID DEBUG: " + insert_query)
            print("RAID DEBUG: " + update_query)
            print("RAID DEBUG: " + existing_raid_check_query)
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
                    print("RAID DEBUG: raid_data[0][2] = " + str(raid_data[0][2]))
                    logging.debug("RAID DEBUG: raid_data[0][2] = " + str(raid_data[0][2]))
              
                #If exisiting pokemon_id in table is an egg, update with new boss_id
                if ( raid_data[0][2] == 0 ):
                    try:
                        database.ping(True)
                        cursor.execute(update_query)
                        database.commit()
                        print("RAID UPDATED. Old Boss:" + str(raid_data[0][2]) + " New Boss:" + str(boss_id) + " Move 1: " + str(boss_move_1) + " Move 2: " + str(boss_move_2) + " CP: " + str(boss_cp) + "\n")
                        logging.info("RAID UPDATED. Old Boss:" + str(raid_data[0][2]) + " New Boss:" + str(boss_id) + " Move 1: " + str(boss_move_1) + " Move 2: " + str(boss_move_2) + " CP: " + str(boss_cp) + "\n")
                    except:
                        database.rollback()
                        print("RAID UPDATE FAILED.\n")
                        logging.info("RAID UPDATE FAILED.\n")
                else:
                    pass
                return 'Duplicate webhook message was ignored.\n', 200
            else:
                try:
                    database.ping(True)
                    cursor.execute(insert_query)
                    database.commit()
                    print("RAID INSERT EXECUTED. Gym:" + str(gym_id) + " Raid:" + str(raid_level) + " Boss:" + str(boss_id))
                    logging.info("RAID INSERT EXECUTED. Gym:" + str(gym_id) + " Raid:" + str(raid_level) + " Boss:" + str(boss_id))
                except:
                    database.rollback()
                    print("RAID INSERT FAILED.")
                    logging.info("RAID INSERT FAILED.")
                        
                #Need to check if fort_id is in fort_sightings. If not, insert as new entry, otherwise update.
                database.ping(True)
                cursor.execute(fort_sightings_query)
                fs_count = cursor.rowcount
                        
                if ( fs_count ):
                    fort_sightings_update = "UPDATE fort_sightings SET team='" + str(gym_team) + "' WHERE fort_id='" + str(gym_id) + "';"
                            
                    try:
                        database.ping(True)
                        cursor.execute(fort_sightings_update)
                        database.commit()
                        print("RAID UPDATED FORT_SIGHTINGS. Gym:" + str(gym_id) + " Team:" + str(gym_team) + "\n")
                        logging.info("RAID UPDATED FORT_SIGHTINGS. Gym:" + str(gym_id) + " Team:" + str(gym_team) + "\n")
                    except:
                        database.rollback()
                        print("RAID UPDATE TO FORT_SIGHTINGS FAILED.\n")
                        logging.info("RAID UPDATE TO FORT_SIGHTINGS FAILED.\n")

                else:
                    fort_sightings_insert = "INSERT INTO fort_sightings(fort_id, team, last_modified) VALUES (" + str(gym_id) + ", " + str(gym_team) + ", " + str(calendar.timegm(current_time.timetuple())) + ");"
                            
                    try:
                        database.ping(True)
                        cursor.execute(fort_sightings_insert)
                        database.commit()
                        print("RAID INSERTED INTO FORT_SIGHTINGS. Gym:" + str(gym_id) + " Team:" + str(gym_team) + "\n")
                        logging.info("RAID INSERTED INTO FORT_SIGHTINGS. Gym:" + str(gym_id) + " Team:" + str(gym_team) + "\n")
                    except:
                        database.rollback()
                        print("RAID INSERT INTO FORT_SIGHTINGS FAILED.\n")
                        logging.info("RAID INSERT INTO FORT_SIGHTINGS FAILED.\n")

                return 'Raid type was sent and processed.\n', 200
        except:
            database.rollback()
            print("EXISTING RAID QUERY FAILED")
    else:
        print("RAID Gym ID Not Found.")
        logging.info("RAID Gym ID Not Found.")
                
        add_gym_query = "INSERT INTO forts(external_id, lat, lon, name, url) VALUES('" + str(gym_id) + "', " +  str(gym_lat) + ", " + str(gym_lon) + ", '" + str(gym_name) + "', '" + str(gym_url) + "');"
                
        if ( DEBUG ):
            print("RAID DEBUG: add_gym_query = " + str(add_gym_query))
            logging.debug("RAID DEBUG: add_gym_query = " + str(add_gym_query))
                
        try:
            database.ping(True)
            cursor.execute(add_gym_query)
            database.commit()
            print("RAID GYM ADDED. Gym:" + str(gym_id) + " Lat:" + str(gym_lat) + " Lon:" + str(gym_lon) + " Name:" + str(gym_name) + " URL:" + str(gym_url) + "\n")
            logging.info("RAID GYM ADDED. Gym:" + str(gym_id) + " Lat:" + str(gym_lat) + " Lon:" + str(gym_lon) + " Name:" + str(gym_name) + " URL:" + str(gym_url) + "\n")
            return 'Unknown gym. Insert successful.\n', 200
        except:
            database.rollback()
            print("RAID GYM INSERT FAILED.\n")
            logging.info("RAID GYM INSERT FAILED.\n")
            return 'Unknown gym. Insert failed.\n', 500

def process_pokemon(data):
    current_time = datetime.datetime.utcnow()

    #Load payload data into variables
    if 'gender' in data[0]['message']:
        gender = data[0]['message']['gender']
    else:
        gender = 0
    if 'form' in data[0]['message']:
        form = data[0]['message']['form']
    else:
        form = 0
    if 'boosted_weather' in data[0]['message']:
        boosted_weather = data[0]['message']['boosted_weather']
    else:
        boosted_weather = 0
    disappear_time = data[0]['message']['disappear_time']
    encounter_id = data[0]['message']['encounter_id']
    last_modified_time = data[0]['message']['last_modified_time']
    latitude = data[0]['message']['latitude']
    longitude = data[0]['message']['longitude']
    pokemon_id = data[0]['message']['pokemon_id']
    spawnpoint_id = data[0]['message']['spawnpoint_id']
    time_until_hidden_ms = data[0]['message']['time_until_hidden_ms']

    pokemon_insert_query = "INSERT INTO sightings(pokemon_id, gender, form, weather_boosted_condition, spawn_id, expire_timestamp, encounter_id, lat, lon) VALUES(" + str(pokemon_id) + ", " + str(gender) + ", " + str(form) + ", " + str(boosted_weather) + ", " + str(spawnpoint_id) + ", " + str(disappear_time) + ", " + str(encounter_id) + ", " + str(latitude) + ", " + str(longitude) + ");"

    if ( DEBUG ):
        print("POKEMON DEBUG: " + str(pokemon_insert_query))
        logging.debug("POKEMON DEBUG: " + str(pokemon_insert_query))
    try:
        database.ping(True)
        cursor.execute(pokemon_insert_query)
        database.commit()
        print("POKEMON ADDED. Pokemon ID:" + str(pokemon_id) + " Lat:" + str(latitude) + " Lon:" + str(longitude) + "\n")
        logging.info("POKEMON ADDED. Pokemon ID:" + str(pokemon_id) + " Lat:" + str(latitude) + " Lon:" + str(longitude) + "\n")
    except:
        database.rollback()
        print("POKEMON INSERT FAILED.\n")
        logging.info("POKEMON INSERT FAILED.\n")
      
    return 'Pokemon type was sent and processed.\n', 200

def process_gym(data):
    current_epoch_time = time.time()
    
    if ( DEBUG ):
        print("GYM DEBUG: LOADING DATA")
        logging.debug("GYM DEBUG: LOADING DATA")
    
    #Load payload data into variables
    raid_active_until = data[0]['message']['raid_active_until']
    external_id = data[0]['message']['gym_id']
    gym_name = data[0]['message']['name']
    gym_description = data[0]['message']['description']
    gym_url = data[0]['message']['url']
    gym_team = data[0]['message']['team_id']
    slots_available = data[0]['message']['slots_available']
    guard_pokemon_id = data[0]['message']['guard_pokemon_id']
    lowest_pokemon_motivation = data[0]['message']['lowest_pokemon_motivation']
    total_cp = data[0]['message']['total_cp']
    enabled = data[0]['message']['enabled']
    gym_lat = data[0]['message']['latitude']
    gym_lon = data[0]['message']['longitude']
    last_modified = data[0]['message']['last_modified']

    get_gym_id_query = "SELECT id FROM forts WHERE external_id='" + str(external_id) + "';"

    insert_gym_query = "INSERT INTO forts(external_id, lat, lon, name, url) VALUES ('" + str(external_id) + "','" + str(gym_lat) + "','" + str(gym_lon) + "','" + str(gym_name) + "','" + str(gym_url) + "');"

    if ( DEBUG ):
        print("GYM DEBUG: get_gym_id_query = " + str(get_gym_id_query))
        logging.debug("GYM DEBUG: get_gym_id_query = " + str(get_gym_id_query))
        print("GYM DEBUG: insert_gym_query = " + str(insert_gym_query))
        logging.debug("GYM DEBUG: insert_gym_query = " + str(insert_gym_query))

    try:
        database.ping(True)
        cursor.execute(get_gym_id_query)
        fort_data = cursor.fetchall()
        fort_count = cursor.rowcount

        database.commit()
    except:
        database.rollback()

    gym_id_1 = fort_data[0][0]

    if ( DEBUG ):
        print("GYM DEBUG: gym_id_1 = " + str(gym_id_1))
        print("GYM DEBUG: fort_count = " + str(fort_count))

    if not ( fort_count ):
        print("Fort ID was not found. Attempting to insert new gym.")

        try:
            database.ping(True)
            cursor.execute(insert_gym_query)
            database.commit()
        
            print("New gym added. External ID: " + str(external_id) + " Lat: " + str(gym_lat) + " Lon: " + str(gym_lon) + " Name: " + str(gym_name) + " URL: " + str(gym_url) + "\n")
            logging.info("GYM ADDED. External ID: " + str(external_id) + " Lat: " + str(gym_lat) + " Lon: " + str(gym_lon) + " Name: " + str(gym_name) + " URL: " + str(gym_url) + "\n")
        except:
            database.rollback()

    try:
        database.ping(True)
        cursor.execute(get_gym_id_query)
        fort_data = cursor.fetchall()

        database.commit()
    except:
        database.rollback()

    gym_id_2 = fort_data[0][0]

    if ( DEBUG ):
        print("GYM DEBUG: gym_id_2 = " + str(gym_id_2))

    insert_fort_sighting_query = "INSERT INTO fort_sightings(fort_id, last_modified, team, guard_pokemon_id, slots_available, updated) VALUES ('" + str(gym_id_2) + "','" + str(last_modified) + "','" + str(gym_team) + "','" + str(guard_pokemon_id) +  "','" + str(slots_available) + "','" + str(current_epoch_time) + "');"

    update_fort_sighting_query = "UPDATE fort_sightings SET last_modified='" + str(last_modified) + "', team='" + str(gym_team) + "', guard_pokemon_id='" + str(guard_pokemon_id) +  "', slots_available='" + str(slots_available) + "', updated='" + str(current_epoch_time) + "' WHERE fort_id='" + str(gym_id_2) + "';"

    fort_sightings_query = "SELECT id, fort_id FROM fort_sightings WHERE fort_id='" + str(gym_id_2) + "';"

    try:
        database.ping(True)
        cursor.execute(fort_sightings_query)
        fs_count = cursor.rowcount
        
        database.commit()
    except:
        database.rollback()

    if ( DEBUG ):
        print("GYM DEBUG: fs_count = " + str(fs_count))
        print("GYM DEBUG: insert_fort_sighting_query = " + str(insert_fort_sighting_query))
        print("GYM DEBUG: update_fort_sighting_query = " + str(update_fort_sighting_query))

    if ( fs_count ):
        try:
            database.ping(True)
            cursor.execute(update_fort_sighting_query)
            database.commit()
            
            print("GYM SIGHTING UPDATED. Gym: " + str(gym_id_2) + " Last Modified: " + str(last_modified) + " Gym Team: " + str(gym_team) + " Guarding Pokemon: " + str(guard_pokemon_id) + " Slots Available: " + str(slots_available) + "\n")
            logging.info("GYM SIGHTING UPDATED. Gym: " + str(gym_id_2) + " Last Modified: " + str(last_modified) + " Gym Team: " + str(gym_team) + " Guarding Pokemon: " + str(guard_pokemon_id) + " Slots Available: " + str(slots_available) + "\n")
        
        except:
            database.rollback()
    else:
        try:
            database.ping(True)
            cursor.execute(insert_fort_sighting_query)
            database.commit()
            
            print("GYM SIGHTING INSERTED. Gym: " + str(gym_id_2) + " Last Modified: " + str(last_modified) + " Gym Team: " + str(gym_team) + " Guarding Pokemon: " + str(guard_pokemon_id) + " Slots Available: " + str(slots_available) + "\n")
            logging.info("GYM SIGHTING INSERTED. Gym: " + str(gym_id_2) + " Last Modified: " + str(last_modified) + " Gym Team: " + str(gym_team) + " Guarding Pokemon: " + str(guard_pokemon_id) + " Slots Available: " + str(slots_available) + "\n")

        except:
            database.rollback()
            print("GYM INSERT FAILED. Gym:" + str(gym_id_2) + "\n")
            logging.info("GYM INSERT FAILED. Gym: " + str(gym_id_2) + "\n")
    
    return 'Gym type was sent and processed.\n', 200

def process_quest(data):
    if ( DEBUG ):
        print("QUEST DEBUG: LOADING DATA")
        logging.debug("QUEST DEBUG: LOADING DATA")
    
    #Load payload data into variables
    external_id = data[0]['message']['pokestop_id']
    latitude = data[0]['message']['latitude']
    longitude = data[0]['message']['longitude']
    quest_type = data[0]['message']['quest_type']
    quest_type_raw = data[0]['message']['quest_type_raw']
    item_type = data[0]['message']['item_type']
    item_amount = data[0]['message']['item_amount']
    item_id = data[0]['message']['item_id']
    pokemon_id = data[0]['message']['pokemon_id']
    name = data[0]['message']['name']
    url = data[0]['message']['url']
    timestamp = data[0]['message']['timestamp']
    quest_reward_type = data[0]['message']['quest_reward_type']
    quest_reward_type_raw = data[0]['message']['quest_reward_type_raw']
    quest_target = data[0]['message']['quest_target']
    quest_task = data[0]['message']['quest_task']
    quest_condition = data[0]['message']['quest_condition']

    if len(quest_condition) != 0:
        if 'type' in quest_condition[0]:
            qc_type = quest_condition[0]['type']
        else:
            qc_type = 0

        if 'with_pokemon_type' in quest_condition[0]:
            qc_type_with_pokemon_type = quest_condition[0]['with_pokemon_type']

            if len(qc_type_with_pokemon_type) != 0:
                qc_type_with_pokemon_type_pokemon_type = qc_type_with_pokemon_type['pokemon_type']
            else:
                qc_type_with_pokemon_type_pokemon_type = 0
        else:
            qc_type_with_pokemon_type = 0
            
    else:
        qc_type = 0
        qc_type_with_pokemon_type = 0
        qc_type_with_pokemon_type_pokemon_type = 0

    if ( DEBUG ):
        print("QUEST DEBUG: quest_condition = " + str(quest_condition))
        print("QUEST DEBUG: qc_type = " + str(qc_type))
        print("QUEST DEBUG: qc_type_with_pokemon_type = " + str(qc_type_with_pokemon_type))
        print("QUEST DEBUG: qc_type_with_pokemon_type_pokemon_type = " + str(qc_type_with_pokemon_type_pokemon_type))
        for ids in range(len(qc_type_with_pokemon_type['pokemon_type'])):
            print("QUEST DEBUG: qc_type_with_pokemon_type_pokemon_type[" + str(ids) + "]: " + str(qc_type_with_pokemon_type_pokemon_type[ids]))

    get_pokestop_id_query = "SELECT id FROM pokestops WHERE external_id='" + str(external_id) + "';"

    insert_pokestop_query = "INSERT INTO pokestops(external_id, lat, lon, name, url, updated) VALUES ('" + str(external_id) + "', '" + str(latitude) + "', '" + str(longitude) + "', '" + str(name) + "', '" + str(url) + "', '" + str(timestamp) + "');"

    if ( DEBUG ):
        print("QUEST DEBUG: get_pokestop_id_query = " + str(get_pokestop_id_query))
        print("QUEST DEBUG: insert_pokestop_query = " + str(insert_pokestop_query))

    #Check if pokestop exists, if not insert new one
    try:
        database.ping(True)
        cursor.execute(get_pokestop_id_query)
        ps_count = cursor.rowcount

        database.commit()
    except:
        database.rollback()

    if not ( ps_count ):
        print ("POKESTOP NOT FOUND. Inserting new pokestop: " + str(name) + " Lat: " + str(latitude) + " Lon: " + str(longitude))
        
        try:
            database.ping(True)
            cursor.execute(insert_pokestop_query)
            database.commit()
        except:
            database.rollback()
 
    #Get pokestop_id now
    try:
        database.ping(True)
        cursor.execute(get_pokestop_id_query)
        ps_data = cursor.fetchall()
    
        database.commit()
    except:
        database.rollback()

    pokestop_id = ps_data[0][0];

    insert_quest_query = "INSERT INTO quests(pokestop_id, quest_type, quest_type_raw, item_type, item_amount, item_id, pokemon_id, quest_reward_type, quest_reward_type_raw, quest_target, quest_task, qc_type, qc_type_with_pokemon_pokemon_type) VALUES ('" + str(pokestop_id) + "', '" + str(quest_type) + "', '" + str(quest_type_raw) + "', '" + str(item_type) + "', '" + str(item_amount) + "', '" + str(item_id) + "', '" + str(pokemon_id) + "', '" + str(quest_reward_type) + "', '" + str(quest_reward_type_raw) + "', '" + str(quest_target) + "', '" + str(quest_task) + "', '" + str(qc_type) + "', '" + str(qc_type_with_pokemon_type_pokemon_type) + "');"

    update_quest_query = "UPDATE quests SET quest_type='" + str(quest_type) + "', quest_type_raw='" + str(quest_type_raw) + "', item_type='" + str(item_type) + "', item_amount='" + str(item_amount) + "', item_id='" + str(item_id) + "', pokemon_id='" + str(pokemon_id) + "', quest_reward_type='" + str(quest_reward_type) + "', quest_reward_type_raw='" + str(quest_reward_type_raw) + "', quest_target='" + str(quest_target) + "', quest_task='" + str(quest_task) + "', qc_type='" + str(qc_type) + "', qc_type_with_pokemon_pokemon_type='" + str(qc_type_with_pokemon_type_pokemon_type) + "' WHERE pokestop_id='" + str(pokestop_id) + "';"

    quests_query = "SELECT id, pokestop_id FROM quests WHERE pokestop_id='" + str(pokestop_id) + "';"

    if ( DEBUG ):
        print("QUEST DEBUG: insert_quest_query = " + str(insert_quest_query))
        print("QUEST DEBUG: update_quest_query = " + str(update_quest_query))
        print("QUEST DEBUG: quests_query = " + str(quests_query))

    #Check if quest entry exists
    try:
        database.ping(True)
        cursor.execute(quests_query)
        ps_count = cursor.rowcount
        database.commit()
    except:
        database.rollback()

    #If quest entry exists, update the entry, otherwise, insert new quest
    if ( ps_count ):
        try:
            database.ping(True)
            cursor.execute(update_quest_query)
            database.commit()

            print("QUEST UPDATED. Quest: " + str(quest_type) + ". Pokestop ID: " + str(pokestop_id) + "\n")
            logging.info("QUEST UPDATED. Quest: " + str(quest_type) + ". Pokestop ID: " + str(pokestop_id) + "\n")
        except:
            database.rollback()
            
            print("QUEST UPDATE FAILED. Quest: " + str(quest_type) + " Pokestop ID: " + str(pokestop_id) + "\n")
            logging.info("QUEST UPDATE FAILED. Quest: " + str(quest_type) + " Pokestop ID: " + str(pokestop_id) + "\n")
    else:
        try:
            database.ping(True)
            cursor.execute(insert_quest_query)
            database.commit()

            print("QUEST INSERTED. Quest: " + str(quest_type) + ". Pokestop ID: " + str(pokestop_id) + "\n")
            logging.info("QUEST INSERTED. Quest: " + str(quest_type) + ". Pokestop ID: " + str(pokestop_id) + "\n")
        except:
            database.rollback()
            
            print("QUEST INSERT FAILED. Quest: " + str(quest_type) + " Pokestop ID: " + str(pokestop_id) + "\n")
            logging.info("QUEST INSERT FAILED. Quest: " + str(quest_type) + " Pokestop ID: " + str(pokestop_id) + "\n")


    return 'Quest type was sent and processed.\n', 200

@app.route('/submit', methods=['POST'])
def webhook():
    if request.method == 'POST':
        utc_now = pytz.utc.localize(datetime.datetime.utcnow())
        pst_now = utc_now.astimezone(pytz.timezone("America/Los_Angeles"))
        data = json.loads(request.data)
        print("MESSAGE RECEIVED AT " + str(pst_now) + ": " + str(request.json))
        logging.info("MESSAGE RECEIVED AT " + str(pst_now) + ": " + str(request.json))
        
        if ( DEBUG ):
            print("MAIN DEBUG: type=" + str(data[0]['type']))
            logging.debug("MAIN DEBUG: type=" + str(data[0]['type']))
        
        message_type = data[0]['type']
        
        if message_type == "raid":
            result = proces_raid(data)
            return result

        if message_type == "pokemon":
            result = process_pokemon(data)
            return result

        if message_type == "gym":
            result = process_gym(data)
            return result

        if message_type == "quest":
            result = process_quest(data)
            return result
    else:
        abort(400)


if __name__ == '__main__':
    app.run(host=HOST,port=PORT)
