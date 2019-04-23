import json
import MySQLdb
import datetime
import pytz
import calendar
import time
import logging
from flask import Flask, request, abort
from config import HOST, PORT, DB_HOST, DB_USER, DB_PASSWORD, DATABASE, MAIN_DEBUG, SHOW_PAYLOAD, RAID_DEBUG, GYM_DEBUG, POKEMON_DEBUG, QUEST_DEBUG, WHITELIST

logging.basicConfig(filename='debug_webhook.log',level=logging.DEBUG)

app = Flask(__name__)

database = MySQLdb.connect(DB_HOST, DB_USER, DB_PASSWORD, DATABASE)

cursor = database.cursor()

print('Webhook Started at ' + str(time.strftime('%I:%M %p on %m.%d.%y',  time.localtime(calendar.timegm(datetime.datetime.utcnow().timetuple())))))
logging.info('Webhook Started at ' + str(time.strftime('%I:%M %p on %m.%d.%y',  time.localtime(calendar.timegm(datetime.datetime.utcnow().timetuple())))))

def proces_raid(data):
    current_time = datetime.datetime.utcnow()

    #Load payload data into variables
    gym_name = data['name']
    gym_id = data['gym_id']
    gym_lat = data['latitude']
    gym_lon = data['longitude']
    if 'url' not in data:
        gym_url = ''
    else:
        gym_url = data['url']
        gym_url_raw = str(gym_url)
        if "http:" in gym_url:
            gym_url = gym_url.replace("http:","https:")
    gym_team = data['team_id']
    raid_level = data['level']
    raid_begin = data['start']
    raid_end = data['end']


    #Check if message has pokemon_id sent. If not, its an egg
    if 'pokemon_id' in data:
        boss_id = data['pokemon_id']
        if boss_id != 0:
            boss_cp = data['cp']
            boss_move_1 = data['move_1']
            boss_move_2 = data['move_2']
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
    database.commit()

    if ( gym_id_count ):
        gym_id = gym_ids[0][0]
        insert_query = "INSERT INTO raids(id, external_id, fort_id, level, pokemon_id, move_1, move_2, time_spawn, time_battle, time_end, cp) VALUES (null, null, " + str(gym_id) + ", " + str(raid_level) + ", " + str(boss_id) + ", " + str(boss_move_1) + ", " + str(boss_move_2) + ", null, " + str(raid_begin) + ", " + str(raid_end) + ", " + str(boss_cp) + ");"
                
        update_query = "UPDATE raids SET pokemon_id='" + str(boss_id) + "', move_1='" + str(boss_move_1) + "', move_2='" + str(boss_move_2) + "', cp='" + str(boss_cp) + "' WHERE fort_id='" + str(gym_id)+ "' AND time_end>'" + str(calendar.timegm(current_time.timetuple())) + "';"
                
        existing_raid_check_query = "SELECT id, fort_id, pokemon_id, time_end FROM raids WHERE fort_id='" + str(gym_id) + "' AND time_end>'" + str(calendar.timegm(current_time.timetuple())) + "';"
        
        fort_sightings_query = "SELECT id, fort_id, team FROM fort_sightings WHERE fort_id='" + str(gym_id) + "';"
                
        if ( RAID_DEBUG ):
            print("RAID DEBUG: insert_query = " + insert_query)
            print("RAID DEBUG: update_query = " + update_query)
            print("RAID DEBUG: existing_raid_check_query = " + existing_raid_check_query)
            print("RAID DEBUG: fort_sightings_query = " + fort_sightings_query)
            logging.debug(insert_query)
            logging.debug(update_query)
            logging.debug(existing_raid_check_query)
            logging.debug(fort_sightings_query)
                
        try:
            database.ping(True)
            cursor.execute(existing_raid_check_query)
            raid_data = cursor.fetchall()
            raid_count = cursor.rowcount
            database.commit()

            if ( RAID_DEBUG ):
                print("RAID DEBUG: raid_count = " + str(raid_count))
                logging.debug("RAID DEBUG: raid_count = " + str(raid_count))
                print("RAID DEBUG: boss_id = " + str(boss_id))
                logging.debug("RAID DEBUG: boss_id = " + str(boss_id))
        
            #If raid entry already exists and current boss_id is provided in message, update entry
            if raid_count > 0 and boss_id != "0":
                if ( RAID_DEBUG ):
                    print("RAID DEBUG: raid_data[0][2] = " + str(raid_data[0][2]))
                    logging.debug("RAID DEBUG: raid_data[0][2] = " + str(raid_data[0][2]))
              
                #If exisiting pokemon_id in table is an egg, update with new boss_id
                if ( raid_data[0][2] == 0 ):
                    try:
                        database.ping(True)
                        cursor.execute(update_query)
                        database.commit()
                        
                        if ( RAID_DEBUG ):
                            print("RAID UPDATED. Old Boss:" + str(raid_data[0][2]) + " New Boss:" + str(boss_id) + " Move 1: " + str(boss_move_1) + " Move 2: " + str(boss_move_2) + " CP: " + str(boss_cp) + "\n")
                            logging.info("RAID UPDATED. Old Boss:" + str(raid_data[0][2]) + " New Boss:" + str(boss_id) + " Move 1: " + str(boss_move_1) + " Move 2: " + str(boss_move_2) + " CP: " + str(boss_cp) + "\n")
                    except:
                        database.rollback()
                        
                        if ( RAID_DEBUG ):
                            print("RAID UPDATE FAILED.\n")
                            logging.info("RAID UPDATE FAILED.\n")
                else:
                    if ( RAID_DEBUG ):
                        print("DUPLICATE RAID. IGNORED.\n")
                        logging.info("DUPLICATE RAID. IGNORED.\n")
                    pass
                return 'Duplicate webhook message was ignored.\n', 200
            else:
                if raid_count == 0: # This is a new egg that popped so go ahead and insert it
                    try:
                        database.ping(True)
                        cursor.execute(insert_query)
                        database.commit()
                        
                        if ( RAID_DEBUG ):
                            print("RAID INSERT EXECUTED. Gym:" + str(gym_id) + " Raid:" + str(raid_level) + " Boss:" + str(boss_id))
                            logging.info("RAID INSERT EXECUTED. Gym:" + str(gym_id) + " Raid:" + str(raid_level) + " Boss:" + str(boss_id))
                    except:
                        database.rollback()
                        
                        if ( RAID_DEBUG ):
                            print("RAID INSERT FAILED.\n")
                            logging.info("RAID INSERT FAILED.\n")
                else:
                    if ( RAID_DEBUG ):
                        print("DUPLICATE RAID. IGNORED.\n")
                        logging.info("DUPLICATE RAID. IGNORED.\n")
                    pass
                #Need to check if fort_id is in fort_sightings. If not, insert as new entry, otherwise update.
                database.ping(True)
                cursor.execute(fort_sightings_query)
                fs_data = cursor.fetchall()
                fs_count = cursor.rowcount
                database.commit()

                if ( fs_count ):
                    fort_sightings_update = "UPDATE fort_sightings SET team='" + str(gym_team) + "' WHERE fort_id='" + str(gym_id) + "';"
                    
                    if fs_data[0][2] != gym_team: #Check if gym (team) changed
                        try:
                            database.ping(True)
                            cursor.execute(fort_sightings_update)
                            database.commit()
                            
                            if ( RAID_DEBUG ):
                                print("RAID UPDATED FORT_SIGHTINGS. Gym:" + str(gym_id) + " Team:" + str(gym_team) + "\n")
                                logging.info("RAID UPDATED FORT_SIGHTINGS. Gym:" + str(gym_id) + " Team:" + str(gym_team) + "\n")
                        except:
                            database.rollback()
                            
                            if ( RAID_DEBUG ):
                                print("RAID UPDATE TO FORT_SIGHTINGS FAILED.\n")
                                logging.info("RAID UPDATE TO FORT_SIGHTINGS FAILED.\n")
                    else:
                        if ( RAID_DEBUG ):
                            print("NO GYM CHANGE. IGNORING.\n")
                            logging.info("NO GYM CHANGE. IGNORING.\n")
                        pass

                else:
                    fort_sightings_insert = "INSERT INTO fort_sightings(fort_id, team, last_modified) VALUES (" + str(gym_id) + ", " + str(gym_team) + ", " + str(calendar.timegm(current_time.timetuple())) + ");"
                            
                    try:
                        database.ping(True)
                        cursor.execute(fort_sightings_insert)
                        database.commit()
                        
                        if ( RAID_DEBUG ):
                            print("RAID INSERTED INTO FORT_SIGHTINGS. Gym:" + str(gym_id) + " Team:" + str(gym_team) + "\n")
                            logging.info("RAID INSERTED INTO FORT_SIGHTINGS. Gym:" + str(gym_id) + " Team:" + str(gym_team) + "\n")
                    except:
                        database.rollback()
                        
                        if ( RAID_DEBUG ):
                            print("RAID INSERT INTO FORT_SIGHTINGS FAILED.\n")
                            logging.info("RAID INSERT INTO FORT_SIGHTINGS FAILED.\n")

                return 'Raid type was sent and processed.\n', 200
        except:
            database.rollback()
            
            if ( RAID_DEBUG ):
                print("EXISTING RAID QUERY FAILED.\n")
    else:
        if ( RAID_DEBUG ):
            print("RAID Gym ID Not Found.\n")
            logging.info("RAID Gym ID Not Found.\n")
                
        add_gym_query = "INSERT INTO forts(external_id, lat, lon, name, url) VALUES('" + str(gym_id) + "', " +  str(gym_lat) + ", " + str(gym_lon) + ", '" + str(gym_name) + "', '" + str(gym_url) + "');"
                
        if ( RAID_DEBUG ):
            print("RAID DEBUG: add_gym_query = " + str(add_gym_query))
            logging.debug("RAID DEBUG: add_gym_query = " + str(add_gym_query))
                
        try:
            database.ping(True)
            cursor.execute(add_gym_query)
            database.commit()
            
            if ( RAID_DEBUG ):
                print("RAID GYM ADDED. Gym:" + str(gym_id) + " Lat:" + str(gym_lat) + " Lon:" + str(gym_lon) + " Name:" + str(gym_name) + " URL:" + str(gym_url) + "\n")
                logging.info("RAID GYM ADDED. Gym:" + str(gym_id) + " Lat:" + str(gym_lat) + " Lon:" + str(gym_lon) + " Name:" + str(gym_name) + " URL:" + str(gym_url) + "\n")
            return 'Unknown gym. Insert successful.\n', 200
        except:
            database.rollback()
            
            if ( RAID_DEBUG ):
                print("RAID GYM INSERT FAILED.\n")
                logging.info("RAID GYM INSERT FAILED.\n")
            return 'Unknown gym. Insert failed.\n', 500

def process_pokemon(data):
    current_time = datetime.datetime.utcnow()

    #Load payload data into variables
    if 'gender' in data:
        gender = data['gender']
    else:
        gender = 0
    if 'form' in data:
        form = data['form']
    else:
        form = 0
    if 'boosted_weather' in data:
        boosted_weather = data['boosted_weather']
    else:
        boosted_weather = 0
    if 'individual_attack' in data:
        atk_iv = data['individual_attack']
    else:
        atk_iv = None
    if 'individual_defense' in data:
        def_iv = data['individual_defense']
    else:
        def_iv = None
    if 'individual_stamina' in data:
        sta_iv = data['individual_stamina']
    else:
        sta_iv = None
    if 'cp' in data:
        cp = data['cp']
    else:
        cp = None
    if 'pokemon_level' in data:
        level = data['pokemon_level']
    else:
        level = None
    if 'weight' in data:
        weight = data['weight']
    else:
        weight = None
    if 'move_1' in data:
        move_1 = data['move_1']
    else:
        move_1 = None
    if 'move_2' in data:
        move_2 = data['move_2']
    else:
        move_2 = None
    disappear_time = data['disappear_time']
    encounter_id = data['encounter_id']
    #last_modified_time = data['last_modified_time']
    latitude = data['latitude']
    longitude = data['longitude']
    pokemon_id = data['pokemon_id']
    spawnpoint_id = data['spawnpoint_id']
    #time_until_hidden_ms = data['time_until_hidden_ms']

    iv_pokemon_insert_query = "INSERT INTO sightings(pokemon_id, gender, form, weather_boosted_condition, spawn_id, expire_timestamp, encounter_id, lat, lon, atk_iv, def_iv, sta_iv, cp, level, weight, move_1, move_2) VALUES(" + str(pokemon_id) + ", " + str(gender) + ", " + str(form) + ", " + str(boosted_weather) + ", " + str(spawnpoint_id) + ", " + str(disappear_time) + ", " + str(encounter_id) + ", " + str(latitude) + ", " + str(longitude) + ", " + str(atk_iv) + ", " + str(def_iv) + ", " + str(sta_iv) + ", " + str(cp) + ", " + str(level) + ", " + str(weight) + ", " + str(move_1) + ", " + str(move_2) + ");"

    update_pokemon_query = "UPDATE sightings SET atk_iv='" + str(atk_iv) + "', def_iv='" + str(def_iv) + "', sta_iv='" + str(sta_iv) + "', cp='" + str(cp) + "', level='" + str(level) + "', weight='" + str(weight) + "', move_1='" + str(move_1) + "', move_2='" + str(move_2) + "' WHERE encounter_id='" + str(encounter_id) + "';"

    encounter_id_query = "SELECT encounter_id, atk_iv, def_iv, sta_iv FROM sightings WHERE encounter_id='" + str(encounter_id) + "';"

    if ( POKEMON_DEBUG ):
        print("POKEMON DEBUG: " + str(iv_pokemon_insert_query))
        logging.debug("POKEMON DEBUG: " + str(iv_pokemon_insert_query))
        print("POKEMON DEBUG: " + str(encounter_id_query))
        logging.debug("POKEMON DEBUG: " + str(encounter_id_query))

    #Check to see if encounter_id already exists in sightings
    try:
        database.ping(True)
        cursor.execute(encounter_id_query)
        encounter_id_count = cursor.rowcount
        pokemon_data = cursor.fetchall()
        database.commit()
    except:
        database.rollback()

    #New pokemon, go ahead and insert new
    if not ( encounter_id_count ):
        try:
            database.ping(True)
            cursor.execute(iv_pokemon_insert_query)
            database.commit()
            
            if ( POKEMON_DEBUG ):
                print("POKEMON ADDED. Pokemon ID:" + str(pokemon_id) + " Lat:" + str(latitude) + " Lon:" + str(longitude) + "\n")
                logging.info("POKEMON ADDED. Pokemon ID:" + str(pokemon_id) + " Lat:" + str(latitude) + " Lon:" + str(longitude) + "\n")
        except:
            database.rollback()
            
            if ( POKEMON_DEBUG ):
                print("POKEMON INSERT FAILED.\n")
                logging.debug("POKEMON INSERT FAILED.\n")
    else: #Existing pokemon, check to see if its an IV update
        stored_atk_iv = pokemon_data[0][1]
        
        #Check to see if stored IV is different from what is being sent this time
        if str(stored_atk_iv) != str(atk_iv):
            try:
                database.ping(True)
                cursor.execute(update_pokemon_query)
                database.commit()
                if ( POKEMON_DEBUG ):
                    print("POKEMON UPDATED: encounter_id: " + str(encounter_id) + " atk_iv: " + str(atk_iv) + " def_iv: " + str(def_iv) + " sta_iv: " + str(sta_iv) + " cp: " + str(cp) + "\n")
                    logging.info("POKEMON UPDATED: encounter_id: " + str(encounter_id) + " atk_iv: " + str(atk_iv) + " def_iv: " + str(def_iv) + " sta_iv: " + str(sta_iv) + " cp: " + str(cp) + "\n")
            except:
                database.rollback()
                if ( POKEMON_DEBUG ):
                    print("POKEMON UPDATE FAILED.\n")
                    logging.info("POKEMON UPDATE FAILED.\n")
        else:
            if ( POKEMON_DEBUG ):
                print("DUPLICATE POKEMON MESSAGE. IGNORED.\n")
                logging.info("DUPLICATE POKEMON MESSAGE. IGNORED.\n")
            pass

    return 'Pokemon type was sent and processed.\n', 200

def process_gym(data):
    current_epoch_time = time.time()
    
    if ( GYM_DEBUG ):
        print("GYM DEBUG: LOADING DATA")
        logging.debug("GYM DEBUG: LOADING DATA")
    
    #Load payload data into variables
    external_id = data['gym_id']
    gym_team = data['team_id']
    slots_available = data['slots_available']
    gym_lat = data['latitude']
    gym_lon = data['longitude']
    if 'name' in data:
        gym_name = data['name']
    else:
        gym_name = None
    if 'url' in data:
        gym_url = data['url']
        if "http:" in gym_url:
            gym_url = gym_url.replace("http:","https:")
    else:
        gym_url = None
    if 'guard_pokemon_id' in data:
        guard_pokemon_id = data['guard_pokemon_id']
    else:
        guard_pokemon_id = 0
    if 'last_modified' in data:
        last_modified = data['last_modified']
    else:
        last_modified = current_epoch_time;
    #lowest_pokemon_motivation = data['lowest_pokemon_motivation']
    #total_cp = data['total_cp']
    #enabled = data['enabled']
    #gym_description = data['description']
    #raid_active_until = data['raid_active_until']

    get_gym_id_query = "SELECT id, name, url FROM forts WHERE external_id='" + str(external_id) + "';"

    insert_gym_query = "INSERT INTO forts(external_id, lat, lon, name, url) VALUES ('" + str(external_id) + "','" + str(gym_lat) + "','" + str(gym_lon) + "','" + str(gym_name) + "','" + str(gym_url) + "');"

    if ( GYM_DEBUG ):
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



    if ( GYM_DEBUG ):
        print("GYM DEBUG: fort_count = " + str(fort_count))
        logging.debug("GYM DEBUG: fort_count = " + str(fort_count))
        
        if ( fort_count ):
            gym_id_1 = fort_data[0][0]
            print("GYM DEBUG: gym_id_1 = " + str(gym_id_1))
            logging.debug("GYM DEBUG: gym_id_1 = " + str(gym_id_1))

    if not ( fort_count ):
        print("Fort ID was not found. Attempting to insert new gym.")

        try:
            database.ping(True)
            cursor.execute(insert_gym_query)
            database.commit()
        
            if ( GYM_DEBUG ):
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
    gym_name_2 = fort_data[0][1]
    gym_url_2 = fort_data[0][2]

    if ( GYM_DEBUG ):
        print("GYM DEBUG: gym_id_2 = " + str(gym_id_2))
        logging.debug("GYM DEBUG: gym_id_2 = " + str(gym_id_2))

    insert_fort_sighting_query = "INSERT INTO fort_sightings(fort_id, last_modified, team, guard_pokemon_id, slots_available, updated) VALUES ('" + str(gym_id_2) + "','" + str(last_modified) + "','" + str(gym_team) + "','" + str(guard_pokemon_id) +  "','" + str(slots_available) + "','" + str(current_epoch_time) + "');"

    update_fort_sighting_query = "UPDATE fort_sightings SET last_modified='" + str(last_modified) + "', team='" + str(gym_team) + "', guard_pokemon_id='" + str(guard_pokemon_id) +  "', slots_available='" + str(slots_available) + "', updated='" + str(current_epoch_time) + "' WHERE fort_id='" + str(gym_id_2) + "';"

    fort_sightings_query = "SELECT id, fort_id FROM fort_sightings WHERE fort_id='" + str(gym_id_2) + "';"

    update_fort_name_query = "UPDATE forts SET name='" + str(gym_name) + "' WHERE external_id='" + str(external_id) + "';"

    update_fort_url_query = "UPDATE forts SET url='" + str(gym_url) + "' WHERE external_id='" + str(external_id) + "';"

    if gym_name_2 is None and gym_name is not None:
        try:
            database.ping(True)
            cursor.execute(update_fort_name_query)
            database.commit()
        
            if ( GYM_DEBUG ):
                print("GYM NAME UPDATED. NAME: " + str(gym_name))
                logging.debug("GYM NAME UPDATED. NAME: " + str(gym_name))
        except:
            database.rollback()
            
            if ( GYM_DEBUG ):
                print("FAILED TO UPDATE GYM NAME.")
                logging.debug("FAILED TO UPDATE GYM NAME.")

    if gym_url_2 is None and gym_url is not None:
        try:
            database.ping(True)
            cursor.execute(update_fort_url_query)
            database.commit()
            
            if ( GYM_DEBUG ):
                print("GYM URL UPDATED. URL: " + str(gym_url))
                logging.debug("GYM URL UPDATED. URL: " + str(gym_url))
        except:
            database.rollback()

            if ( GYM_DEBUG ):
                print("FAILED TO UPDATE GYM URL.")
                logging.debug("FAILED TO UPDATE GYM URL.")

    try:
        database.ping(True)
        cursor.execute(fort_sightings_query)
        fs_count = cursor.rowcount
        
        database.commit()
    except:
        database.rollback()

    if ( GYM_DEBUG ):
        print("GYM DEBUG: fs_count = " + str(fs_count))
        print("GYM DEBUG: insert_fort_sighting_query = " + str(insert_fort_sighting_query))
        print("GYM DEBUG: update_fort_sighting_query = " + str(update_fort_sighting_query))

    if ( fs_count ):
        try:
            database.ping(True)
            cursor.execute(update_fort_sighting_query)
            database.commit()
            
            if ( GYM_DEBUG ):
                print("GYM SIGHTING UPDATED. Gym: " + str(gym_id_2) + " Last Modified: " + str(last_modified) + " Gym Team: " + str(gym_team) + " Guarding Pokemon: " + str(guard_pokemon_id) + " Slots Available: " + str(slots_available) + "\n")
                logging.info("GYM SIGHTING UPDATED. Gym: " + str(gym_id_2) + " Last Modified: " + str(last_modified) + " Gym Team: " + str(gym_team) + " Guarding Pokemon: " + str(guard_pokemon_id) + " Slots Available: " + str(slots_available) + "\n")
        
        except:
            database.rollback()
    else:
        try:
            database.ping(True)
            cursor.execute(insert_fort_sighting_query)
            database.commit()
            
            if ( GYM_DEBUG ):
                print("GYM SIGHTING INSERTED. Gym: " + str(gym_id_2) + " Last Modified: " + str(last_modified) + " Gym Team: " + str(gym_team) + " Guarding Pokemon: " + str(guard_pokemon_id) + " Slots Available: " + str(slots_available) + "\n")
                logging.info("GYM SIGHTING INSERTED. Gym: " + str(gym_id_2) + " Last Modified: " + str(last_modified) + " Gym Team: " + str(gym_team) + " Guarding Pokemon: " + str(guard_pokemon_id) + " Slots Available: " + str(slots_available) + "\n")

        except:
            database.rollback()
            
            if ( GYM_DEBUG ):
                print("GYM INSERT FAILED. Gym:" + str(gym_id_2) + "\n")
                logging.info("GYM INSERT FAILED. Gym: " + str(gym_id_2) + "\n")
    
    return 'Gym type was sent and processed.\n', 200

def process_quest(data):
    if ( QUEST_DEBUG ):
        print("QUEST DEBUG: LOADING DATA")
        logging.debug("QUEST DEBUG: LOADING DATA")
    
    #Load payload data into variables
    external_id = data['pokestop_id']
    latitude = data['latitude']
    longitude = data['longitude']
    quest_type = data['quest_type']
    quest_type_raw = data['quest_type_raw']
    item_type = data['item_type']
    item_amount = data['item_amount']
    item_id = data['item_id']
    pokemon_id = data['pokemon_id']
    timestamp = data['timestamp']
    quest_reward_type = data['quest_reward_type']
    quest_reward_type_raw = data['quest_reward_type_raw']
    quest_target = data['quest_target']
    quest_task = data['quest_task']
    quest_condition = data['quest_condition']
    if 'name' in data:
        name = data['name']
    else:
        name = ''
    if 'url' in data:
        url = data['url']
        if "http:" in url:
            url = url.replace("http:","https:")
    else:
        url = ''

    if ( QUEST_DEBUG ):
        print("QUEST DEBUG: quest_condition = " + str(quest_condition))

    get_pokestop_id_query = "SELECT id, name, url FROM pokestops WHERE external_id='" + str(external_id) + "';"

    insert_pokestop_query = "INSERT INTO pokestops(external_id, lat, lon, name, url, updated) VALUES ('" + str(external_id) + "', '" + str(latitude) + "', '" + str(longitude) + "', '" + str(name) + "', '" + str(url) + "', '" + str(timestamp) + "');"

    if ( QUEST_DEBUG ):
        print("QUEST DEBUG: get_pokestop_id_query = " + str(get_pokestop_id_query))
        logging.debug("QUEST DEBUG: get_pokestop_id_query = " + str(get_pokestop_id_query))
        print("QUEST DEBUG: insert_pokestop_query = " + str(insert_pokestop_query))
        logging.debug("QUEST DEBUG: insert_pokestop_query = " + str(insert_pokestop_query))

    #Check if pokestop exists, if not insert new one
    try:
        database.ping(True)
        cursor.execute(get_pokestop_id_query)
        ps_count = cursor.rowcount

        database.commit()
    except:
        database.rollback()

    if not ( ps_count ): #If 0 records are returned, must be a new pokestop
        if ( QUEST_DEBUG ):
            print("POKESTOP NOT FOUND. Inserting new pokestop: " + str(name) + " Lat: " + str(latitude) + " Lon: " + str(longitude))
            logging.debug("POKESTOP NOT FOUND. Inserting new pokestop: " + str(name) + " Lat: " + str(latitude) + " Lon: " + str(longitude))
        
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

    pokestop_id = ps_data[0][0]
    pokestop_url = ps_data[0][2]

    update_pokestop_url = "UPDATE pokestops SET url='" + str(url) + "' WHERE id='" + str(pokestop_id) + "';"

    if ( QUEST_DEBUG ):
        print("QUEST DEBUG: update_pokestop_url = " + str(update_pokestop_url))
        logging.debug("QUEST DEBUG: update_pokestop_url = " + str(update_pokestop_url))

    if pokestop_url is None and url is not None:
        try:
            database.ping(True)
            cursor.execute(update_pokestop_url)
            database.commit()
        
            if ( QUEST_DEBUG ):
                print("POKESTOP URL UPDATED. URL: " + str(url))
                logging.debug("POKESTOP URL UPDATED. URL: " + str(url))
        except:
            database.rollback()
            if ( QUEST_DEBUG ):
                print("FAILED TO UPDATE POKESTOP URL. URL: " + str(url))
                logging.debug("FAILED TO UPDATE POKESTOP URL. URL: " + str(url))


    insert_quest_query = "INSERT INTO quests(pokestop_id, quest_type, quest_type_raw, item_type, item_amount, item_id, pokemon_id, quest_reward_type, quest_reward_type_raw, quest_target, quest_task, quest_condition, timestamp) VALUES ('" + str(pokestop_id) + "', '" + str(quest_type) + "', '" + str(quest_type_raw) + "', '" + str(item_type) + "', '" + str(item_amount) + "', '" + str(item_id) + "', '" + str(pokemon_id) + "', '" + str(quest_reward_type) + "', '" + str(quest_reward_type_raw) + "', '" + str(quest_target) + "', '" + str(quest_task) + "', \"" + str(quest_condition) + "\", '" + str(timestamp) + "');"

    update_quest_query = "UPDATE quests SET quest_type='" + str(quest_type) + "', quest_type_raw='" + str(quest_type_raw) + "', item_type='" + str(item_type) + "', item_amount='" + str(item_amount) + "', item_id='" + str(item_id) + "', pokemon_id='" + str(pokemon_id) + "', quest_reward_type='" + str(quest_reward_type) + "', quest_reward_type_raw='" + str(quest_reward_type_raw) + "', quest_target='" + str(quest_target) + "', quest_task='" + str(quest_task) + "', quest_condition=\"" + str(quest_condition) + "\", timestamp='" + str(timestamp) +  "' WHERE pokestop_id='" + str(pokestop_id) + "';"

    quests_query = "SELECT id, pokestop_id FROM quests WHERE pokestop_id='" + str(pokestop_id) + "';"

    if ( QUEST_DEBUG ):
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

            if ( QUEST_DEBUG ):
                print("QUEST UPDATED. Quest: " + str(quest_type) + ". Pokestop ID: " + str(pokestop_id) + "\n")
                logging.info("QUEST UPDATED. Quest: " + str(quest_type) + ". Pokestop ID: " + str(pokestop_id) + "\n")
        except:
            database.rollback()
            
            if ( QUEST_DEBUG ):
                print("QUEST UPDATE FAILED. Quest: " + str(quest_type) + " Pokestop ID: " + str(pokestop_id) + "\n")
                logging.info("QUEST UPDATE FAILED. Quest: " + str(quest_type) + " Pokestop ID: " + str(pokestop_id) + "\n")
    else:
        try:
            database.ping(True)
            cursor.execute(insert_quest_query)
            database.commit()

            if ( QUEST_DEBUG ):
                print("QUEST INSERTED. Quest: " + str(quest_type) + ". Pokestop ID: " + str(pokestop_id) + "\n")
                logging.info("QUEST INSERTED. Quest: " + str(quest_type) + ". Pokestop ID: " + str(pokestop_id) + "\n")
        except:
            database.rollback()
            
            if ( QUEST_DEBUG ):
                print("QUEST INSERT FAILED. Quest: " + str(quest_type) + " Pokestop ID: " + str(pokestop_id) + "\n")
                logging.info("QUEST INSERT FAILED. Quest: " + str(quest_type) + " Pokestop ID: " + str(pokestop_id) + "\n")


    return 'Quest type was sent and processed.\n', 200

@app.route('/submit', methods=['POST'])
def webhook():
    if request.method == 'POST':
        utc_now = pytz.utc.localize(datetime.datetime.utcnow())
        pst_now = utc_now.astimezone(pytz.timezone("America/Los_Angeles"))
        data = json.loads(request.data)
        payload = str(request.json)
        utf_payload = payload.encode()

        #ip_address = request.remote_addr
        ip_address_raw = request.environ.get('HTTP_X_FORWARDED_FOR')

        if ip_address_raw is None:
            ip_address = "Local Host"
        else:
            ip_address_split = ip_address_raw.split(',')
            ip_address = ip_address_split[0]

        if ( MAIN_DEBUG ):
            if ( SHOW_PAYLOAD ):
                print("MESSAGE RECEIVED FROM " + str(ip_address) + " AT " + str(pst_now) + ": " + str(utf_payload))
                logging.info("MESSAGE RECEIVED FROM " + str(ip_address) + " AT " + str(pst_now) + ": " + str(utf_payload))
            else:
                print("MESSAGE RECEIVED FROM " + str(ip_address) + " AT " + str(pst_now))
                logging.info("MESSAGE RECEIVED FROM " + str(ip_address) + " AT " + str(pst_now))
            print("NUMBER OF MESSAGES TO PROCESS (RECEIVED): " + str(len(data)))
            logging.info("NUMBER OF MESSAGES TO PROCESS (RECEIVED): " + str(len(data)))

        # Validate JSON data for duplicates and parse them into separate JSON lists
        raids = []
        gyms = []
        pokemons = []
        quests = []
        for item in data:
            if item['type'] == "raid":
                if item['message'] not in raids:
                    raids.append(item['message'])
            if item['type'] == "gym":
                if item['message'] not in gyms:
                    gyms.append(item['message'])
            if item['type'] == "pokemon":
                if item['message'] not in pokemons:
                    pokemons.append(item['message'])
            if item['type'] == "quest":
                if item['message'] not in quests:
                    quests.append(item['message'])

        if ( MAIN_DEBUG ):
            print("NUMBER OF RAIDS PROCESSED: " + str(len(raids)))
            logging.debug("NUMBER OF RAIDS PROCESSED: " + str(len(raids)))
            print("NUMBER OF GYMS PROCESSED: " + str(len(gyms)))
            logging.debug("NUMBER OF GYMS PROCESSED: " + str(len(gyms)))
            print("NUMBER OF POKEMONS PROCESSED: " + str(len(pokemons)))
            logging.debug("NUMBER OF POKEMONS PROCESSED: " + str(len(pokemons)))
            print("NUMBER OF QUESTS PROCESSED: " + str(len(quests)))
            logging.debug("NUMBER OF QUESTS PROCESSED: " + str(len(quests)))

        if ( len(raids) ):
            for raid in raids:
                result = proces_raid(raid)
        if ( len(gyms) ):
            for gym in gyms:
                result = process_gym(gym)
        if ( len(pokemons) ):
            for pokemon in pokemons:
                result = process_pokemon(pokemon)
        if ( len(quests) ):
            for quest in quests:
                result = process_quest(quest)

        return 'DONE PROCESSING ' + str(len(data)) + ' MESSAGE(S).\n', 200
    else:
        abort(400)


if __name__ == '__main__':
    app.run(host=HOST,port=PORT)

