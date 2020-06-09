
import json
import pymongo
import logging

from pymongo import MongoClient
from datetime import datetime


# import entrance door data into the db
def import_fridge_data(db, loggging) : 
    
    # get db collection
    collection = db.itsite

    # open bathroom log file
    with open('data/testsite-it/testsite-it-fridge.txt', 'r') as f : 
        # read data records
        for line in f : 
            
            # extract record parameters
            params = line.split("|")
            # get event time 
            time = datetime.fromtimestamp(float(params[1]) / 1000)
            
            # check if record already exists
            cursor = collection.find({"_id" : float(params[1])}, no_cursor_timeout = True)
            if cursor.count() == 0 :
                
                # event content as json object
                content = json.loads(params[2])
                if content['open'] == 'true' : 
                    
                    # record to insert 
                    record = {
                        "_id" : float(params[1]),
                        "date" : time,
                        "location" : "fridge",
                        "value" : "open"
                    }
                    
                    # detected presence in the badroom
                    logging.debug("Inserting record : {}".format(record))
                    # insert data into the db 
                    collection.insert_one(record)
                    
                elif content['open'] == 'false' : 
                        
                    # record to insert 
                    record = {
                        "_id" : float(params[1]),
                        "date" : time,
                        "location" : "fridge",
                        "value" : "close"
                    }
                    
                    # detected presence in the badroom
                    logging.debug("Inserting record : {}".format(record))
                    # insert data into the db 
                    collection.insert_one(record)                    
                        
                else : 
                 
                    # record to insert 
                    record = {
                        "_id" : float(params[1]),
                        "date" : time,
                        "location" : "fridge",
                        "value" : "unknown"
                    }
                    
                    # detected presence in the badroom
                    logging.debug("Inserting record : {}".format(record))
                    # insert data into the db 
                    collection.insert_one(record)                    
                
            else : 
                
                # record already stored
                logging.debug("Record already stored : {}".format(float(params[1])))            
                    
                    
            # close cursor
            cursor.close()


# import entrance door data into the db
def import_entrance_door_data(db, loggging) : 
    
    # get db collection
    collection = db.itsite

    # open bathroom log file
    with open('data/testsite-it/testsite-it-entrancedoor.txt', 'r') as f : 
        # read data records
        for line in f : 
            
            # extract record parameters
            params = line.split("|")
            # get event time 
            time = datetime.fromtimestamp(float(params[1]) / 1000)
            
            # check if record already exists
            cursor = collection.find({"_id" : float(params[1])}, no_cursor_timeout = True)
            if cursor.count() == 0 :
                
                # event content as json object
                content = json.loads(params[2])
                if content['open'] == 'true' : 
                    
                    # record to insert 
                    record = {
                        "_id" : float(params[1]),
                        "date" : time,
                        "location" : "entrance door",
                        "value" : "open"
                    }
                    
                    # detected presence in the badroom
                    logging.debug("Inserting record : {}".format(record))
                    # insert data into the db 
                    collection.insert_one(record)
                    
                elif content['open'] == 'false' : 
                        
                    # record to insert 
                    record = {
                        "_id" : float(params[1]),
                        "date" : time,
                        "location" : "entrance door",
                        "value" : "close"
                    }
                    
                    # detected presence in the badroom
                    logging.debug("Inserting record : {}".format(record))
                    # insert data into the db 
                    collection.insert_one(record)                    
                        
                else : 
                 
                    # record to insert 
                    record = {
                        "_id" : float(params[1]),
                        "date" : time,
                        "location" : "entrance door",
                        "value" : "unknown"
                    }
                    
                    # detected presence in the badroom
                    logging.debug("Inserting record : {}".format(record))
                    # insert data into the db 
                    collection.insert_one(record)                    
                
            else : 
                
                # record already stored
                logging.debug("Record already stored : {}".format(float(params[1])))            
                    
                    
            # close cursor
            cursor.close()


# import hallway data into the db
def import_hallway_data(db, loggging) : 
    
    # get db collection
    collection = db.itsite

    # open bathroom log file
    with open('data/testsite-it/testsite-it-hallway.txt', 'r') as f : 
        # read data records
        for line in f : 
            
            # extract record parameters
            params = line.split("|")
            # get event time 
            time = datetime.fromtimestamp(float(params[1]) / 1000)
            
            # check if record already exists
            cursor = collection.find({"_id" : float(params[1])}, no_cursor_timeout = True)
            if cursor.count() == 0 :
                
                # event content as json object
                content = json.loads(params[2])
                if content['is present'] == 'true' : 
                    
                    # record to insert 
                    record = {
                        "_id" : float(params[1]),
                        "date" : time,
                        "location" : "hallway",
                        "value" : "presence"
                    }
                    
                    # detected presence in the badroom
                    logging.debug("Inserting record : {}".format(record))
                    # insert data into the db 
                    collection.insert_one(record)
                    
                elif content['is present'] == 'false' : 
                        
                    # record to insert 
                    record = {
                        "_id" : float(params[1]),
                        "date" : time,
                        "location" : "hallway",
                        "value" : "none"
                    }
                    
                    # detected presence in the badroom
                    logging.debug("Inserting record : {}".format(record))
                    # insert data into the db 
                    collection.insert_one(record)                    
                        
                else : 
                 
                    # record to insert 
                    record = {
                        "_id" : float(params[1]),
                        "date" : time,
                        "location" : "hallway",
                        "value" : "unknown"
                    }
                    
                    # detected presence in the badroom
                    logging.debug("Inserting record : {}".format(record))
                    # insert data into the db 
                    collection.insert_one(record)                    
                
            else : 
                
                # record already stored
                logging.debug("Record already stored : {}".format(float(params[1])))            
                    
                    
            # close cursor
            cursor.close()

# import livingroom data into the db
def import_livingroom_data(db, loggging) : 
    
    # get db collection
    collection = db.itsite

    # open bathroom log file
    with open('data/testsite-it/testsite-it-livingroom.txt', 'r') as f : 
        # read data records
        for line in f : 
            
            # extract record parameters
            params = line.split("|")
            # get event time 
            time = datetime.fromtimestamp(float(params[1]) / 1000)
            
            # check if record already exists
            cursor = collection.find({"_id" : float(params[1])}, no_cursor_timeout = True)
            if cursor.count() == 0 :
                
                # event content as json object
                content = json.loads(params[2])
                if content['is present'] == 'true' : 
                    
                    # record to insert 
                    record = {
                        "_id" : float(params[1]),
                        "date" : time,
                        "location" : "livingroom",
                        "value" : "presence"
                    }
                    
                    # detected presence in the badroom
                    logging.debug("Inserting record : {}".format(record))
                    # insert data into the db 
                    collection.insert_one(record)
                    
                elif content['is present'] == 'false' : 
                        
                    # record to insert 
                    record = {
                        "_id" : float(params[1]),
                        "date" : time,
                        "location" : "livingroom",
                        "value" : "none"
                    }
                    
                    # detected presence in the badroom
                    logging.debug("Inserting record : {}".format(record))
                    # insert data into the db 
                    collection.insert_one(record)                    
                        
                else : 
                 
                    # record to insert 
                    record = {
                        "_id" : float(params[1]),
                        "date" : time,
                        "location" : "livingroom",
                        "value" : "unknown"
                    }
                    
                    # detected presence in the badroom
                    logging.debug("Inserting record : {}".format(record))
                    # insert data into the db 
                    collection.insert_one(record)                    
                
            else : 
                
                # record already stored
                logging.debug("Record already stored : {}".format(float(params[1])))            
                    
                    
            # close cursor
            cursor.close()


# import bedroom chair data into the db
def import_bedroom_chair_data(db, logging) : 
    
    # get db collectiobn
    collection = db.itsite
    
    # open bathroom log file
    with open('data/testsite-it/testsite-it-bedroom-chair.txt', 'r') as f : 
        # read data records
        for line in f : 
            # extract record parameters
            params = line.split("|")
            # get event time 
            time = datetime.fromtimestamp(float(params[1]) / 1000)
            
            # check if record already exists in the db
            cursor = collection.find({"_id" : float(params[1])}, no_cursor_timeout = True)
            if cursor.count() == 0 : 
                
                # event content as json object
                content = json.loads(params[2])
                if content['is pressed'] == 'true' : 
                    
                    # record to insert
                    record = {
                        "_id" : float(params[1]),
                        "date" : time,
                        "location" : "bedroom chair",
                        "value" : "pressed"
                    }
                    
                    # detected presence in the bathroom
                    logging.debug("Inserting record : {}".format(record)) 
                    # insert data into the database
                    collection.insert_one(record)
                    
                elif content['is pressed'] == 'false' : 
                    
                    # record to insert
                    record = {
                        "_id" : float(params[1]),
                        "date" : time,
                        "location" : "bedroom chair",
                        "value" : "not pressed"
                    }
                    
                    # detected presence in the kitchen
                    logging.debug("Inserting record : {}".format(record)) 
                    # insert data into the database
                    collection.insert_one(record)                    
                    
                else : 
                    
                    # record to insert
                    record = {
                        "_id" : float(params[1]),
                        "date" : time,
                        "location" : "bedroom chair",
                        "value" : "unknown"
                    }
                    
                    # detected presence in the kitchen
                    logging.debug("Inserting record : {}".format(record)) 
                    # insert data into the database
                    collection.insert_one(record)                    
                    
            else : 
                
                # record already stored
                logging.debug("Record already stored : {}".format(float(params[1])))            
                
                    
            # close cursor
            cursor.close()     


# import livingroom chair data into the db
def import_livingroom_chair_data(db, logging) : 
    
    # get db collectiobn
    collection = db.itsite
    
    # open bathroom log file
    with open('data/testsite-it/testsite-it-livingroom-chair.txt', 'r') as f : 
        # read data records
        for line in f : 
            # extract record parameters
            params = line.split("|")
            # get event time 
            time = datetime.fromtimestamp(float(params[1]) / 1000)
            
            # check if record already exists in the db
            cursor = collection.find({"_id" : float(params[1])}, no_cursor_timeout = True)
            if cursor.count() == 0 : 
                
                # event content as json object
                content = json.loads(params[2])
                if content['is pressed'] == 'true' : 
                    
                    # record to insert
                    record = {
                        "_id" : float(params[1]),
                        "date" : time,
                        "location" : "livingroom chair",
                        "value" : "pressed"
                    }
                    
                    # detected presence in the bathroom
                    logging.debug("Inserting record : {}".format(record)) 
                    # insert data into the database
                    collection.insert_one(record)
                    
                elif content['is pressed'] == 'false' : 
                    
                    # record to insert
                    record = {
                        "_id" : float(params[1]),
                        "date" : time,
                        "location" : "livingroom chair",
                        "value" : "not pressed"
                    }
                    
                    # detected presence in the kitchen
                    logging.debug("Inserting record : {}".format(record)) 
                    # insert data into the database
                    collection.insert_one(record)                    
                    
                else : 
                    
                    # record to insert
                    record = {
                        "_id" : float(params[1]),
                        "date" : time,
                        "location" : "livingroom chair",
                        "value" : "unknown"
                    }
                    
                    # detected presence in the kitchen
                    logging.debug("Inserting record : {}".format(record)) 
                    # insert data into the database
                    collection.insert_one(record)                    
                    
            else : 
                
                # record already stored
                logging.debug("Record already stored : {}".format(float(params[1])))            
                
                    
            # close cursor
            cursor.close()     



# import kitchen data into the db
def import_kitchen_data(db, logging) : 
    
    # get db collectiobn
    collection = db.itsite
    
    # open bathroom log file
    with open('data/testsite-it/testsite-it-kitchen.txt', 'r') as f : 
        # read data records
        for line in f : 
            # extract record parameters
            params = line.split("|")
            # get event time 
            time = datetime.fromtimestamp(float(params[1]) / 1000)
            
            # check if record already exists in the db
            cursor = collection.find({"_id" : float(params[1])}, no_cursor_timeout = True)
            if cursor.count() == 0 : 
                
                # event content as json object
                content = json.loads(params[2])
                if content['is present'] == 'true' : 
                    
                    # record to insert
                    record = {
                        "_id" : float(params[1]),
                        "date" : time,
                        "location" : "kitchen",
                        "value" : "presence"
                    }
                    
                    # detected presence in the bathroom
                    logging.debug("Inserting record : {}".format(record)) 
                    # insert data into the database
                    collection.insert_one(record)
                    
                elif content['is present'] == 'false' : 
                    
                    # record to insert
                    record = {
                        "_id" : float(params[1]),
                        "date" : time,
                        "location" : "kitchen",
                        "value" : "none"
                    }
                    
                    # detected presence in the kitchen
                    logging.debug("Inserting record : {}".format(record)) 
                    # insert data into the database
                    collection.insert_one(record)                    
                    
                else : 
                    
                    # record to insert
                    record = {
                        "_id" : float(params[1]),
                        "date" : time,
                        "location" : "kitchen",
                        "value" : "unknown"
                    }
                    
                    # detected presence in the kitchen
                    logging.debug("Inserting record : {}".format(record)) 
                    # insert data into the database
                    collection.insert_one(record)                    
                    
            else : 
                
                # record already stored
                logging.debug("Record already stored : {}".format(float(params[1])))            
                
                    
            # close cursor
            cursor.close()     

# import bathroom data into the db
def import_bathroom_data(db, logging) : 
    
    # get db collectiobn
    collection = db.itsite
    
    # open bathroom log file
    with open('data/testsite-it/testsite-it-bathroom.txt', 'r') as f : 
        # read data records
        for line in f : 
            # extract record parameters
            params = line.split("|")
            # get event time 
            time = datetime.fromtimestamp(float(params[1]) / 1000)
            
            # check if record already exists in the db
            cursor = collection.find({"_id" : float(params[1])}, no_cursor_timeout = True)
            if cursor.count() == 0 : 
                
                # event content as json object
                content = json.loads(params[2])
                if content['is present'] == 'true' : 
                    
                    # record to insert
                    record = {
                        "_id" : float(params[1]),
                        "date" : time,
                        "location" : "bathroom",
                        "value" : "presence"
                    }
                    
                    # detected presence in the bathroom
                    logging.debug("Inserting record : {}".format(record)) 
                    # insert data into the database
                    collection.insert_one(record)
                    
                elif content['is present'] == 'false' : 
                    
                    # record to insert
                    record = {
                        "_id" : float(params[1]),
                        "date" : time,
                        "location" : "bathroom",
                        "value" : "none"
                    }
                    
                    # detected presence in the kitchen
                    logging.debug("Inserting record : {}".format(record)) 
                    # insert data into the database
                    collection.insert_one(record)                    
                    
                else : 
                    
                    # record to insert
                    record = {
                        "_id" : float(params[1]),
                        "date" : time,
                        "location" : "bathroom",
                        "value" : "unknown"
                    }
                    
                    # detected presence in the kitchen
                    logging.debug("Inserting record : {}".format(record)) 
                    # insert data into the database
                    collection.insert_one(record)                    
                    
            else : 
                
                # record already stored
                logging.debug("Record already stored : {}".format(float(params[1])))            
                
                    
            # close cursor
            cursor.close()            
                
                
# import bedroom data into the db
def import_bedroom_data(db, loggging) : 
    
    # get db collection
    collection = db.itsite

    # open bathroom log file
    with open('data/testsite-it/testsite-it-bedroom.txt', 'r') as f : 
        # read data records
        for line in f : 
            
            # extract record parameters
            params = line.split("|")
            # get event time 
            time = datetime.fromtimestamp(float(params[1]) / 1000)
            
            # check if record already exists
            cursor = collection.find({"_id" : float(params[1])}, no_cursor_timeout = True)
            if cursor.count() == 0 :
                
                # event content as json object
                content = json.loads(params[2])
                if content['is present'] == 'true' : 
                    
                    # record to insert 
                    record = {
                        "_id" : float(params[1]),
                        "date" : time,
                        "location" : "bedroom",
                        "value" : "presence"
                    }
                    
                    # detected presence in the badroom
                    logging.debug("Inserting record : {}".format(record))
                    # insert data into the db 
                    collection.insert_one(record)
                    
                elif content['is present'] == 'false' : 
                        
                    # record to insert 
                    record = {
                        "_id" : float(params[1]),
                        "date" : time,
                        "location" : "bedroom",
                        "value" : "none"
                    }
                    
                    # detected presence in the badroom
                    logging.debug("Inserting record : {}".format(record))
                    # insert data into the db 
                    collection.insert_one(record)                    
                        
                else : 
                 
                    # record to insert 
                    record = {
                        "_id" : float(params[1]),
                        "date" : time,
                        "location" : "bedroom",
                        "value" : "unknown"
                    }
                    
                    # detected presence in the badroom
                    logging.debug("Inserting record : {}".format(record))
                    # insert data into the db 
                    collection.insert_one(record)                    
                
            else : 
                
                # record already stored
                logging.debug("Record already stored : {}".format(float(params[1])))            
                    
                    
            # close cursor
            cursor.close()
                

# main method
if __name__ == '__main__' : 
    
    # set logging level 
    logging.getLogger().setLevel(logging.DEBUG)    
    # connect to mongodb 
    client = MongoClient()
    # get db 
    db = client.giraff
    
    # import observations from bathroom
    logging.info('Importing data about bathroom')
    import_bathroom_data(db, logging)
    
    logging.info('Importing data about bedroom')
    import_bedroom_data(db, logging)
    
    logging.info('Importing data about kitchen')
    import_kitchen_data(db, logging)    
    
    logging.info('Importing data about livingroom chair')
    import_livingroom_chair_data(db, logging)

    logging.info('Importing data about bedroom chair')    
    import_bedroom_chair_data(db, logging)
    
    logging.info('Importing data about livingroom')    
    import_livingroom_data(db, logging)    

    logging.info('Importing data about hallway')        
    import_hallway_data(db, logging)
    