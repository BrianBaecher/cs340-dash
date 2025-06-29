from pymongo import MongoClient
from bson.objectid import ObjectId
from pymongo.errors import ConnectionFailure


class AnimalShelter(object):
    """ CRUD operations for Animal collection in MongoDB """

    def __init__(self, username, pwd):
        USER = username
        PASS = pwd
        HOST = 'nv-desktop-services.apporto.com'
        PORT = 34492
        DB = 'AAC'
        COL = 'animals'

        self.client = MongoClient('mongodb://%s:%s@%s:%d' % (USER, PASS, HOST, PORT))
        self.database = self.client['%s' % (DB)]
        self.collection = self.database['%s' % (COL)]

        # I don't think invalid credentials/rejected connection will throw an error here, so ctor could exit normally but the class wont work...
        # looked at MongoClient.ping, but that doesn't require credentials so it won't throw.
        # try a query, if it throws error that serves as a rejected login...
        self.read({"animal_type": "Dog"}, 1)

    def create(self, data):
        """
        Attempts to insert a record to database.
        :param data: object to insert
        :return: if successful, the Bson ObjectID of created entry. else null/None
        """
        try:
            if data is not None:
                result = self.collection.insert_one(data)
                if result.acknowledged:
                    print("successfully inserted entry")
                    return result.inserted_id
                else:
                    print("failed to insert entry")
                    return None
            else:
                raise Exception("Nothing to save, because data parameter is empty")
        except Exception as ex:
            print("error on create")
            print(ex)
            return None

    # Create method to implement the R in CRUD.
    def read(self, query, resLimit=None)-> list:
        """
        Attempts to fetch entries from database using supplied query parameters.
        :param query: dictionary representing mongo query
        :param resLimit: Optional integer argument to limit the number of entries received from query. If left unspecified, no limit is applied.
        :return: list of dicts representing mongo entries, or empty list if no results/error is thrown
        """
        cursor = None

        try:
            if (resLimit is not None and resLimit > 0):
                cursor = self.collection.find(query).limit(resLimit)
            else:
                cursor = self.collection.find(query)

        except Exception as ex:
            print("error on read")
            print(ex)
            return []

        # cast to list on an empty cursor produces an empty list.
        as_list = list(cursor)
        return as_list

    def update(self, query, updated_data) -> int:
        # Input -> arguments to function should be the key/value lookup pair to use with the MongoDB driver Find API call.
        # The last argument to function will be a set of key/value pairs in the data--
        # --type acceptable to the MongoDB driver update_one() or update_many() API call.
        # Return -> The number of objects modified in the collection.
        try:
            _res = None

            # UPDATE ONE - query is of type dict
            if (type(query) == dict):
                res = self.collection.update_one(query, {"$set": updated_data})
                if (res.acknowledged):
                    _res = res

            # UPDATE MANY - query is list of dict
            elif (type(query) == list):
                or_query = { "$or": query}

                res = self.collection.update_many(or_query, {"$set": updated_data})
                if (res.acknowledged):
                    _res = res

            if(_res is not None):
                return _res.modified_count
            else:
                raise Exception("null UpdateResponse")
        except Exception as ex:
            print("error on update")
            print(ex)
            return -1

    def delete(self, query) -> int:
        # Input -> arguments to function should be the key/value lookup pair to use with the MongoDB driver find API call.
        # Return -> The number of objects removed from the collection.
        try:
            _res = None

            # DELETE ONE - query is of type dict
            if(type(query) == dict):
                res = self.collection.delete_one(query);
                if(res.acknowledged):
                    _res = res

            # DELETE MANY - query is list of dict
            elif(type(query) == list):
                # include $or keyword
                or_query = {
                    "$or": query
                }

                res = self.collection.delete_many(or_query)
                if(res.acknowledged):
                    _res = res

            if(_res is not None):
                return _res.deleted_count
            else:
                raise Exception("null DeleteResponse")
        except Exception as ex:
            print("Error on delete")
            print(ex)
            return -1
