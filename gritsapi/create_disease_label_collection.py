from pymongo import MongoClient
import disease_label_table
import config

if __name__ == '__main__':
    client = MongoClient(config.mongo_url)
    db = client.girder
    db.drop_collection('diseaseNames')
    for label in disease_label_table.get_labels():
        db['diseaseNames'].insert({'_id': label})
