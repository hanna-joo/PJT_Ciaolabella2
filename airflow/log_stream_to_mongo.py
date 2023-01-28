import pymongo
from kafka import KafkaConsumer
from kafka import TopicPartition
import time
from datetime import date, timedelta
from json import loads
from airflow_settings import kafka_servers, mongo_nodes


def ConsumerCreate(kafka_servers):
    kafka = KafkaConsumer(bootstrap_servers=kafka_servers,
                          auto_offset_reset='earliest')
    return kafka


def MongoClient(mongo_nodes, db):
    client_mongo = pymongo.MongoClient(mongo_nodes)
    db = client_mongo[db]
    return db


def KafkaToMongo(kafka, mongo, topic, partition, alias, refer_date):
    # get data from kafka
    topic_part = TopicPartition(topic, partition)
    kafka.assign([topic_part])
    # set start offset which timestamp is after refer_date
    start_offsets = kafka.offsets_for_times({topic_part: int(refer_date * 1000)})
    end_offsets = kafka.end_offsets([topic_part])
    # from topic to document
    doc = []
		yesterday = (date.today() - timedelta(1)).strftime('%Y%m%d')
    try:
        offset_ts = start_offsets.get(topic_part)
        start, end = offset_ts[0], end_offsets.get(topic_part)
        kafka.seek(topic_part, start)
        for msg in kafka:
            temp = loads(msg.value)
            if temp['date'] == yesterday:
                doc.append(temp)
            if msg.offset == end - 1:
                break
    except TypeError as t:
        doc.append({'date': yesterday})
        print(f'Error Message : {t}')
    # save data
    coll = mongo[alias]
    insert_id = coll.update_one({'_id': yesterday}, {"$set":{'values': doc}}, upsert=True).upserted_id
    #insert_id = coll.insert_one({'_id': yesterday}, {'values': doc}).inserted_id
    print(insert_id)
    # delete data before 7 days
    seven_days_ago = str(int(yesterday) - 7)
    coll.delete_one({'_id': seven_days_ago})

    return insert_id


if __name__ == "__main__":
    refer_date = time.time() - 1 * 24 * 60 * 60
    topic = 'log_aggregation'
    partitions = {0: 'Common', 1: 'NolabelSearch'}

    kafka = ConsumerCreate(kafka_servers)
    mongo = MongoClient(mongo_nodes, topic)

    for partition, alias in partitions.items():
        insert_id = KafkaToMongo(kafka, mongo, topic, partition, alias, refer_date)
        print(insert_id)
    kafka.close()