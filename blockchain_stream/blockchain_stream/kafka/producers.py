"""
The producer will listen to wss://ws.blockchain.info/inv websocket for bitcoin transactions. 
It will post the transactions to the topic 'bitcoin_transactions', if the topic is not already present 
it will create new topic 'bitcoin_transactions' with three partitions and one replica, 
then it will start posting the transaction data to kafka. Messages are routed to partitions
based on minute of the timestamp in the message.
"""

import aiohttp
import datetime
import json
import msgpack
import logging
from kafka import KafkaProducer, KafkaClient
from kafka.admin import KafkaAdminClient, NewTopic
from django.conf import settings


BITCOIN_TRANANSACTIONS = 'bitcoin_transactions'
RETENTION_TIME = '10800000'  # 3 hours
BITCOIN_SOCKET_ADDRESS = "wss://ws.blockchain.info/inv"


class BitcoinTransactionsProducer:

    def __init__(self):
        self.session = session = aiohttp.ClientSession()
        self.topic = BITCOIN_TRANANSACTIONS
        self.producer = KafkaProducer(
            bootstrap_servers=[settings.KAFKA['SERVER']],
            value_serializer=msgpack.dumps,
            key_serializer=msgpack.dumps)

    def create_topic(self):
        kafka_admin_client = KafkaAdminClient(
            bootstrap_servers=[settings.KAFKA['SERVER']])
        topic = NewTopic(
            name=topic, num_partitions=3, topic_configs={'retention.ms': RETENTION_TIME},
            replication_factor=1)
        kafka_admin_client.create_topics([topic])

    def make_sure_topic_exists(self):
        kafka_client = KafkaClient(settings.KAFKA['SERVER'])
        if BITCOIN_TRANANSACTIONS not in kafka_client.topics:
            self.create_topic()

    def on_send_error(self, exception):
        # TODO: Retry mechanism
        logging.error('Producer Error - %s', exception)

    def send_data(self, partition, data):
        try:
            self.producer.send(
                self.topic,
                value=data,
                partition=partition
            ).add_errback(self.on_send_error)
        except BaseException as e:
            logging.exception(e)

    async def produce(self):
        self.make_sure_topic_exists()
        async with self.session.ws_connect(BITCOIN_SOCKET_ADDRESS) as ws:
            # subscribe to all transactions
            await ws.send_str('{"op":"unconfirmed_sub"}')
            logging.info(
                'Subsribed to bitcoin transactions, read to receive transactions')
            async for msg in ws:
                data = json.loads(msg.data)
                time = int(data['x']['time'])
                minute = datetime.datetime.utcfromtimestamp(time).minute
                partition = minute % 3  # partition based on minute
                logging.debug('Message - %s Partition - %s',
                              data, str(partition))
                self.send_data(partition, data)
