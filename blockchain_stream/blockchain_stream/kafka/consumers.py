import json
import asyncio
import aioredis
import datetime
import msgpack
import logging
from kafka import KafkaConsumer
from django.conf import settings


EXPIRE_IN_MS = 10800


class BitcoinTransactionConsumerBase:

    def __init__(self):
        self.__redis_conn = None
        self.topic = 'bitcoin_transactions'

    async def get_redis_connection(self):
        if self.__redis_conn is None:
            redis = await aioredis.create_redis_pool(f'redis://{settings.REDIS["SERVER"]}')
            self.__redis_conn = redis
        return self.__redis_conn

    async def consume(self):
        raise NotImplementedError()


class BitcoinTransactionsPerMinute(BitcoinTransactionConsumerBase):

    """
    Consumer to store transaction rate per minute in redis
    """

    async def set_transactions_rate(self, key, value):
        # increase timestamp by count
        redis = await self.get_redis_connection()
        try:
            transaction = redis.multi_exec()
            transaction.incrby(key, value)
            # expire the key after 3 hours
            transaction.expire(key, EXPIRE_IN_MS)
            await transaction.execute()
        except BaseException as e:
            # TODO: Retry mechanism
            logging.exception(e)

    async def consume(self):
        consumer = KafkaConsumer(
            self.topic,
            group_id='bitcoin_consumer_transactions_per_minute',
            bootstrap_servers=[settings.KAFKA['SERVER']],
            key_deserializer=msgpack.loads,
            value_deserializer=lambda value: msgpack.loads(value, raw=False),
            auto_offset_reset='latest'
        )
        previous_minute = None
        messages_count = 0
        pending_tasks = set()
        for message in consumer:
            event_time = int(message.value['x']['time'])
            event_time = datetime.datetime.utcfromtimestamp(event_time)
            minute = event_time.minute
            # as soon the message for next minute is received, store the message count
            if previous_minute is not None and not previous_minute == minute:
                previous_minute = minute
                event_time_key = f'{event_time.hour}:{event_time.minute:02}'

                # create a task and execute it with timeout of 0.5, so the
                # processing does not halt the consumer for more time.
                task = asyncio.create_task(
                    self.set_transactions_rate(event_time_key, messages_count))
                pending_tasks.add(task)
                done, pending = await asyncio.wait(pending_tasks, timeout=0.5)
                if pending:
                    pending_tasks.update(pending)

                # reinitialize message count
                messages_count = 0
                logging.info('Saved Message Count %s', str(event_time))
            elif previous_minute is None:
                previous_minute = minute
            messages_count += 1


class BitcoinValueAggregation(BitcoinTransactionConsumerBase):

    """
    Consumer to store value aggregations
    """

    async def store_agg_value(self, addresses_values):
        # store bitcoin addr in a set
        # store aggregated value in hash using address as part of the key

        # to get top transactions, we can sort the addr set using the
        # aggregated value stored in the hashes and return the
        # top addresses

        redis = await self.get_redis_connection()
        try:
            transaction = redis.multi_exec()
            for addr, value in addresses_values:
                if not addr:
                    continue
                transaction.hincrby(f'bitcoin_addr:{addr}', 'agg', value)
                transaction.sadd('bitcoin_addresses', addr)
                # expire the key after 3 hours
                await redis.expire(f'bitcoin_addr:{addr}', 10800)
            await transaction.execute()
        except Exception as e:
            # TODO: retry mechanism
            logging.exception(e)

    async def consume(self):
        consumer = KafkaConsumer(
            self.topic,
            group_id='bitcoin_consumer_aggregate',
            bootstrap_servers=[settings.KAFKA['SERVER']],
            key_deserializer=msgpack.loads,
            value_deserializer=lambda value: msgpack.loads(value, raw=False),
            auto_offset_reset='latest'
        )
        pending_tasks = set()
        for message in consumer:
            data = message.value
            input_ = [(_input['prev_out']['addr'], 0 - _input['prev_out']['value'])
                      for _input in data['x']['inputs']]
            output = [(out['addr'], out['value'])
                      for out in data['x']['out']]
            total_value = sum([out[1] for out in output])
            redis_data = output + input_
            task = asyncio.create_task(
                self.store_agg_value(redis_data))
            pending_tasks.add(task)
            done, pending = await asyncio.wait(pending_tasks, timeout=0.5)
            if pending:
                pending_tasks.update(pending)


class BitcoinStoreTransactions(BitcoinTransactionConsumerBase):

    """
    Consumer to store transactions according to hash, timestamp
    """

    async def store_transactions(self, hash_, timestamp, data):
        # stores hashes in a set
        # stores data along with timestamp, with hash as key

        # To get the latest transactions, we can sort set in desc order
        # using the timestamp stored in the hash, and return the latest
        # transactions according to timestamp

        redis = await self.get_redis_connection()
        data = json.dumps(data)
        try:
            transaction = redis.multi_exec()
            transaction.hmset(
                f'bitcoin_transaction:{hash_}', 'timestamp', timestamp, 'data', data)
            transaction.sadd('bitcoin_transactions', hash_)
            # expire the key after 3 hours
            await redis.expire(f'bitcoin_transaction:{hash_}', EXPIRE_IN_MS)
            await transaction.execute()
        except Exception as e:
            logging.exception(e)

    async def consume(self):
        consumer = KafkaConsumer(
            'bitcoin_transactions',
            group_id='bitcoin_consumer_aggregate',
            bootstrap_servers=[settings.KAFKA['SERVER']],
            key_deserializer=msgpack.loads,
            value_deserializer=lambda value: msgpack.loads(
                value, raw=False),
            auto_offset_reset='latest'
        )
        pending_tasks = set()
        for message in consumer:
            data = message.value
            hash_ = message.value['x']['hash']
            timestamp = message.value['x']['time']
            task = asyncio.create_task(
                self.store_transactions(hash_, timestamp, data,))
            pending_tasks.add(task)
            done, pending = await asyncio.wait(pending_tasks, timeout=0.5)
            if pending:
                pending_tasks.update(pending)
