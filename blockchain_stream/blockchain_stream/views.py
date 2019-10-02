import datetime
import json
from django.conf import settings
from rest_framework import response
from rest_framework.decorators import api_view, renderer_classes
from rest_framework.renderers import JSONRenderer
import redis


@renderer_classes(JSONRenderer)
@api_view(http_method_names=['GET', ])
def show_transactions(request):
    redis_conn = redis.Redis(
        host=settings.REDIS['SERVER'], port=settings.REDIS['PORT'])
    # sort the hash sort by timestamp
    latest_transaction_hashes = redis_conn.sort(
        'bitcoin_transactions', start=0, num=10, by='bitcoin_transaction:*->timestamp', desc=True)
    # for keys for getting transaction data
    keys = [f"bitcoin_transaction:{hash_.decode('utf-8')}"
            for hash_ in latest_transaction_hashes
            ]
    pipe = redis_conn.pipeline()
    for key in keys:
        pipe.hget(key, 'data')
    latest_transactions = [json.loads(data) for data in pipe.execute()]
    return response.Response(data=latest_transactions)


@renderer_classes(JSONRenderer)
@api_view(http_method_names=['GET', ])
def transactions_per_minute(request, min_value):
    keys = []
    redis_conn = redis.Redis(
        host=settings.REDIS['SERVER'], port=settings.REDIS['PORT'])
    for minute_difference in range(0, 60):
        event_time = datetime.datetime.now() - datetime.timedelta(minutes=minute_difference)
        key = f'{event_time.hour}:{event_time.minute:02}'
        keys.append(key)
    pipe = redis_conn.pipeline()
    for key in keys:
        pipe.get(key)
    data = [
        value for value in list(
            zip(keys, pipe.execute())) if value[1] and int(value[1]) >= min_value]
    return response.Response(data=data)


@renderer_classes(JSONRenderer)
@api_view(http_method_names=['GET', ])
def high_addr(request):
    redis_conn = redis.Redis(
        host=settings.REDIS['SERVER'], port=settings.REDIS['PORT'])
    latest_transaction_addresses = redis_conn.sort(
        'bitcoin_addresses', start=0, num=10, by='bitcoin_addr:*->agg', desc=True)
    keys = [f"bitcoin_addr:{address.decode('utf-8')}"
            for address in latest_transaction_addresses
            ]
    pipe = redis_conn.pipeline()
    for key in keys:
        pipe.hget(key, 'agg')
    agg_values = list(zip(keys, pipe.execute()))
    return response.Response(data=agg_values)
