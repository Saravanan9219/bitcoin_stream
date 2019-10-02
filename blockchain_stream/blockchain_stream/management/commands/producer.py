import asyncio
from django.core.management.base import BaseCommand, CommandError
from blockchain_stream.kafka.producers import BitcoinTransactionsProducer


class Command(BaseCommand):
    help = 'Start Producer for sending transactions to kafka'

    def handle(self, *args, **kwargs):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(
            BitcoinTransactionsProducer().produce()
        )
