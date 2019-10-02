import asyncio
from django.core.management.base import BaseCommand, CommandError
from blockchain_stream.kafka.consumers import BitcoinStoreTransactions


class Command(BaseCommand):
    help = 'Start consumer for storing transactions according to timestamp, hash'

    def handle(self, *args, **kwargs):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(
            BitcoinStoreTransactions().consume()
        )
