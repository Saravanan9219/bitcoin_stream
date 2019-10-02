import asyncio
from django.core.management.base import BaseCommand, CommandError
from blockchain_stream.kafka.consumers import BitcoinValueAggregation


class Command(BaseCommand):
    help = 'Start consumer for storing aggregated value per address'

    def handle(self, *args, **kwargs):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(
            BitcoinValueAggregation().consume()
        )
