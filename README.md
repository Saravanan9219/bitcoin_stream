Build a streaming application which reads data from realtime bitcoin transactions and host it in GitHub.

1. Receive streaming bitcoin transaction data from here: https://www.blockchain.com/api/api_websocket
2. Stream the transaction log to kafka
3. Analyze the transactions in realtime and count the rate of transactions on a given minute, save this in redis
4. Consume the transactions from a kafka consumer and persist only the transactions made in the last 3 hours
5. Build a rest api in any python framework to read from redis to show the latest transactions


/show_transactions/

Display latest 100 transactions


/transactions_count_per_minute/{min_value}

Display number of transactions per minute for the last hour

Example:

minute counts

1:01   102

1:02   98

1:03   113

...
j
...


/high_value_addr

Display the bitcoin addresses which has the most aggregate value in transactions in the last 3 hours.

Example:

address                                 total_value

35wEodJ1x64uUy3PkenD1gT3tnrHhQSNTG      1790000

3AL3peHJm1FpFAQ1Hp9rFBmt2cbp1deXHX      490000

...

...


## Implementation
It is assumed that kafka and zookeeper are already setup and it is running. There are three consumer groups and one producer for consuming and producing bitcoin transactions and storing it in kafka.


### Producer
The producer will listen to wss://ws.blockchain.info/inv websocket for bitcoin transactions. It will post the transactions to the topic 'bitcoin_transactions', if the topic is not already present it will create new topic 'bitcoin_transactions' with three partitions and one replica, then it will start posting the transaction data to kafka. Messages are routed to partitions
based on minute of the timestamp in the message.


### Consumers
There are three consumer groups for consuming transaction data from kafka.

- Consumer group for storing transactions per minute.
- Consumer group for storing aggregated value for bitcoin addresses.
- Consumer group for storing transactions.


#### Consumer group for storing transactions per minute.
There will be three consumer processes running in this consumer group, so that each partition can be processed one consumer.
The consumer will keep a count of messages for current minute of the transaction it is processing, as soon the message for 
next minute is received, the message count is stored in redis. As there are three partitions, most of the time no messages
will be processed by the same consumer for sequential minutes.


#### Consumer group for storing aggregated value for bitcoin addresses.
There will be one consumer in this consumer group, if required more consumers can be created. It stores the aggregated value
per bitcoin address in redis.


### Consumer group for storing transactions.
There will be one consumer in this consumer group, if required more consumers can be created. It stores the transactions according to the hash and timestamp of the transaction.



## TODO:
TESTS