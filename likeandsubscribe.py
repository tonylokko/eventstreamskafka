import requests, json
from confluent_kafka import Consumer

settings = {
    'bootstrap.servers': 'kafkabox:9092',
    'group.id': 'newgroup',
    'client.id': 'client-2',
    'enable.auto.commit': False,
    'session.timeout.ms': 6001,
    'default.topic.config': {'auto.offset.reset': 'latest'}
}

c = Consumer(settings)
c.subscribe(['monitoredurls'])

try:
    while True:
        msg = c.poll(0.1)
        if msg is None:
            continue
        elif not msg.error():
            whatimlookingfor = '{0}'.format(msg.value())
            yoink = requests.get(whatimlookingfor[2:-1])

            print(yoink.headers)
            c.commit()
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            print('End of partition reached {0}/{1}'
                  .format(msg.topic(), msg.partition()))
        else:
            print('Error occured: {0}'.format(msg.error().str()))

except KeyboardInterrupt:
    pass

finally:
    c.close()
