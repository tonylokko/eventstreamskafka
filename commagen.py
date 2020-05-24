import re
import socket

from confluent_kafka import Consumer, KafkaError, Producer
from confluent_kafka.admin import AdminClient,NewTopic

settings = {
    'bootstrap.servers': 'kafkabox:9092',
    'group.id': 'mygroup',
    'client.id': 'client-1',
    'enable.auto.commit': True,
    'session.timeout.ms': 6001,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
}
domain = "myexampledomain.com test4"
c = Consumer(settings)

c.subscribe(['gatewayaudit'])
conf = {'bootstrap.servers': 'kafkabox:9092', 'client.id': socket.gethostname() }
producer = Producer(conf)
kafka_admin = AdminClient(conf)
newtopic = []
newtopic.append(NewTopic('commalogs', num_partitions=1, replication_factor=1))
kafka_admin.create_topics(new_topics=newtopic, validate_only=False)




try:
    while True:
        msg = c.poll(0.1)
        if msg is None:
            continue
        elif not msg.error():
            whatimlookingfor = '{0}'.format(msg.value())
            try:
                whatimlookingfor2 = whatimlookingfor.replace(" ", ",")
                whatimlookingfor2 = whatimlookingfor2[2:]
                producer.produce('commalogs',value=whatimlookingfor2)
            except:
                continue
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            print('End of partition reached {0}/{1}'
                  .format(msg.topic(), msg.partition()))
        else:
            print('Error occured: {0}'.format(msg.error().str()))

except KeyboardInterrupt:
    pass

finally:
    c.close()