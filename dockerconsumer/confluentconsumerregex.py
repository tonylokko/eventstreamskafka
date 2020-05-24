from confluent_kafka import Consumer, KafkaError
import re
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

try:
    while True:
        msg = c.poll(0.1)
        if msg is None:
            continue
        elif not msg.error():
            whatimlookingfor = '{0}'.format(msg.value())
            myregex = r"(?:)" + re.escape(domain) + r"(.*)"
            try:
                matchobj = re.search(myregex, whatimlookingfor, re.IGNORECASE).group(0)
            #matchobj = re.search(rf"(?:\bre.escape{(domain)}\b(.*))", whatimlookingfor).group(0)
            except:
                matchobj = None
            if matchobj is not None:
                whatisit = (matchobj)
                thatsit = format(whatisit)
                print(thatsit)
            else:
                continue
           # matchobj = re.findall(r"myexampledomain.com$", whatimlookingfor)
           # matchobj = re.search("myexampledomain.com", whatimlookingfor)
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            print('End of partition reached {0}/{1}'
                  .format(msg.topic(), msg.partition()))
        else:
            print('Error occured: {0}'.format(msg.error().str()))

except KeyboardInterrupt:
    pass

finally:
    c.close()