import re
import socket
import argparse
from confluent_kafka import Consumer, KafkaError, Producer
from confluent_kafka.admin import AdminClient,NewTopic


settings = {
    'bootstrap.servers': 'kafkabox:9092',
    'group.id': 'mygroup',
    'client.id': 'client-1',
    'enable.auto.commit': False,
    'session.timeout.ms': 6001,
    'default.topic.config': {'auto.offset.reset': 'earliest'}
}
domain = "myexampledomain.com test4"

parser = argparse.ArgumentParser()
parser.add_argument('domain',help="input domain to monitor", type=str)
parser.add_argument('bucket',help="input bucket to monitor", type=str)
args,unknown = parser.parse_known_args()
domain = args.domain
bucket = args.bucket
mydomain = domain + " " + bucket
print(mydomain)
c = Consumer(settings)

c.subscribe(['gatewayaudit'])
conf = {'bootstrap.servers': 'kafkabox:9092', 'client.id': socket.gethostname() }
producer = Producer(conf)
kafka_admin = AdminClient(conf)
newtopic = []
newtopic.append(NewTopic('monitoredurls', num_partitions=1, replication_factor=1))
kafka_admin.create_topics(new_topics=newtopic, validate_only=False)




try:
    while True:
        msg = c.poll(0.1)
        if msg is None:
            continue
        elif not msg.error():
            whatimlookingfor = '{0}'.format(msg.value())
            try:
                myregex = r"(?:)" + re.escape(mydomain) + r"(.*)"
                otherregex = re.compile("(POST|MULTIPART_COMPLETE)" + r"(.*)")
                matchobj = re.search(otherregex, whatimlookingfor).group(0)
                print(matchobj)
                matchobj2 = re.search(myregex, matchobj, re.IGNORECASE).group(0)
                print(matchobj2)
            except:
                matchobj = None
                matchobj2 = None
            #matchobj = re.search(rf"(?:\bre.escape{(domain)}\b(.*))", whatimlookingfor).group(0)
            if matchobj is not None:
                whatisit = (matchobj2)
                thatsit = format(whatisit)
                thatsit2 = thatsit.replace(" ", "/")
                finalurl = f"http://{thatsit2}"
                print(thatsit)
                print(thatsit2)
                print(finalurl[:-1])
                producer.produce('monitoredurls',key='url',value=finalurl[:-1])
                c.commit()
            else:
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
