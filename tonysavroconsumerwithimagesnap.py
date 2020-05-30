#this is all the stuff we need to pull avro messages.
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

#we use requests and json for processing the image and pulling the schema initially.
import requests,json
#These imports are specific to the image puller
from PIL import Image
from io import BytesIO



topic = 'structuredauditgw'
SCHEMA_REGISTRY_URL = 'http://kafkabox:8081'
print("SCHEMA_REGISTRY_URL: ", SCHEMA_REGISTRY_URL)
URL = 'http://kafkabox:8081/subjects/structuredauditgw-value/versions/latest/schema'
r = requests.get(url=URL)
schema_registry = r.json()
group = 'testgroup'
bootstrap_servers = 'kafkabox:9092'
print("Schema From Schema Registry ==========================>>")
print("Schema: ", schema_registry)


def imagesnap(urllist,OBJECT):
    print(urllist)
    print(OBJECT)
    url = urllist
    namelenght = len(OBJECT) + 1
    print(namelenght)
    url2 = urllist[:- namelenght]
    print("url2 is" + url2)

    # img =Image.open(urlopen(url))
    response = requests.get(url, stream=True)
    response.raw.decode_content = True
    img = Image.open(response.raw)

    # this is the copy + thumbnail section

    new_img = img.copy()
    new_img.thumbnail((300, 300), Image.ANTIALIAS)
    byte_io = BytesIO()
    new_img.save(byte_io, 'jpeg')
    byte_io.seek(0)
    objectname = OBJECT + "_snap" + ".jpg"
    x = requests.post(url2, files={'files': (objectname, byte_io, 'image/jpeg')})
    print(x.status_code)
    print(x)

def dict_to_user(obj):
    """
    Converts object literal(dict) to a User instance.

    Args:
        obj (dict): Object literal(dict)

        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

    """
    if obj is None:
        return None
    else:
        return record(domain=obj['domain'], bucket=obj['bucket'], object=obj['object'])


def main():

    sr_conf = {'url': SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(sr_conf)
    schema_str = """
    {
        "namespace": "io.confluent.ksql.avro_schemas",
        "name": "User",
        "type": "record",
        "fields":[
        {"name":"DATESTAMP","type":"string"},
        {"name":"TIMESTAMP","type":"string"},
        {"name":"MILLISEC","type":"string"},
        {"name":"LOGLEVEL","type":"string"},
        {"name":"REQUESTID","type":"string"},
        {"name":"RECORDFORMATVERSION","type":"string"},
        {"name":"SOURCEIP","type":"string"},
        {"name":"DNSDOMAIN","type":"string"},
        {"name":"MESSAGETYPE","type":"string"},
        {"name":"OPERATION","type":"string"},
        {"name":"AUTHUSER","type":"string"},
        {"name":"AUTHDOMAIN","type":"string"},
        {"name":"HTTPCODE","type":"string"},
        {"name":"SOURCEBYTES","type":"string"},
        {"name":"RESPONSEBYTES","type":"string"},
        {"name":"ELAPSEDTIME","type":"string"},
        {"name":"DOMAIN","type":"string"},
        {"name":"BUCKET","type":"string"},
        {"name":"OBJECT","type":"string"}
        ]
    }
    """

    avro_deserializer = AvroDeserializer(schema_str,schema_registry_client)
    string_deserializer = StringDeserializer('utf_8')

    consumer_conf = {'bootstrap.servers': bootstrap_servers,
                     'key.deserializer': string_deserializer,
                     'value.deserializer': avro_deserializer,
                     'group.id': group,
                     'auto.offset.reset': "earliest"}

    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe([topic])

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            record = msg.value()
            if record is not None:
                if record['OPERATION'] == "POST" or record['OPERATION'] == "MULTIPART_COMPLETE" and record['DOMAIN'] != "%28none%29":
                    urllistraw = "http://" + record['DOMAIN'] + "/" + record['BUCKET'] + "/" + record['OBJECT']
                    urllist = urllistraw[:-1]
                    print(urllist)
                    r = requests.head(urllist)
                    headercheck = r.headers
                    contentimgstring = 'image/jpeg'
                    if contentimgstring in headercheck['Content-Type']:
                        print("here")
                        OBJECTR = record['OBJECT']
                        OBJECT = OBJECTR[:-1]
                        imagesnap(urllist,OBJECT)
                    else:
                        print("skipped")
                        continue
                    print(r.headers)
                else:
                    continue
        except KeyboardInterrupt:
            break

    consumer.close()


if __name__ == '__main__':
    main()


