from confluent_kafka import Producer


def send(conf, topic, messages):
    # initialize a producer
    producer = Producer(conf)
    # send a message to a  topic using a key and a string value
    for message in messages:
        producer.produce(topic, key="key", value=message, callback=acked)
        producer.poll(1)
    producer.flush()


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))


def read_data(path):
    my_file = open(path, "r")
    content = my_file.read()
    content_list = content.splitlines()
    my_file.close()
    return content_list
