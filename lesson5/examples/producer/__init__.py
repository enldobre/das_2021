import socket

from lesson5.examples.producer.producer import send,read_data


def main():
    """Entry point for the application script"""
    path = "/training/data_academy_spark/lesson5/examples/resources/sample_logs.txt"
    logs = read_data(path)
    conf = {'bootstrap.servers': "localhost:9092", 'client.id': socket.gethostname()}
    topic = "sample"
    print("running kafka producer")
    send(conf, topic, logs)
    print("finished")


if __name__ == '__main__':
    main()
