import argparse
import atexit
import logging
import redis

from kafka import KafkaConsumer


logger_format = "%(asctime)s - %(message)s"  #set logging format
logging.basicConfig(format = logger_format)
logger = logging.getLogger('redis-publisher')  #save all log at 'data-producer'
logger.setLevel(logging.DEBUG)


def shutdown_hook(kafka_consumer):
	"""
	shutdown hook before system down
	"""
	try:
		kafka_consumer.close(10)
	except Exception as e:
		logger.warn('Failed to close kafka consumer connection because of: %s', e)


if __name__ == '__main__':

	#setup command line arguments
	parser = argparse.ArgumentParser()
	parser.add_argument('topic_name')
	parser.add_argument('kafka_broker')
	parser.add_argument('redis_channel')
	parser.add_argument('redis_host')
	parser.add_argument('redis_port')

	#Parse arguments
	args = parser.parse_args()
	topic_name = args.topic_name
	kafka_broker = args.kafka_broker
	redis_channel = args.redis_channel
	redis_host = args.redis_host
	redis_port = args.redis_port

	#Instantiate a simple kafka consumer.
	kafka_consumer = KafkaConsumer(topic_name, bootstrap_servers = kafka_broker)

	#Instantiate a redis client
	redis_client = redis.StrictRedis(host=redis_host, port = redis_port, db = 0 )

	#Setup Proper Shutdown Hook
	atexit.register(shutdown_hook, kafka_consumer)

	for msg in kafka_consumer:
		logger.debug('Received date from kafka %s', msg)
		redis_client.publish(redis_channel, msg.value)





