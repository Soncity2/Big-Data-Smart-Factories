# consumer/run_consumers.py
import threading
from consumer.consumer import KafkaConsumerWorker

def start_consumer(topic, group_id, consumer_name):
    consumer_worker = KafkaConsumerWorker(topic, group_id, consumer_name)
    consumer_worker.run()

if __name__ == '__main__':
    # Topics for each company
    company_a_topic = 'company_A_topic'
    company_b_topic = 'company_B_topic'
    
    # Use separate consumer groups for each company to ensure load balancing
    company_a_group = 'company_A_group'
    company_b_group = 'company_B_group'
    
    # Define four consumers (2 per company)
    consumers = [
        {'topic': company_a_topic, 'group_id': company_a_group, 'consumer_name': 'Consumer_A1'},
        {'topic': company_a_topic, 'group_id': company_a_group, 'consumer_name': 'Consumer_A2'},
        {'topic': company_b_topic, 'group_id': company_b_group, 'consumer_name': 'Consumer_B1'},
        {'topic': company_b_topic, 'group_id': company_b_group, 'consumer_name': 'Consumer_B2'},
    ]
    
    threads = []
    for consumer_info in consumers:
        t = threading.Thread(target=start_consumer, kwargs=consumer_info)
        t.start()
        threads.append(t)
    
    for t in threads:
        t.join()
