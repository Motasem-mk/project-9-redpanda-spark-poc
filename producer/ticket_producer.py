import json
import random
import time
import logging
import os
from datetime import datetime
from faker import Faker
from confluent_kafka import Producer

# Configure logging to see our progress in the Docker console
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("ticket_producer")

logger.info("Starting Ticket Producer with Hybrid Sync/Async strategy")

fake = Faker()

# Look for the environment variable defined in docker-compose.yml
# This allows the script to be portable between Docker and Local testing
broker = os.getenv('KAFKA_BROKER', 'redpanda:9092')

conf = {
    'bootstrap.servers': broker,
    'client.id': 'ticket-producer',
    'client.dns.lookup': 'use_all_dns_ips'
}

# Initialize the Producer
producer = Producer(conf)

REQUEST_TYPES = ['technical', 'billing', 'account', 'general']
PRIORITIES = ['low', 'medium', 'high']

def generate_ticket():
    """Generates a fake customer support ticket."""
    return {
        "ticket_id": fake.uuid4(),
        "client_id": random.randint(1000, 9999),
        "created_at": datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
        "request": fake.sentence(nb_words=6),
        "request_type": random.choice(REQUEST_TYPES),
        "priority": random.choice(PRIORITIES)
    }

def delivery_report(err, msg):
    """
    This is our FEEDBACK mechanism. 
    It is called once the background thread receives an ACK from Redpanda.
    """
    if err is not None:
        logger.error(f"❌ Delivery failed: {err}")
    else:
        logger.info(f"✅ Confirmed: {msg.topic()} [Part: {msg.partition()}] Offset: {msg.offset()}")

# --- MAIN LOOP ---
ticket_counter = 0
try:
    while True:
        ticket = generate_ticket()
        ticket_counter += 1

        # 1. ASYNC STEP: produce() is non-blocking.
        # It puts the message in a local buffer and returns immediately.
        # This keeps our app fast.
        producer.produce(
            topic='client_tickets',
            key=ticket['ticket_id'], # Key helps with partitioning
            value=json.dumps(ticket),
            callback=delivery_report
        )

        # 2. SYNC-ISH STEP: poll(0).
        # This tells the producer to check the 'outbox' for any 
        # delivery reports (receipts) waiting to be processed.
        # It triggers the delivery_report function above.
        producer.poll(0) 

        logger.info(f"📤 Sent Ticket #{ticket_counter}: {ticket['ticket_id']} ({ticket['priority']})")

        time.sleep(2)

except KeyboardInterrupt:
    logger.info("Stopping script...")

finally:
    # 3. FULL SYNC STEP: flush().
    # Before the program exits, we block and wait for all messages in the 
    # buffer to be delivered. This ensures NO DATA LOSS on shutdown.
    logger.info("Cleaning up... Flushing final messages to Redpanda.")
    producer.flush(timeout=10)
    logger.info("Producer shut down cleanly.")



####################


# import json
# import random
# import time
# import logging
# from datetime import datetime
# from faker import Faker
# from confluent_kafka import Producer

# # Configure logging
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s"
# )
# logger = logging.getLogger("ticket_producer")

# logger.info("Running latest version of producer script")

# # Faker and Kafka setup
# fake = Faker()

# conf = {
#     'bootstrap.servers': 'redpanda:9092',
#     'client.id': 'ticket-producer',
#     'client.dns.lookup': 'use_all_dns_ips'
# }
# producer = Producer(conf)

# REQUEST_TYPES = ['technical', 'billing', 'account', 'general']
# PRIORITIES = ['low', 'medium', 'high']


# # Generate a fake ticket

# def generate_ticket():
#     return {
#         "ticket_id": fake.uuid4(),
#         "client_id": random.randint(1000, 9999),
#         "created_at": datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
#         "request": fake.sentence(nb_words=6),
#         "request_type": random.choice(REQUEST_TYPES),
#         "priority": random.choice(PRIORITIES)
#     }

# # Kafka delivery callback

# def delivery_report(err, msg):
#     if err is not None:
#         logger.error(f"Delivery failed: {err}")
#     else:
#         logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

# # Main loop

# ticket_counter = 0
# while True:
#     ticket = generate_ticket()
#     ticket_counter += 1

#     producer.produce(
#         topic='client_tickets',
#         value=json.dumps(ticket),
#         callback=delivery_report
#     )
#     producer.poll(0)  # Ensure delivery callbacks are triggered

#     logger.info(
#     f"Ticket #{ticket_counter} sent:\n"
#     f"ticket_id={ticket['ticket_id']},\n"
#     f"client_id={ticket['client_id']},\n"
#     f"created_at={ticket['created_at']},\n"
#     f"request={ticket['request']},\n"
#     f"request_type={ticket['request_type']},\n"
#     f"priority={ticket['priority']}"
#     )


#     time.sleep(2)


