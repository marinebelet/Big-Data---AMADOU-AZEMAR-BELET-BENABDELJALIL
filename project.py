import json
import logging
from kafka import KafkaConsumer
from datetime import datetime
import time

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092'] 
WIKIMEDIA_TOPIC = 'wikimedia.recentchange' # The public topic name (or your intermediate topic)
ENTITIES_TO_TRACK = [
    "Interstellar", # A specific movie
    "Christopher Nolan", # A specific director/person
    "Comedy", # A genre (abstract concept)
    "English language", # An abstract concept
    "Dune (2021 film)" # Another specific movie
]
ALERT_USER = "Joe Bloggs" # Example user to alert on

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_wikimedia_event(event_data, metrics, alerts_file):
    """Processes a single event, updates metrics, and checks for alerts."""
    
    # 1. Check for tracked entities
    entity_found = False
    for entity in ENTITIES_TO_TRACK:
        # Simple check: if the entity name is in the event's title or wiki
        if entity.lower() in event_data.get('title', '').lower() or \
           entity.lower() in event_data.get('wiki', '').lower():
            entity_found = True
            
            # 2. Update simple metrics (e.g., total event count per entity)
            metrics['entity_counts'][entity] = metrics['entity_counts'].get(entity, 0) + 1
            
            # Update metric for total events tracked
            metrics['total_events'] += 1
            
            # 3. Track change type metrics
            type_key = event_data.get('type', 'other_type')
            metrics['change_types'][type_key] = metrics['change_types'].get(type_key, 0) + 1
            
            break # Process only once per event if multiple entities are matched

    # 4. Implement Alerting Logic
    user = event_data.get('user')
    if user == ALERT_USER:
        alert_message = f"ALERT: User '{ALERT_USER}' made a change to '{event_data.get('title')}' on wiki '{event_data.get('wiki')}' at {datetime.now().isoformat()}"
        
        # Mimic alerting by routing to a different file/database
        with open(alerts_file, 'a') as f:
            f.write(alert_message + "\n")
            
        metrics['total_alerts'] += 1
        logging.warning(alert_message)


def run_consumer():
    """Sets up and runs the Kafka consumer."""
    
    # Initialize metrics structure
    metrics = {
        'total_events': 0,
        'total_alerts': 0,
        'entity_counts': {}, # Counts of events per tracked entity
        'change_types': {}   # Counts of change types (e.g., edit, new, log)
    }
    
    # Define persistence files
    METRICS_FILE = 'stream_metrics.json'
    ALERTS_FILE = 'stream_alerts.log'

    logging.info(f"Connecting to Kafka topic: {WIKIMEDIA_TOPIC}")

    try:
        # Create Kafka Consumer
        consumer = KafkaConsumer(
            WIKIMEDIA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='latest', # Start reading from the latest message
            enable_auto_commit=True,
            group_id='imdb-wiki-analysis-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        logging.info("Consumer connected. Starting to process messages...")

        for message in consumer:
            try:
                event_data = message.value
                process_wikimedia_event(event_data, metrics, ALERTS_FILE)
                
                # Periodically save the metrics to a file
                if metrics['total_events'] >= 1: 
                    with open(METRICS_FILE, 'w') as f:
                        json.dump(metrics, f, indent=4)
                    logging.info(f"Metrics saved. Total events processed: {metrics['total_events']}")
                
            except json.JSONDecodeError as e:
                logging.error(f"Could not decode JSON: {e}")
            except Exception as e:
                logging.error(f"An unexpected error occurred: {e}")

    except Exception as e:
        logging.critical(f"Failed to initialize or run consumer: {e}")

if __name__ == '__main__':
    run_consumer()