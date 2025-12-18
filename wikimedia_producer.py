import json
import logging
from kafka import KafkaProducer
import requests # Nécessaire pour la connexion HTTP au stream
from sseclient import SSEClient # Nécessaire pour lire les événements Server-Sent Events

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092'] 
WIKIMEDIA_TOPIC = 'wikimedia.recentchange' # Topic créé sur votre broker
WIKIMEDIA_STREAM_URL = 'https://stream.wikimedia.org/v2/stream/recentchange'

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_producer():
    """
    Se connecte au flux d'événements Wikimedia et produit les messages vers Kafka.
    """
    try:
        # Initialiser le producteur Kafka
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        logging.info(f"Connecting to Wikimedia EventStream at: {WIKIMEDIA_STREAM_URL}")
        logging.info(f"Producing to Kafka topic: {WIKIMEDIA_TOPIC}")

        # --- FIX CRITIQUE POUR ÉVITER LE 403 ---
        # L'ajout d'un User-Agent est obligatoire pour l'API Wikimedia.
        headers = {
            # Utilisez un nom d'application et un contact (ici l'email de soumission du projet)
            'User-Agent': 'BigDataProject/1.0 (Contact: joe@adaltas.com)' 
        }
        
        # Connexion au flux SSE (Server-Sent Events) de Wikimedia
        # On passe les headers à la requête HTTP
        messages = SSEClient(WIKIMEDIA_STREAM_URL, headers=headers)
        
        for event in messages:
            if event.data:
                try:
                    event_data = json.loads(event.data)
                    
                    # Produire le message JSON vers Kafka
                    producer.send(WIKIMEDIA_TOPIC, value=event_data)
                    
                    # On utilise INFO ici pour voir les événements produits
                    logging.info(f"Produced event: {event_data.get('title')} on {event_data.get('wiki')}")
                    
                except json.JSONDecodeError:
                    logging.warning("Received non-JSON data, skipping.")
                except Exception as e:
                    logging.error(f"Error sending message to Kafka: {e}")
            
    except requests.exceptions.ConnectionError as e:
        logging.critical(f"Connection error to Wikimedia stream: {e}. Ensure stable internet.")
    except Exception as e:
        # Afficher une erreur plus claire si le 403 persiste
        if '403 Client Error' in str(e):
             logging.critical("Failed to connect to Wikimedia stream due to 403 Forbidden. The server likely requires a User-Agent header.")
        else:
            logging.critical(f"Failed to initialize or run producer: {e}")

if __name__ == '__main__':
    run_producer()