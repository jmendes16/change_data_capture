import json
from datetime import datetime
from kafka import KafkaConsumer

# --- Configuration ---
KAFKA_TOPIC = 'employees_changes'
KAFKA_BROKER = 'localhost:9092'
LOG_FILE = 'employee_changes.log'
CONSUMER_GROUP_ID = 'log-archiver-group'

def main():
    """
    Consumes messages from a Kafka topic and writes them to a log file.
    """
    # --- Kafka Consumer Setup ---
    # Automatically deserializes JSON messages from bytes to a Python dict.
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest', # Start reading at the earliest message
        group_id=CONSUMER_GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

    print(f"Consumer started. Listening for messages on topic '{KAFKA_TOPIC}'...")
    print(f"Logging events to '{LOG_FILE}'")

    try:
        # The consumer is an iterable that blocks until a new message arrives.
        for message in consumer:
            # The message.value is now a Python dictionary thanks to the deserializer
            event_data = message.value

            # --- Format the log entry ---
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3] # Timestamp with milliseconds
            log_level = "INFO"
            
            # Convert the event data back to a compact JSON string for the log message
            log_message = json.dumps(event_data)
            
            log_line = f"[{timestamp}] [{log_level}] - {log_message}\n"

            # --- Write to the log file ---
            try:
                with open(LOG_FILE, 'a') as f:
                    f.write(log_line)
                
                # Also print to console to show real-time activity
                print(f"Successfully logged event: {log_message}")

            except IOError as e:
                print(f"Error: Could not write to log file {LOG_FILE}: {e}")

    except KeyboardInterrupt:
        print("Consumer stopped by user.")
    finally:
        # Cleanly close the consumer connection
        consumer.close()
        print("Consumer connection closed.")


if __name__ == "__main__":
    main()