import psycopg2
import psycopg2.extensions
import select
import json
from kafka import KafkaProducer

# --- Configuration ---
KAFKA_TOPIC = 'employees_changes'
KAFKA_BROKER = 'kafka:9092'
DB_CONN_STRING = "dbname='cdc_db' user='engineer' password='engie_pass' host='postgres'"

# --- Kafka Producer Setup ---
# Handles JSON serialization
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_employee_details(cursor, employee_id):
    """Queries the database to get the full details for a given employee_id."""
    try:
        cursor.execute("SELECT * FROM employees WHERE employee_id = %s;", (employee_id,))
        # Fetch column names from the cursor description
        colnames = [desc[0] for desc in cursor.description]
        row = cursor.fetchone()
        if row:
            # Return data as a dictionary
            return dict(zip(colnames, row))
    except Exception as e:
        print(f"Error fetching details for employee_id {employee_id}: {e}")
    return None

def main():
    conn = psycopg2.connect(DB_CONN_STRING)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()

    # Start listening on the 'employee_changes' channel
    cursor.execute("LISTEN employee_changes;")
    print("Listener started. Waiting for notifications...")

    while True:
        # select() waits until the database connection has data to be read
        if select.select([conn], [], [], 5) == ([], [], []):
            # Timeout, no notifications
            continue
        
        # Pull pending notifications from the connection
        conn.poll()
        while conn.notifies:
            notify = conn.notifies.pop(0)
            print(f"Received notification: {notify.payload}")

            # The payload is a JSON string from the trigger
            payload = json.loads(notify.payload)
            employee_id = payload.get('employee_id')
            operation = payload.get('operation')
            
            event_data = {
                'operation': operation,
                'data': None
            }

            # If the operation was a DELETE, we can't fetch the data anymore.
            # In a real system, the trigger payload might include the full OLD row.
            if operation != 'DELETE':
                # Callback to the DB to get the full row data
                full_row_data = fetch_employee_details(cursor, employee_id)
                event_data['data'] = full_row_data
            else:
                # For DELETE, we only know the ID
                event_data['data'] = {'employee_id': employee_id}

            # Send the enriched event to Kafka
            print(f"Sending to Kafka: {event_data}")
            producer.send(KAFKA_TOPIC, value=event_data)
            producer.flush()

if __name__ == "__main__":
    main()