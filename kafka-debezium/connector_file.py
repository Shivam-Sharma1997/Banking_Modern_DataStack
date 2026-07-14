import os
import json
import requests
from dotenv import load_dotenv

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()

# -----------------------------
# Build connector JSON in memory
# -----------------------------
connector_config = {
    "name": "postgres-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": os.getenv("DEBEZIUM_HOST","postgres"),
        "database.port": os.getenv("DEBEZIUM_PORT","5432"),
        "database.user": os.getenv("POSTGRES_USER","postgres"),
        "database.password": os.getenv("POSTGRES__PASSWORD","postgres"),
        "database.dbname": os.getenv("POSTGRES__DB","banking"),
        "topic.prefix": "banking_server",
        "table.include.list": "public.customers,public.accounts,public.transactions",
        "plugin.name": "pgoutput",
        "slot.name": "banking_slot",
        "publication.autocreate.mode": "filtered",
        "tombstones.on.delete": "false",
        "decimal.handling.mode": "double",
    },
}
print("Debezium host:", os.getenv("DEBEZIUM_POSTGRES_HOST", "postgres"))
print("Debezium port:", os.getenv("DEBEZIUM_POSTGRES_PORT", "5432"))
print("DB name:", os.getenv("POSTGRES_DB"))
print("DB user:", os.getenv("POSTGRES_USER"))
# -----------------------------
# Send request to Debezium Connect
# -----------------------------
url = "http://localhost:8083/connectors"
headers = {"Content-Type": "application/json"}

response = requests.post(url, headers=headers, data=json.dumps(connector_config))

# -----------------------------
# Debug/Output
# -----------------------------
if response.status_code == 201:
    print("Connector created successfully!")
elif response.status_code == 409:
    print("Connector already exists.")
else:
    print(f"Failed to create connector ({response.status_code}): {response.text}")