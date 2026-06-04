#!/usr/bin/env python3
import json
import time
import logging
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ─── Configuration ────────────────────────────────────────────────────────────

API_BASE_URL     = "http://localhost:8000"
KAFKA_BOOTSTRAP  = "localhost:9092"

ENDPOINTS = [
    {
        "url":        f"{API_BASE_URL}/ventes",
        "topic":      "ventes",
        "batch_size": 10,
        "key_field":  "vente_id",
    },
    {
        "url":        f"{API_BASE_URL}/stocks",
        "topic":      "stocks",
        "batch_size": 5,
        "key_field":  "stock_id",
    },
]

POLL_INTERVAL_SEC = 10   # intervalle entre chaque appel API

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── Producer ─────────────────────────────────────────────────────────────────

def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        acks="all",
        retries=3,
    )


def fetch_data(url: str, batch_size: int) -> list[dict]:
    """Appelle l'API et retourne la liste des enregistrements."""
    try:
        resp = requests.get(url, params={"batch_size": batch_size}, timeout=10)
        resp.raise_for_status()
        payload = resp.json()
        return payload.get("data", [])
    except requests.RequestException as e:
        log.error("Erreur API [%s] : %s", url, e)
        return []


def publish_batch(producer: KafkaProducer, topic: str, records: list[dict], key_field: str):
    """Publie une liste d'enregistrements dans un topic Kafka."""
    sent = 0
    for record in records:
        key = record.get(key_field)
        future = producer.send(topic, key=key, value=record)
        try:
            future.get(timeout=10)
            sent += 1
        except KafkaError as e:
            log.error("Échec envoi vers [%s] clé=%s : %s", topic, key, e)

    if sent:
        log.info("[%s] %d/%d messages envoyés", topic, sent, len(records))


# ─── Main loop ────────────────────────────────────────────────────────────────

def main():
    log.info("Démarrage du producer Kafka...")
    log.info("Bootstrap : %s", KAFKA_BOOTSTRAP)
    log.info("API       : %s", API_BASE_URL)
    log.info("Topics    : %s", [e["topic"] for e in ENDPOINTS])

    producer = create_producer()
    log.info("Connecté à Kafka ")

    try:
        while True:
            for endpoint in ENDPOINTS:
                records = fetch_data(endpoint["url"], endpoint["batch_size"])
                if records:
                    publish_batch(
                        producer,
                        topic=endpoint["topic"],
                        records=records,
                        key_field=endpoint["key_field"],
                    )
                else:
                    log.warning("  Aucune donnée reçue de %s", endpoint["url"])

            producer.flush()
            log.info("Attente %ds avant prochain cycle...\n", POLL_INTERVAL_SEC)
            time.sleep(POLL_INTERVAL_SEC)

    except KeyboardInterrupt:
        log.info("Arrêt demandé par l'utilisateur.")
    finally:
        producer.flush()
        producer.close()
        log.info("Producer fermé proprement.")


if __name__ == "__main__":
    main()