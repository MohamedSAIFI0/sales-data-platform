"""
Cassandra connector pour la plateforme de ventes.
Cassandra tourne dans Docker — adapter host/port si besoin.
"""
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
import logging

logger = logging.getLogger(__name__)

CASSANDRA_HOST = "localhost"  
CASSANDRA_PORT = 9042
KEYSPACE = "ventes_platform"


def get_session():
    """Retourne une session Cassandra connectée."""
    try:
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect(KEYSPACE)
        logger.info("✅ Connecté à Cassandra")
        return session, cluster
    except Exception as e:
        logger.error(f"❌ Erreur connexion Cassandra: {e}")
        raise


def load_ventes(session) -> pd.DataFrame:
    rows = session.execute("SELECT * FROM ventes")
    return pd.DataFrame(list(rows))


def load_clients(session) -> pd.DataFrame:
    rows = session.execute("SELECT * FROM clients")
    return pd.DataFrame(list(rows))


def load_produits(session) -> pd.DataFrame:
    rows = session.execute("SELECT * FROM produits")
    return pd.DataFrame(list(rows))


def load_canaux(session) -> pd.DataFrame:
    rows = session.execute("SELECT * FROM canaux_vente")
    return pd.DataFrame(list(rows))


def load_retours(session) -> pd.DataFrame:
    rows = session.execute("SELECT * FROM retours")
    return pd.DataFrame(list(rows))


def load_stocks(session) -> pd.DataFrame:
    rows = session.execute("SELECT * FROM stocks")
    return pd.DataFrame(list(rows))


def load_all_data(session) -> dict:
    """Charge toutes les tables et retourne un dictionnaire de DataFrames."""
    return {
        "ventes":   load_ventes(session),
        "clients":  load_clients(session),
        "produits": load_produits(session),
        "canaux":   load_canaux(session),
        "retours":  load_retours(session),
        "stocks":   load_stocks(session),
    }