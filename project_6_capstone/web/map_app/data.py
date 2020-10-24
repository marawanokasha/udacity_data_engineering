import cassandra
import pandas as pd
from cassandra.cluster import Cluster, Session
from config import CASSANDRA_HOST, CASSANDRA_PORT, CASSANDRA_KEYSPACE


def get_cassandra_session() -> Session:
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect()
    session.set_keyspace(CASSANDRA_KEYSPACE)

    return session


def get_immigration_stats(session) -> pd.DataFrame:
    result = session.execute("""
        SELECT country_code_iso_2, COUNT(*) AS count
        FROM immigration_stats
        GROUP BY country_code_iso_2
    """)
    return pd.DataFrame(result)


def get_countries(session) -> pd.DataFrame:
    result = session.execute("""
        SELECT country_code, country_name
        FROM country
    """)
    return pd.DataFrame(result)


def get_states(session) -> pd.DataFrame:
    result = session.execute("""
        SELECT state_code, state_name
        FROM state
    """)
    return pd.DataFrame(result)


def get_visa_types(session) -> pd.DataFrame:
    result = session.execute("""
        SELECT visa_type
        FROM visa_type
    """)
    return pd.DataFrame(result)
