from supabase import Client, create_client
from supabase.lib.client_options import ClientOptions
import psycopg2
from psycopg2.extras import RealDictCursor
import config

client_options = ClientOptions(postgrest_client_timeout=60)
supabase: Client = create_client(config.SUPABASE_URL, config.SUPABASE_KEY)

def execute_query(query: str,params=None):
    with psycopg2.connect(config.SUPABASE_CONNECTION_STRING) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            try:
                return cur.fetchall()
            except:
                return None

