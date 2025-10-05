from supabase import Client, create_client
import os
from supabase.lib.client_options import ClientOptions
import psycopg2
from psycopg2.extras import RealDictCursor
import dotenv
import os
dotenv.load_dotenv()
connection_string = os.getenv('SUPABASE_CONNECTION_STRING')
url = os.getenv('SUPABASE_URL')
key = os.getenv('SUPABASE_KEY')
client_options = ClientOptions(postgrest_client_timeout=60)
supabase: Client = create_client(url, key)

def execute_query(query: str,params=None):
    with psycopg2.connect(connection_string) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            try:
                return cur.fetchall()
            except:
                return None

