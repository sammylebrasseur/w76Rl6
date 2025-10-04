from supabase import Client, create_client
import os
from supabase.lib.client_options import ClientOptions
import psycopg2
from psycopg2.extras import RealDictCursor

connection_string = 'postgresql://postgres.glyxdcmwlaufznnfxfan:Ca2024$al26@aws-0-us-east-1.pooler.supabase.com:5432/postgres'
url: str = 'https://glyxdcmwlaufznnfxfan.supabase.co'
key: str = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImdseXhkY213bGF1ZnpubmZ4ZmFuIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MzQxNTU4NSwiZXhwIjoyMDY4OTkxNTg1fQ.b1H2gpg28kEJfV9r5_Xk2TZZqy3ezNMo0oQjpEcuSB0'
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

