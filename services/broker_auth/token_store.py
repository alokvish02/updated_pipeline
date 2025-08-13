import psycopg2
from services.broker_auth.config import FYERS_CONFIG  # Ensure this file is in your PYTHONPATH
from services.db_config import DB_HOST,DB_PORT, DB_USER,DB_PASS,DB_SCHEMA, DB_NAME_2

# Provided DB settings
DB_SETTINGS = {
    "dbname": DB_NAME_2,
    "user": DB_USER,
    "password": DB_PASS,
    "host": DB_HOST,
    "port": DB_PORT,
}


class TokenStore:
    def __init__(self, client_id, db_config):
        """
        Initialize the TokenStore with PostgreSQL connection parameters.

        db_config should be a dict containing keys such as:
        {
            "dbname": "trade",
            "user": "postgres",
            "password": "onealpha12345",
            "host": "localhost",
            "port": "5432",
        }
        """
        self.client_id = client_id
        self.db_config = db_config
        self._create_table()

    def _create_table(self):
        """Create a PostgreSQL table for storing tokens if it doesn't exist."""
        conn = psycopg2.connect(**self.db_config)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tokens (
                client_id TEXT PRIMARY KEY,
                access_token TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        cursor.close()
        conn.close()

    def save_token(self, token):
        """
        Save or update the access token in the PostgreSQL database.
        If a token already exists for the given client_id, it will be updated in the same row.
        """
        conn = psycopg2.connect(**self.db_config)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO tokens (client_id, access_token, created_at)
            VALUES (%s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (client_id) DO UPDATE SET 
                access_token = EXCLUDED.access_token,
                created_at = CURRENT_TIMESTAMP
        """, (self.client_id, token))
        conn.commit()
        cursor.close()
        conn.close()

    def load_token(self):
        """Load the access token from the PostgreSQL database for the given client_id."""
        conn = psycopg2.connect(**self.db_config)
        cursor = conn.cursor()
        cursor.execute("SELECT access_token FROM tokens WHERE client_id = %s", (self.client_id,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return result[0] if result else None


# if __name__ == "__main__":
    # # Extract the client id from the FYERS_CONFIG
    # client_id = FYERS_CONFIG.get("CLIENT_ID")
    # if not client_id:
    #     raise ValueError("CLIENT_ID not found in FYERS_CONFIG")
    #
    # token_store = TokenStore(client_id, DB_SETTINGS)
    #
    # # Save a token (replace "example_access_token" with your actual token)
    # token_store.save_token("example_access_token")
    #
    # # Load and print the token
    # token = token_store.load_token()
    # print("Loaded token:", token)
