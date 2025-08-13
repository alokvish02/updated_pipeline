from urllib.parse import urlparse, parse_qs
import pyotp
import requests
import base64
from fyers_apiv3 import fyersModel
import time
from binance.client import Client
from services.broker_auth.config import FYERS_CONFIG, BINANCE_CONFIG
from services.broker_auth.totp_utils import generate_totp
from services.broker_auth.token_store import TokenStore
from services.db_config import DB_HOST,DB_PORT, DB_NAME_2,DB_USER,DB_PASS,DB_SCHEMA

# Provided DB settings to be used with TokenStore
DB_SETTINGS = {
    "dbname": DB_NAME_2,
    "user": DB_USER,
    "password": DB_PASS,
    "host": DB_HOST,
    "port": DB_PORT,
}


class FyersAuth:
    def __init__(self):
        """Initialize FyersAuth with configuration and token storage."""
        self.config = FYERS_CONFIG
        # Use the CLIENT_ID from FYERS_CONFIG for token storage
        self.token_store = TokenStore(self.config.get("CLIENT_ID"), DB_SETTINGS)

    def get_encoded_string(self, string):
        """Encode a string in base64 format."""
        return base64.b64encode(str(string).encode("ascii")).decode("ascii")

    def authenticate(self):
        """Authenticate with Fyers API and return the access token."""
        # Load credentials from config
        fy_id = self.config["FY_ID"]
        totp_key = self.config["TOTP_KEY"]
        pin = self.config["PIN"]
        client_id = self.config["CLIENT_ID"]
        secret_key = self.config["SECRET_KEY"]
        redirect_uri = self.config["REDIRECT_URI"]

        # Step 1: Send Login OTP
        URL_SEND_LOGIN_OTP = "https://api-t2.fyers.in/vagator/v2/send_login_otp_v2"
        res = requests.post(URL_SEND_LOGIN_OTP, json={
            "fy_id": self.get_encoded_string(fy_id),
            "app_id": "2"
        }).json()

        if "request_key" not in res:
            raise Exception(f"Failed to send login OTP: {res}")

        # Wait to ensure TOTP sync
        if time.time() % 30 > 27:
            time.sleep(5)

        # Step 2: Verify OTP
        URL_VERIFY_OTP = "https://api-t2.fyers.in/vagator/v2/verify_otp"
        otp_code = pyotp.TOTP(totp_key).now()
        res2 = requests.post(URL_VERIFY_OTP, json={
            "request_key": res["request_key"],
            "otp": otp_code
        }).json()

        if "request_key" not in res2:
            raise Exception(f"OTP verification failed: {res2}")

        # Step 3: Verify PIN
        ses = requests.Session()
        URL_VERIFY_PIN = "https://api-t2.fyers.in/vagator/v2/verify_pin_v2"
        payload2 = {
            "request_key": res2["request_key"],
            "identity_type": "pin",
            "identifier": self.get_encoded_string(pin)
        }
        res3 = ses.post(URL_VERIFY_PIN, json=payload2).json()

        if "data" not in res3 or "access_token" not in res3["data"]:
            raise Exception(f"PIN verification failed: {res3}")

        ses.headers.update({'authorization': f"Bearer {res3['data']['access_token']}"})

        # Step 4: Retrieve Auth Code
        TOKEN_URL = "https://api-t1.fyers.in/api/v3/token"
        payload3 = {
            "fyers_id": fy_id,
            "app_id": client_id[:-4],
            "redirect_uri": redirect_uri,
            "appType": "100",
            "code_challenge": "",
            "state": "None",
            "scope": "",
            "nonce": "",
            "response_type": "code",
            "create_cookie": True
        }
        res4 = ses.post(TOKEN_URL, json=payload3).json()

        if "Url" not in res4:
            raise Exception(f"Failed to retrieve auth code: {res4}")

        # Extract auth code from URL
        url = res4["Url"]
        parsed = urlparse(url)
        auth_code = parse_qs(parsed.query).get("auth_code", [None])[0]

        if not auth_code:
            raise Exception("Auth code not found in response URL.")

        # Step 5: Generate Final Access Token
        session = fyersModel.SessionModel(
            client_id=client_id,
            secret_key=secret_key,
            redirect_uri=redirect_uri,
            response_type="code",
            grant_type="authorization_code"
        )
        session.set_token(auth_code)
        response = session.generate_token()

        if "access_token" not in response:
            raise Exception(f"Failed to generate access token: {response}")

        access_token = response["access_token"]
        # print("Access token.", access_token)
        # Save access token for future use
        self.token_store.save_token(access_token)

        return access_token


class BinanceAuth:
    def __init__(self):
        self.config = BINANCE_CONFIG
        # Initialize TokenStore for Binance using a unique key
        self.token_store = TokenStore("binance", DB_SETTINGS)

    def authenticate(self):
        """Authenticate with Binance API."""
        api_key = self.config.get("API_KEY")
        api_secret = self.config.get("API_SECRET")
        private_key = self.config.get("PRIVATE_KEY")
        private_key_pass = self.config.get("PRIVATE_KEY_PASS")

        try:
            if private_key:
                # RSA Authentication
                with open(private_key, 'rb') as f:
                    private_key_content = f.read()

                client = Client(api_key=api_key, private_key=private_key_content, private_key_pass=private_key_pass)
            else:
                # HMAC Authentication (API Key & Secret)
                client = Client(api_key, api_secret)

            # Test API access
            account_info = client.get_account()
            # Optionally, store any relevant token or account info using token_store
            return account_info

        except Exception as e:
            raise Exception(f"Binance Authentication Failed: {e}")
