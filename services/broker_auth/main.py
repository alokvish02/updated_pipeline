from services.broker_auth.auth_controller import FyersAuth, BinanceAuth, FyersAuth
from fyers_apiv3 import fyersModel
from services.broker_auth.config import FYERS_CONFIG


def get_profile(fyers):
    """Fetch and return user profile"""
    response = fyers.get_profile()
    return response

def Fyers_Auth():
    """Main function to authenticate with Fyers and Binance."""

    # print("Authenticating Fyers...")
    fyers_auth = FyersAuth()
    access_token = fyers_auth.authenticate()
    # print(f"✅ Access Token: {access_token}") 

    # Initialize Fyers API Client
    client_id = FYERS_CONFIG["CLIENT_ID"]
    fyers = fyersModel.FyersModel(client_id=client_id, token=access_token, log_path="")

    # Fetch and print user profile
    profile = get_profile(fyers)
    print("\n📝 Fyers Profile Data:")
    print(profile)

    # print("Authenticating Binance...")
    # binance_auth = BinanceAuth()
    # binance_account_info = binance_auth.authenticate()
    # print(f"Binance Authentication Successful {binance_account_info}")

# if __name__ == "__main__":
#     Fyers_Auth()

