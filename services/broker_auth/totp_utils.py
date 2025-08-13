import pyotp

def generate_totp(secret_key):
    """Generate a Time-based OTP (TOTP) using the secret key."""
    totp = pyotp.TOTP(secret_key)
    return totp.now()




# secret_key = "FSWS3JBWFLBNG3Y4A4AHG2ZFUGTUUFBU"
# otp = generate_totp(secret_key)
# print(otp)