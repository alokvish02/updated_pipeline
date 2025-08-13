import requests
from datetime import datetime


def details():
    bot_token = '7211231945:AAFuqHDGXzGUUNgPCOnAt2v4gN0osSYOquA'
    # chat_id = -1002376181102
    chat_id = 7815127633
    return bot_token, chat_id

def send_message(message_text):
    # URL for the Telegram API
    headers = details()
    current_time = datetime.now().strftime('%d-%m-%Y %H:%M:%S')


    url = f'https://api.telegram.org/bot{headers[0]}/sendMessage'
    print("url", url)
    # Payload for the POST request
    payload = {
        'chat_id': headers[1],
        'text': f'{message_text}.'
        # 'text': f'{message_text}. \ncurrent_time: {current_time}.'
    }

    # Sending the message
    response = requests.post(url, json=payload)

    # Checking if the message was sent successfully
    if response.status_code == 200:
        print("Message sent successfully!")
    else:
        print(f"Failed to send message. Status code: {response.status_code}")
        print(response.text)

# send_message(message_text= 'Hello, this is a test message from my bot!')



