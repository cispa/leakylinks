import logging
import requests
from config.settings import DISCORD_WEBHOOK_URL

def send_discord_alert(message):
    """Send a message to Discord webhook if configured."""
    webhook_url = DISCORD_WEBHOOK_URL
    if not webhook_url:
        return
    
    try:
        data = {"content": message}
        response = requests.post(webhook_url, json=data)
        if response.status_code != 204:
            logging.error(f"Failed to send Discord alert: {response.text}")
    except Exception as e:
        logging.error(f"Error sending Discord alert: {e}")

def urlscan_login(base_url, email, password):
    login_url = f"{base_url}/user/login/"
    session = requests.Session()  # Use a session object to persist cookies across requests

    # Prepare headers and payload for the login request
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.6613.120 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Origin': base_url,
        'Referer': f'{base_url}/user/login/',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-User': '?1',
        'Sec-Fetch-Dest': 'document'
    }

    data = {
        'email': email,
        'password': password
    }

    # Send the POST request to login
    response = session.post(login_url, headers=headers, data=data)

    # Check if login was successful
    if response.ok:
        print("Login successful")
    else:
        print("Login failed with status code:", response.status_code)

    # Return the session to use its cookie for further requests
    return session

