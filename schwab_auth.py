import os
import base64
import json
import requests
import webbrowser
import time
from loguru import logger
from dotenv import load_dotenv
import tempfile, os, json

load_dotenv()

APP_KEY = os.getenv("APP_KEY")
APP_SECRET = os.getenv("APP_SECRET")
REDIRECT_URI = "https://127.0.0.1"
TOKEN_URL = "https://api.schwabapi.com/v1/oauth/token"

def construct_init_auth_url():
    auth_url = f"https://api.schwabapi.com/v1/oauth/authorize?client_id={APP_KEY}&redirect_uri={REDIRECT_URI}"
    logger.info("Visit to authenticate:")
    logger.info(auth_url)
    return APP_KEY, APP_SECRET, auth_url

def construct_headers_and_payload(returned_url, app_key, app_secret):
    try:
        response_code = returned_url[returned_url.index("code=") + 5 : returned_url.index("%40")] + "@"
    except Exception as e:
        logger.error(f"Failed to parse code from URL: {e}")
        raise

    credentials = f"{app_key}:{app_secret}"
    base64_credentials = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

    headers = {
        "Authorization": f"Basic {base64_credentials}",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    payload = {
        "grant_type": "authorization_code",
        "code": response_code,
        "redirect_uri": REDIRECT_URI,
    }

    return headers, payload

def retrieve_tokens(headers, payload):
    response = requests.post(TOKEN_URL, headers=headers, data=payload)
    try:
        return response.json()
    except Exception as e:
        logger.error(f"Failed to decode token response: {response.text}")
        return None

def main():
    _, _, auth_url = construct_init_auth_url()
    webbrowser.open(auth_url)

    logger.info("Paste returned URL:")
    returned_url = input("> ")

    headers, payload = construct_headers_and_payload(returned_url, APP_KEY, APP_SECRET)
    tokens = retrieve_tokens(headers, payload)
    tokens["issued_at"] = time.time()

    _atomic_write_token_json(tokens, "./data/token.json")

    logger.success("✅ token.json saved.")


def _atomic_write_token_json(tokens, path="./data/token.json"):
    dirpath = os.path.dirname(path) or "."
    os.makedirs(dirpath, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", dir=dirpath, delete=False) as tf:
        json.dump(tokens, tf, indent=4)
        tf.flush()
        os.fsync(tf.fileno())
        tmpname = tf.name
    os.replace(tmpname, path)

if __name__ == "__main__":
    main()
