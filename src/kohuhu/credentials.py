import json
import logging
import kohuhu.encryption as encryption

default_credential_file_encrypted = 'api_credentials.json.encrypted'
default_credential_file = 'api_credentials.json'

_credentials = []

class ApiCredentials:
    def __init__(self, ccxt_id, owner, key, secret, passphrase, url):
        self.ccxt_id = ccxt_id
        self.owner = owner
        self.api_key = key
        self.api_secret = secret
        self.passphrase = passphrase
        self.api_url = url

    def authorize(self, exchange):
        exchange.apiKey = self.api_key
        exchange.secret = self.api_secret
        if self.passphrase:
            exchange.password = self.passphrase
        if self.api_url:
            exchange.urls['api'] = self.api_url


def credentials_for(exchange_id, owner=None):
    cred = None
    for c in _credentials:
        if c.ccxt_id == exchange_id and (owner is None or c.owner == owner):
            cred = c
    if cred:
        logging.info("Found credentials for exchange ({}) and owner({})."
                     .format(exchange_id, owner if owner else "any"))
    else:
        logging.info("No credentials for exchange ({}) and owner({}).\n"
                     .format(exchange_id, owner if owner else "any") +
                     "Available credentials are: " + ",".join("({}:{})"
                     .format(c.ccxt_id, c.owner) for c in _credentials))
    return cred


def load_credentials(credential_file=default_credential_file,
                     decrypt_first=False, passphrase=None):
    if decrypt_first:
        if not passphrase:
            passphrase = encryption.prompt_for_passphrase()
        with open(credential_file, 'rb') as f:
            encrypted_data = f.read()
        input = encryption.decrypt(encrypted_data, passphrase)
    else:
        with open(credential_file, 'r') as f:
            input = f.read()
    global _credentials
    _credentials = parse_credentials(input)
    logging.info("Loaded credentials (exchange:owner): {}.".format(",".join(
        ("({}, {})".format(c.ccxt_id, c.owner) for c in _credentials))))


def parse_credentials(input):
        credentials = json.JSONDecoder(object_hook=as_credential).decode(input)
        return credentials['exchanges']


def as_credential(dct):
    if 'ccxt_id' in dct:
        logging.info("Found cred id: {}".format(dct['ccxt_id']))
        return ApiCredentials(dct['ccxt_id'], dct.get('owner', None),
                              dct['api_key'], dct['api_secret'],
                              dct.get('passphrase', None),
                              dct.get('api_url', None))
    return dct


