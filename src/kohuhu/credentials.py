import json
import getpass
import argparse
import argcomplete
import base64
from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

default_credential_file_encrypted = 'exchanges.json.encrypted'
default_credential_file = 'exchanges.json'

credentials = {}

class ApiCredentials:
    def __init__(self, ccxt_id, owner, key, secret, passphrase, url):
        self.ccxt_id = ccxt_id
        self.owner = owner
        self.api_key = key
        self.api_secret = secret
        self.passphrase = passphrase
        self.api_url = url

    def apply(self, exchange):
        exchange.apiKey = self.api_key
        exchange.secret = self.api_secret
        if self.passphrase:
            exchange.password = self.passphrase
        if self.api_url:
            exchange.urls['api'] = self.api_url


def authorize(exchange, is_sandbox=False):
    # Inspired from: https://github.com/ccxt/ccxt/issues/369
    id = exchange.id + "_sandbox" if is_sandbox else exchange.id
    if id in credentials:
        credentials[id].apply(exchange)
    else:
        print("No credentials for exchange id {}.".format(id))


def load_credentials(credential_file=default_credential_file,
                     decrypt_first=False, passphrase=None):
    if decrypt_first:
        if not passphrase:
            passphrase = prompt_for_passphrase()
        with open(credential_file, 'rb') as f:
            encrypted_data = f.read()
        input = decrypt(encrypted_data, passphrase)
    else:
        with open(credential_file, 'r') as f:
            input = f.read()
    credential_list = parse_credentials(input)
    # Convert to a map.
    global credentials
    credentials = {c.ccxt_id: c for c in credential_list}
    print(str(credentials))


def parse_credentials(input):
        credentials = json.JSONDecoder(object_hook=as_credential).decode(input)
        return credentials['exchanges']


def as_credential(dct):
    if 'ccxt_id' in dct:
        return ApiCredentials(dct['ccxt_id'], dct.get('owner', None),
                              dct['api_key'], dct['api_secret'],
                              dct.get('passphrase', None),
                              dct.get('api_url', None))
    return dct


def key_from_passphrase(passphrase):
    salt = b'\xbf\xcc\x80\xfdv\xafJ\x19\xecN\xbb\xd0\xb1\xd4gW'
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt,
                     iterations=100000, backend=default_backend())
    key = base64.urlsafe_b64encode(kdf.derive(passphrase.encode()))
    return key


def encrypt(input, passphrase):
    f = Fernet(key_from_passphrase(passphrase))
    token = f.encrypt(input.encode())
    return token


def decrypt(input, passphrase):
    f = Fernet(key_from_passphrase(passphrase))
    output = f.decrypt(input).decode()
    return output

def prompt_for_passphrase():
    passphrase = getpass.getpass(prompt="Passphrase (for exchange file):")
    passphrase_again = getpass.getpass(prompt="Passphrase confirm:")
    if passphrase != passphrase_again:
        print("Passphrases do not match. Exiting.")
        exit(1)
    return passphrase


def main():
    parser = argparse.ArgumentParser(description="Encrypt/decrypt exchange "
                                                 "file.")
    parser.add_argument("file", help="path to the exchange file", type=str)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-e", "--encrypt", help="encrypt the file",
                        action="store_true")
    group.add_argument("-d", "--decrypt", help="decrypt the file",
                        action="store_true")
    parser.add_argument("-o", "--output", help="encrypted or decrypted output",
                        type=str, default="out")
    # Argcomplete allows for terminal tab completion.
    argcomplete.autocomplete(parser)
    args = parser.parse_args()

    passphrase = prompt_for_passphrase()

    if args.encrypt:
        with open(args.file) as input_file:
            input = input_file.read()
        output = encrypt(input, passphrase)
        with open(args.output, "wb") as output_file:
            output_file.write(output)
    else:
        with open(args.file, "rb") as input_file:
            input = input_file.read()
        output = decrypt(input, passphrase)
        with open(args.output, "w") as output_file:
            output_file.write(output)

if __name__ == "__main__":
    main()

