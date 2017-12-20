import getpass
import argparse
import argcomplete
import base64
from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

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


def prompt_for_passphrase(confirm=False):
    passphrase = getpass.getpass(prompt="Passphrase (for exchange file):")
    if confirm:
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


    if args.encrypt:
        passphrase = prompt_for_passphrase(confirm=True)
        with open(args.file) as input_file:
            input = input_file.read()
        output = encrypt(input, passphrase)
        with open(args.output, "wb") as output_file:
            output_file.write(output)
    else:
        passphrase = prompt_for_passphrase()
        with open(args.file, "rb") as input_file:
            input = input_file.read()
        output = decrypt(input, passphrase)
        with open(args.output, "w") as output_file:
            output_file.write(output)


if __name__ == "__main__":
    main()