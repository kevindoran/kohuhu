import pytest
import kohuhu.credentials



def test_load_credentials():
    passphrase = "password"
    kohuhu.credentials.load_credentials("exchanges.json.example.encrypted",
                                        decrypt_first=True,
                                        passphrase=passphrase)
    print(kohuhu.credentials.credentials)
    assert len(kohuhu.credentials.credentials) == 1
    credentials = kohuhu.credentials.credentials['independentreserve']
    assert credentials.owner == "kevin"
    assert credentials.api_key == "removed. See the encrypted version."
    assert credentials.api_secret == "removed. See the encrypted version."
    assert credentials.passphrase is None
