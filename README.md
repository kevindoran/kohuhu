# kohuhu

Decrypt the exchange API information via:
.. 

    python ./src/kohuhu/encryption.py --decrypt api_credentials.json.encrypted

Encrypt it again via:
..

    python ./src/kohuhu/encryption.py --encrypt api_credentials.json -o=api_credentials.json.encrypted
    
If you get sick of bash not auto-completing Python scripts, 
from the Python env, run:
..

    activate-global-python-argcomplete --user
   
   
The directory structure for src and test code following the 
reccomendations here:

https://docs.pytest.org/en/latest/goodpractices.html

## Testing
pytest is the framework used used for testing.

tox is used to manage tests and their environment.

### Running tests
Run the tests:

    tox

Alternative, from the directory, test:

    pytest

If you want to print std output of a test, run:

    pytest -s

I'm not sure how to pass this argument to pytest from tox yet.

Run a single test file like so:

pytest ./test/test_encryption.py

### Possible install of kohuhu needed
Installing kohuhu is needed run the tests if you use the `pytest` option. Install kohuhu in development 
mode via either:


    # Install the current directory and allow edits.
    pip install -e .
    python setup.py develop

However, it seems to work without doing this. Regardless, I've added git ignore entries
for the directories that those install commands would create, just incase we do need it.
