# kohuhu

Decrypt the exchange API information via:
.. 

    python ./src/exchanges.py --decrypt exchanges.json.encrypted

Encrypt it again via:
..

    python ./src/exchanges.py --encrypt exchanges.json -o=exchanges.json.encrypted
    
If you get sick of bash not auto-completing Python scripts, 
from the Python env, run:
..

    activate-global-python-argcomplete --user
    
