#!/usr/bin/env python3                                                                                                                                                       
from jwt import decode                                                                                                                                                      
from os import environ
from cryptography.hazmat.primitives import serialization

### Decode routine
def token_dec(token):                                                                                                                                                  
    # Read public key
    try:
        with open(os.environ["TOKEN_SIGNING_PUB_KEY_PATH"], 'rb') as public_key:
            pub_key = serialization.load_ssh_public_key(public_key.read())
    except:
        raise IOError("Could not read public key for validation")
    
    # Validate and decode
    out = decode(token, key=pub_key, algorithms=['RS256', ])
    return(out)
