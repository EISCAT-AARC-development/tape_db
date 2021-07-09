#!/usr/bin/env python3                                                                                                                                                       
from jwt import decode                                                                                                                                                      
from os import environ
from cryptography.hazmat.primitives import serialization


### Decode routine
def token_dec(token):                                                                                                                                                  
    # Read public key
    try:
        with open(environ["TOKEN_SIGNING_PUB_KEY_PATH"], 'rb') as pkey:
            public_key = serialization.load_ssh_public_key(pkey.read())
    except:
        print("Reading key failed")
        raise IOError("Could not read public key for validation")
    # Validate and decode
    out = decode(token, key=public_key, algorithms=['RS256', ])
    return(out)
