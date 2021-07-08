#!/usr/bin/env python3
import re
import datetime


def parse_groups(claim):
    ### Country codes to modify
    Codes ={
        'de': 'ge',
        'jp': 'ni',
        'se': 'sw',
    }
    groups = []
    # Claim example
    # urn:mace:egi.eu:group:eiscat.se:EI:Experiments:role=member#aai.egi.eu,urn:mace:egi.eu:group:eiscat.se:EI:role=member#aai.egi.eu
    claim_pat = r'urn\:mace\:egi\.eu\:group\:eiscat\.se\:(?P<group>\w+)\:'
    c = re.compile(claim_pat)
    for cl in claim.split(','):
        m = c.match(cl)
        if group in Codes:
            group = Codes[group]
        if group not in groups:
            groups.append(group)
    return groups

def auth_download(groups, expdate, account, codes):
    ### Check age of experiment
    # Allow > 1 year
    timediff = expdate - datetime.datetime.now()
    if timediff.days > 365:
        return True
    ### Check download permission
    # Allow AA and CP
    for group in ('aa', 'cp'):
        if group in account.lower() or group in codes.lower():
            return True
    # Allow owner
    for group in groups:
        if group.lower() in account.lower() or group.lower in codes.lower():
            return True
    # Otherwise deny access
    return False
    
    
