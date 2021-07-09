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
        group = m.groupdict()['group'].lower()
        if group in Codes:
            group = Codes[group]
        if group not in groups:
            groups.append(group)
    return groups

def auth_download(groups, expdate, account, codes):
    ### Check age of experiment
    # Allow > 1 year
    # expdate is in unix time
    timediff = datetime.datetime.fromtimestamp(expdate) - datetime.datetime.now()
    if timediff.days > 365:
        return True
    # Merge Account and Group
    allowed = []
    try:
        for assoc in account.split(' '):
            # remove any number in bracket
            assoc = re.sub('\(\d+\)', '', assoc)
            if assoc not in allowed:
                allowed.append(assoc.lower()) 
    except:
        pass
    try:
        for assoc in codes.split(' '):
            assoc = re.sub('\(\d+\)', '', assoc)
            if assoc not in allowed:
                allowed.append(assoc.lower()) 
    except:
        pass

    ### Check download permission
    # Allow if no account information in db
    if len(allowed) == 0:
        return True
    # Allow AA and CP
    for group in ('aa', 'cp'):
        if group in allowed:
            return True
    # Allow owner
    for group in groups:
        if group.lower() in allowed:
            return True
    # Otherwise deny access
    return False
    
    
