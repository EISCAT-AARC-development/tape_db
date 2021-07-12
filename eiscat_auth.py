#!/usr/bin/env python3
import re
import datetime

"""
EISCAT Download authorization rules

Summary:
-Everything older than 1 year is allowed
-AA and CP are always allowed
-Other experiments are only accessible by group members
matching Country or Account in db.


(C) Carl-Fredrik Enell 2021
carl-fredrik.enell@eiscat.se
"""

def _parse_groups(claim):
    ### Help routines
    """
    Return list of groups
    parsed from eduperson_entitlement claim.
    Input: list of claims as returned by OIDC introspection
    in result.json()['eduperson_entitlement']
    """

    # Country codes to modify to EISCAT db standard
    Codes ={
        'de': 'ge',
        'jp': 'ni',
        'se': 'sw',
    }
    groups = []
    # Match pattern
    claim_pat = r'urn\:mace\:egi\.eu\:group\:eiscat\.se\:(?P<group>\w+)\:'
    c = re.compile(claim_pat)
    for cl in claim:
        m = c.match(cl)
        group = m.groupdict()['group'].lower()
        if group in Codes:
            group = Codes[group]
        if group not in groups:
            groups.append(group)
    return groups

def auth_download(claim, expdate, account, codes):
    ### Main funtion
    """
    Authorization

    Inputs:
    claim: List of OIDC eduperson_entitlement claims
    expdate: Experiment start UNIX time
    account: Account string from db e.g "SW(30) NO(50)"
    codes: Assoc code(s) from db e.g. "FI"

    Return: True or False
    """
    ### Check age of experiment
    # Allow > 1 year
    timediff = datetime.datetime.fromtimestamp(expdate) - datetime.datetime.now()
    if timediff.days > 365:
        return True

    ### Merge Account and Assoc codes to one unique list
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

    ### Check download permission by group
    # Allow if no account information in db
    if len(allowed) == 0:
        return True
    # Allow AA and CP
    for group in ('aa', 'cp'):
        if group in allowed:
            return True
    # Allow group member
    for group in _parse_groups(claim):
        if group.lower() in allowed:
            return True
    # Otherwise deny access
    return False
