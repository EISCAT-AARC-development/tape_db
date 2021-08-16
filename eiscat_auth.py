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
    mod_codes ={
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
        try:
            group = m.groupdict()['group'].lower()
            if group in mod_codes:
                group = mod_codes[group]
            if group not in groups:
                groups.append(group)
        except:
            pass
    return groups

def auth_download(claim, expdate, account, country):
    ### Main funtion
    """
    Authorization

    Inputs:
    claim: List of OIDC eduperson_entitlement claims
    expdate: Experiment start UNIX time
    account: Account string from db e.g "SW(30)NO(50)"
    country: Assoc code(s) from db e.g. "FI"

    Return: (True or False, Comments)
    """
    vo_groups = _parse_groups(claim)
    ### Verify VO membership
    try:
        assert 'members' in vo_groups
    except AssertionError:
        print("Download attempt by non VO member")
        return (False, 'You are not a member of the EISCAT VO. Please sign up.')
    
    ### Check age of experiment
    # Allow > 1 year
    timediff = datetime.datetime.now() - datetime.datetime.fromtimestamp(expdate)
    if timediff.days > 365:
        print("Access granted: old data\n")
        return (True, '')

    ### Merge Account and Country codes to one unique list
    allowed = []
    try:
        # remove any number in bracket
        account = re.sub('\(\d+\)', ' ', account)
        for assoc in account.split(' '):
            assoc = assoc[0:2].lower() # CP6 -> cp, etc
            if len(assoc) == 2 and assoc not in allowed:
                allowed.append(assoc) 
    except:
        pass
    
    try:
        # Country is always a 2-letter string
        assoc = country[0:2].lower()
        if assoc != 'sp' and assoc not in allowed:
            allowed.append(assoc) 
    except:
        pass
    
    print(f"Allowed groups: {allowed}")
    ### Check download permission by group
    # Allow if no account information in db
    if len(allowed) == 0:
        print("Allowed groups empty. Granting access\n")
        return (True, '')
    # Allow AA and CP
    for group in ('aa', 'cp'):
        if group in allowed:
            print(f"Group {group} is allowed. Granting access\n")
            return (True, '')
    # Allow group member
    for group in vo_groups:
        if group == 'ei':
            print(f"Download by staff is always allowed. Granting access\n")
            return (True, '')
        if group in allowed:
            print(f"Group {group} is allowed. Granting access\n")
            return (True, '')
    # Otherwise deny access
    print(f"Access denied ")
    return (False, f'Access to this experiment is restricted to {allowed}')
