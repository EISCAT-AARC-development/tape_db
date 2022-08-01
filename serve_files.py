#!/usr/bin/env -S python3 -u
""" 
EISCAT File server

This script listens for https requests on a tcp port (default 37009)
and prepares data for download

It accepts queries of the form
https://machine:portno?id=<int>&id=<int>&fname=<str>.tar&access_token=<oidc token>
where
id: resource_id:s as in EISCAT SQL file catalogue
fname: desired name and format of the output archive, <experiment name>.(tar|tbz2|tgz|zip)
access_token: JWT provided by EGI Checkin for authenticated user

Additions 2021-2022: Access token, OIDC introspection and access authorization check
Last modifications (C) Carl-Fredrik Enell 2022
carl-fredrik.enell@eiscat.se
"""

from http.server import *
from urllib.parse import urlparse, parse_qs
import os
import sys
import cgi
import tapelib
import datetime
import eiscat_auth
import requests


### Configuration
if len(sys.argv) > 1:
    portno = int(sys.argv[1])
else:
    try:
        portno = int(os.environ["PORT_NO"])
    except:
        portno = 37009

print(f"Configured port {portno}")

client_url = os.environ["OIDC_URL"]
client_id = os.environ["OIDC_CLIENT_ID"]
client_secret = os.environ["OIDC_CLIENT_SECRET"]
data_server_ssl_cert_path = ""
data_server_ssl_key_path = ""
if portno == 37009:
    data_server_ssl_cert_path = os.environ["DATA_SERVER_SSL_CERT_PATH"]
    data_server_ssl_key_path = os.environ["DATA_SERVER_SSL_KEY_PATH"]
    print(f"serve_files: {datetime.datetime.utcnow().isoformat()} Using SSL")
    print(f"SSL cert path: {data_server_ssl_cert_path}")
    print(f"SSL key path: {data_server_ssl_key_path}")
###
    
def GETorHEAD(self):
    # Get parameters from query in URL
    p = urlparse(self.path)
    q = parse_qs(p.query)
    ip = f"Real IP: {self.headers['X-Real-IP']} Forwarded for: {self.headers['X-Forwarded-For']}"

    try:
        fname = q['fname'][0]
    except:
        fname = ''
    try:
        access_token = q['access_token'][0]
    except:
        access_token = ''
    
    # OIDC Introspection of token
    # ans = requests.get(f"{client_url}?token={access_token}", auth=requests.auth.HTTPBasicAuth(client_id, client_secret), headers={"Content-Type": "application/x-www-form-urlencoded"})
    postdata = {"client_id": client_id, "client_secret": client_secret, "token": access_token}
    ans = requests.post(url=client_url, data=postdata)
    if not ans.ok:
        print(f"serve_files {datetime.datetime.utcnow().isoformat()} Could not connect to OIDC server")
        self.send_error(ans.status_code, message="OIDC Introspection failure", explain=ans.reason) # Auth failed
        return
    if not (ans.json()['active']):
        print(f"serve_files {datetime.datetime.utcnow().isoformat()} No active login session")
        self.send_error(401, message="No active login session", explain="Something is wrong: Make sure you are logged in.") # Auth failed
        return
    try:
        # exp_time = datetime.datetime.fromisoformat(ans.json()['expires_at'][0:19])
        exp_time = datetime.datetime.fromtimestamp(ans.json()['exp'])
        assert exp_time > datetime.datetime.utcnow()
    except:
        self.send_error(401, message="Invalid authentication", explain="Access token in request has expired.") # Auth invalid
        return
    try:
        claim = ans.json()['eduperson_entitlement']
    except:
        claim = ''
        self.send_error(401, message="Invalid authentication", explain="OIDC Claim eduperson_entitlement is missing") # Auth invalid
        return

    # Get data locations
    eids = q['id']
    try:
        sql = tapelib.opendefault()
    except:
        print(f"serve_files {datetime.datetime.utcnow().isoformat()} Could not open database connection")
        self.send_error(500, message="Server failure", explain="Could not connect to file catalogue database.") # Auth failed
        return
    paths = []
    machine = tapelib.nodename()
    for eid in eids:
        cmd = "SELECT location FROM storage WHERE resource_id=%s AND location LIKE 'eiscat-raid://%s%%'" % (eid, machine)
        sql.cur.execute(cmd)
        ls = sql.cur.fetchall()[0][0]
        m, path1, f = tapelib.parse_raidurl(ls)
        paths.append(path1)

    # Authorization check
    try:
        for path in paths:
            url = tapelib.create_raidurl(tapelib.nodename(), path)
            l = sql.select_experiment_storage("location = %s", (url,), what="account, country, UNIX_TIMESTAMP(start) AS date, type")[0]
            (allowed, comment) =  eiscat_auth.auth_download(claim, l.date, l.account, l.country)
            assert allowed
    except AssertionError:
        if not l.account is None:
            assoc = l.account
        else:
            assoc = l.country
        self.send_error(403, message="Not authorized", explain=comment) #Access forbidden
        print(f"serve_files {datetime.datetime.utcnow().isoformat()} Access denied for {ip} with entitlement {claim}. {comment}")
        return
    finally:
        sql.close()
        del sql
    try:
        self.send_response(200)
    except error as why:
        print(f"{why} -- Timed out?")
        self.send_error(500, message="Server failure", explain="Timeout sending response.") # Auth failed
        return

    # Selected output format valid?
    format = os.path.splitext(fname)[1][1:]
    try:
        assert format in ('tbz2', 'tar', 'tgz', 'zip')
    except AssertionError:
        print(f"serve_files {datetime.datetime.utcnow().isoformat()} Unknown format: {ip} {fname}")
        self.send_error(415, message="Unknown format", explain=f"Requested file format {format} is not supporte by this server.")
        return
    
    # Send output
    import mimetypes
    mime, enc = mimetypes.guess_type(fname)
    print(f"serve_files {datetime.datetime.utcnow().isoformat()} Sending data to {ip} with entitlement: {claim}. {comment}")
    self.send_header("Content-type", mime)
    self.send_header("Content-Disposition", f"attachment; filename={fname}")
    if enc:
        self.send_header("Content-encoding", enc)
    self.end_headers()
    send_archive(paths, format, fname, self.wfile)
    print(f"serve_files {datetime.datetime.utcnow().isoformat()} Done sending data to {ip}")

class ReqHandler(BaseHTTPRequestHandler):
    def do_PING(self):
        self.wfile.write("PONG".encode('utf-8'))

    def do_GET(self):
        GETorHEAD(self)

    def do_HEAD(self):
        GETorHEAD(self)

    def log_request(self, code):
        # This suppresses standard logging; URLs contain access tokens.
        pass

def arcname(path):
    return '/'.join(path.split('/')[-2:])

def send_archive(paths, format, fname, fout):
    if format in ('tbz2', 'tar', 'tgz'):
        import tarfile
        import socket
        if format == 'tbz2':
            mode = 'w|bz2'
        elif format == 'tgz':
            mode = 'w|gz'
        else:
            mode = 'w|'
            packer = tarfile.open(name=fname.encode('utf-8'), fileobj=fout, mode=mode, format=tarfile.GNU_FORMAT)
        for path in paths:
            try:
                packer.add(path, arcname(path))
            except socket.error as why:
                print(f"{why}-- tar stream aborted")
                return
            except:
                print("File disappered")
    elif format == 'zip':
        import zipfile
        packer = zipfile.ZipFile(fout, "w", zipfile.ZIP_STORED)
        files = [(x, arcname(x)) for x in paths]
        while files:
            tfile, arc = files.pop()
            if os.path.isdir(tfile):
                content = os.listdir(tfile)
                files.extend([(tfile+'/'+m, arc+'/'+m) for m in content])
            else:
                packer.write(tfile, arc)
    else:
        raise ValueError("Checkme: should never end up here since file type was asserted above")
    packer.close()

def run_as_server():
    server_address = ('', portno)
    httpd = ThreadingHTTPServer(server_address, ReqHandler)
    if portno == 37009:
        print(f"serve_files {datetime.datetime.utcnow().isoformat()} Enabling SSL on port {portno}")
        import ssl
        httpd.socket = ssl.wrap_socket(httpd.socket, keyfile=data_server_ssl_key_path, certfile=data_server_ssl_cert_path, server_side=True)
    print(f'serve_files {datetime.datetime.utcnow().isoformat()} Starting server on port {portno}')
    httpd.serve_forever()

if __name__ == '__main__':
    run_as_server()
