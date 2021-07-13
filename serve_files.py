#!/usr/bin/env python3

# the purpose of this file is to listen on a tcp port and handle
# download requests by HTTPS query on the form
# ?id=3&id=5&id=6&fname=output.tar&auth_token=<OIDC Token>
# the id is the resource_id and it is the
# responsibility for this script to select an appropriate
# source to fetch from.

""" 
EISCAT File server

This script listens for https requests on a tcp port (default 37009)
and prepares data for download

Additions 2021: Access token, OIDC introspection and access authorization check
URL format: https://machine:portno?id=<int>&id=<int>&fname=<str>.tar&access_token=<oidc token>

Last modifications
(C) Carl-Fredrik Enell 2021
carl-fredrik.enell@eiscat.se
"""

# from socketserver import BaseServer, ThreadingMixIn
from http.server import *
from urllib.parse import urlparse, parse_qs
import os
import sys
import cgi
import tapelib
import datetime
import eiscat_auth
import requests

portno = 37009

if len(sys.argv) > 1:
    portno = int(sys.argv[1])

# data_server_ssl_ca_path = os.environ["DATA_SERVER_SSL_CA_PATH"]
data_server_ssl_cert_path = os.environ["DATA_SERVER_SSL_CERT_PATH"]
data_server_ssl_key_path = os.environ["DATA_SERVER_SSL_KEY_PATH"]
client_url = os.environ["OIDC_URL"]
client_id = os.environ["OIDC_CLIENT_ID"]
client_secret = os.environ["OIDC_CLIENT_SECRET"]


if portno == 37009:
    print("Using SSL")
    print(f"SSL CA path: {data_server_ssl_ca_path}")
    print(f"SSL cert path: {data_server_ssl_cert_path}")
    print(f"SSL key path: {data_server_ssl_key_path}")

def GETorHEAD(self):
    import socket
    import subprocess
    ip = socket.gethostbyname(self.client_address[0])
    # Maximum 9 connections
    netst = 'netstat -nt | grep %d | grep ESTABLISHED | grep %s | wc -l' % (portno, ip)
    nconn = subprocess.Popen(['bash', '-c', netst], stdout=subprocess.PIPE).communicate()[0]
    if ip != "192.168.11.6" and int(nconn) > 9:
        sys.stderr.write(f"Too many connections: {ip}")
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write('<meta http-equiv="Refresh" content="9;url=javascript:history.go(-1)">'.encode('utf-8'))
        self.wfile.write(f"{ip} has reached maximum number of parallel streams".encode('utf-8'))
        return

    # Get parameters from query in URL
    p = urlparse(self.path)
    q = parse_qs(p.query)
    fname = q['fname'][0]
    access_token = q['access_token'][0]

    # OIDC Introspection of token
    ans = requests.get(f"{client_url}?token={access_token}", auth=(client_id, client_secret), headers={"Content-Type": "application/x-www-form-urlencoded"})
    if not ans.ok:
        sys.stderr.write("Could not connect to OIDC server")
        self.send_error(400, message="400 Authentication failure", explain="Could not connect to Checkin.") # Auth failed
    if not (ans.json()['active']):
        sys.stderr.write("No active login session")
        self.send_error(400, message="400 No active login session", explain="Something is wrong: Make sure you are logged in.") # Auth failed
    try:
        exp_time = datetime.datetime.fromisoformat(ans.json()['expires_at'].strip('+0000'))
        assert exp_time >= datetime.datetime.utcnow()
    except:
        self.send_error(401, message="401 Got invalid authentication", explain="Access token in URL has expired.") # Auth invalid
    try:
        claim = ans.json()['eduperson_entitlement']
    except:
        claim = ''
        self.send_error(401, message="401 Got invalid authentication", explain="OIDC Claim eduperson_entitlement is missing") # Auth invalid

    # Selected output format valid?
    format = os.path.splitext(fname)[1][1:]
    try:
        assert format in ('tar', 'tgz', 'zip')
    except AssertionError:
        sys.stderr.write(f"Unknown format: {ip} {fname}")
        return

    # Get data locations
    eids = q['id']
    try:
        sql = tapelib.opendefault()
    except:
        sys.stderr.write("Could not open database connection")
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
            assert eiscat_auth.auth_download(claim, l.date, l.account, l.country)
    except AssertionError:
        self.send_error(403, message="403 Not authorized", explain=f"You are not authorized to download this file. It is owned by: {country + ' ' + account}") #Access forbidden
        sys.stderr.write(f"Access denied for user {ans.json()['email']}")
        return
    finally:
        sql.close()
        del sql
    try:
        self.send_response(200)
        self.end_headers()
    except error as why:
        sys.stderr.write(f"{why} -- Timed out?")
        return

    # Send output
    import mimetypes
    mime, enc = mimetypes.guess_type(fname)
    if False:   # debug
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(f"Filename:  {fname}".encode('utf-8'))
        self.wfile.write(f"Mime: {mime} encoding {enc}".encode('utf-8'))
        self.wfile.write('\n'.join(paths).encode('utf-8'))
    else:
        self.send_header("Content-type", mime)
        self.send_header("Content-Disposition", f"attachment; filename={fname}")
        if enc:
            self.send_header("Content-encoding", enc)
        self.end_headers()
        send_archive(paths, format, fname, self.wfile)


class ReqHandler(BaseHTTPRequestHandler):
    def do_PING(self):
        self.wfile.write("PONG".encode('utf-8'))

    def do_GET(self):
        GETorHEAD(self)

    def do_HEAD(self):
        GETorHEAD(self)


##-class InetdHTTPServer(BaseServer):
##-    def __init__(self, requestHandler):
##-        BaseServer.__init__(self, None, requestHandler)
##-
##-    def get_request(self):
##-        import socket
##-        s = socket.fromfd(0, socket.AF_INET, socket.SOCK_STREAM)
##-        try:
##-            peer = s.getpeername()
##-        except socket.error:
##-            peer = ('127.0.0.1', 0)
##-        return s, peer
##-


##-class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
##-    """ This handles each request in a separate thread """
##-    #def handle_error(self, request, client_address):
##-    #   print ('ERROR:', client_address)
##-

def arcname(path):
    return '/'.join(path.split('/')[-2:])

def send_archive(paths, format, fname, fout):
    if format in ('tar', 'tgz'):
        import tarfile
        import socket
        mode = format == 'tgz' and 'w|gz' or 'w|'
        packer = tarfile.open(name=fname.encode('utf-8'), fileobj=fout, mode=mode)
        for path in paths:
            try:
                packer.add(path, arcname(path))
            except socket.error as why:
                sys.stderr.write(f"{why}-- tar stream aborted")
                return
            except:
                sys.stderr.write("File disappered")
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
    packer.close()

##def run_from_inetd():
##    httpd = InetdHTTPServer(ReqHandler)
##    httpd.handle_request()

def run_as_server():
    server_address = ('', portno)
    httpd = ThreadingHTTPServer(server_address, ReqHandler)
    if portno == 37009:
        print(f"Enabling SSL on port {portno}")
        import ssl
        # sslcontext = ssl.create_default_context(cafile=data_server_ssl_ca_path,purpose=ssl.Purpose.CLIENT_AUTH)
        # sslcontext.load_cert_chain(certfile=data_server_ssl_cert_path, keyfile=data_server_ssl_key_path)
        # httpd.socket = sslcontext.wrap_socket(httpd.socket, server_side=True)
        httpd.socket = ssl.wrap_socket(httpd.socket, keyfile=data_server_ssl_key_path, certfile=data_server_ssl_cert_path, server_side=True)
    print(f'Starting server on port {portno}')
    httpd.serve_forever()

def testzipper(path):
    for fname in ('test.tar', 'test.tgz', 'test.zip'):
        fout = open(fname, 'w')
        send_archive([path], fname[-3:], fname, fout)
        fout.close()
    sys.exit()

if __name__ == '__main__':
    run_as_server()
