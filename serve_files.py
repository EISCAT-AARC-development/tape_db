#!/usr/bin/env python3

# the purpose of this file is to listen on a tcp port and handle
# download requests by HTTP(S) query on the form
# id=3&id=5&id=6&format=tar
# the id is the resource_id and it is the
# responsibility for this script to select an appropriate
# source to fetch from.
# format is tar, tgz or zip

from socketserver import BaseServer, ThreadingMixIn
from http.server import *
import os
import sys
import cgi
import tapelib

sys.path.append("/home/archive/shared-auth")
from token_url_utility import ExtendedUrl

portno = 37009

if len(sys.argv) > 1:
    portno = int(sys.argv[1])

token_signing_pub_key_path = os.environ["TOKEN_SIGNING_PUB_KEY_PATH"]
# data_server_ssl_ca_path = os.environ["DATA_SERVER_SSL_CA_PATH"]
# data_server_ssl_ca_file = os.environ["DATA_SERVER_SSL_CA_FILE"]
data_server_ssl_cert_path = os.environ["DATA_SERVER_SSL_CERT_PATH"]
data_server_ssl_key_path = os.environ["DATA_SERVER_SSL_KEY_PATH"]
token_signing_pub_key = open(token_signing_pub_key_path, 'r').read()

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
    ext_url = ExtendedUrl(self.path)
    try:
        claims = ext_url.get_claims(token_signing_pub_key)
    except Exception as e:
        print(e)
    ext_url.remove_token_from_url()
    req = '/' + str(ext_url.path)
    print('token validated correctly')
    print(f'requested resource: {req}')
    req, fname = os.path.split(req)
    format = os.path.splitext(fname)[1][1:]
    try:
        assert format in ('tar', 'tgz', 'zip')
    except AssertionError:
        sys.stderr.write(f"Unknown format: {ip} {fname}".encode('utf-8'))
        return
    paths = req.split(';')
    for i, path in enumerate(paths):
        if path[0] != '/':
            paths[i] = '/'+path
    sql = tapelib.opendefault()
    if paths[0][1:].isdigit():
        path = []
        machine = tapelib.nodename()
        for id in paths:
            cmd = "SELECT location FROM storage WHERE resource_id=%s AND location LIKE 'eiscat-raid://%s%%'" % (id[1:], machine)
            sql.cur.execute(cmd)
            ls = sql.cur.fetchall()[0][0]
            m, path1, f = tapelib.parse_raidurl(ls)
            path.append(path1)
            paths = path
    try:
        try:
            for path in paths:
                url = tapelib.create_raidurl(tapelib.nodename(), path)
                l = sql.select_experiment_storage("location = %s", (url,), what="account, country, UNIX_TIMESTAMP(start) AS date, type")[0]
                print(l)
        except AssertionError:
            sys.stderr.write(f"Bad IP: {ip} {(l.account or l.country)}".encode('utf-8'))
        finally:
            sql.close()
        del sql
    except IOError as why:
        print(f"{why} -- Don't really need it if all is right")

    try:
        self.send_response(200)
    except error as why:
        sys.stderr.write(f"{why} -- Timed out?")
        return
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


class InetdHTTPServer(BaseServer):
    def __init__(self, requestHandler):
        BaseServer.__init__(self, None, requestHandler)

    def get_request(self):
        import socket
        s = socket.fromfd(0, socket.AF_INET, socket.SOCK_STREAM)
        try:
            peer = s.getpeername()
        except socket.error:
            peer = ('127.0.0.1', 0)
        return s, peer

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """ This handles each request in a separate thread """
    #def handle_error(self, request, client_address):
    #   print ('ERROR:', client_address)

def arcname(path):
    return '/'.join(path.split('/')[-2:])

def send_archive(paths, format, fname, fout):
    if format in ('tar', 'tgz'):
        import tarfile
        import socket
        mode = format == 'tgz' and 'w|gz' or 'w|'
        packer = tarfile.open(name=fname, fileobj=fout, mode=mode)
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
            file, arc = files.pop()
            if os.path.isdir(file):
                content = os.listdir(file)
                files.extend([(file+'/'+m, arc+'/'+m) for m in content])
            else:
                packer.write(file, arc)
    packer.close()

def run_from_inetd():
    httpd = InetdHTTPServer(ReqHandler)
    httpd.handle_request()

def run_as_server():
    server_address = ('', portno)
    httpd = ThreadedHTTPServer(server_address, ReqHandler)
    if portno == 37009:
        print("Enabling SSL")
        import ssl
        sslcontext = ssl.create_default_context(purpose=ssl.Purpose.CLIENT_AUTH)
        sslcontext.load_cert_chain(certfile=data_server_ssl_cert_path, keyfile=data_server_ssl_key_path)
        httpd.socket = sslcontext.wrap_socket(httpd.socket, server_side=True)
    print('Starting server on port %i' % portno)
    httpd.serve_forever()

def testzipper(path):
    for fname in ('test.tar', 'test.tgz', 'test.zip'):
        fout = open(fname, 'w')
        send_archive([path], fname[-3:], fname, fout)
        fout.close()
    sys.exit()

if __name__ == '__main__':
    run_as_server()
