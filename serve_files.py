#!/usr/bin/env python

# the purpose of this file is to listen on a tcp port and handle
# download requests by HTTP(S) query on the form
# id=3&id=5&id=6&format=tar
# the id is the resource_id and it is the
# responsibility for this script to select an appropriate
# source to fetch from.
# format is tar, tgz or zip

import os
import sys
import tapelib
from eiscat_auth import download_authz
from token_url_utility import ExtendedUrl

portno = 37009

if len(sys.argv)>1:
	portno=int(sys.argv[1])

from BaseHTTPServer import *
import SocketServer

portal_public_key_path = '/var/***REMOVED***/auth/public_key.pem' 

portal_public_key = open(portal_public_key_path,'r').read()

def GETorHEAD(self):
		# self.path and self.client_address
		import socket, subprocess
		ip = socket.gethostbyname(self.client_address[0])
		# Maximum 9 connections
		netst='netstat -nt | grep %d | grep ESTABLISHED | grep %s | wc -l'%(portno,ip)
		#netst='netstat -n | grep %d | grep %s | wc -l'%(portno,ip)
		nconn=subprocess.Popen(['bash','-c',netst], stdout=subprocess.PIPE).communicate()[0]
		if ip != "192.168.11.6" and int(nconn)>9:
			print >> sys.stderr, "Too many connections:", ip
			self.send_header("Content-type", "text/html")
			self.end_headers()
			print >> self.wfile, '<meta http-equiv="Refresh" content="9;url=javascript:history.go(-1)">'
			print >> self.wfile, ip, "has reached maximum number of parallel streams"
			return

		# retrived decoded url here

		ext_url = ExtendedUrl(self.path)
		
		try:
			claims = ext_url.get_claims(portal_public_key)
		except Exception as e:
			print e
		ext_url.remove_token_from_url()
		req = '/' + str(ext_url.path)
		req, fname = os.path.split(req)
		format = os.path.splitext(fname)[1][1:]

		try:
			assert format in ('tar', 'tgz', 'zip')
		except AssertionError:
			print >> sys.stderr, "Unknown format:", ip, fname
			return
		paths = req.split(';')
		for i, path in enumerate(paths):
			if path[0] != '/': paths[i] = '/'+path
		sql = tapelib.opendefault()
		if paths[0][1:].isdigit():
			path=[]
			machine=tapelib.nodename()
			for id in paths:
				cmd="SELECT location FROM storage WHERE resource_id=%s AND location LIKE 'eiscat-raid://%s%%'"%(id[1:], machine)
				sql.cur.execute(cmd)
				ls=sql.cur.fetchall()[0][0]
				m, path1 ,f= tapelib.parse_raidurl(ls)
				path.append(path1)
			paths=path
		try:
			try:
				for path in paths:
					url = tapelib.create_raidurl(tapelib.nodename(), path)
					l = sql.select_experiment_storage("location = %s", (url,), what="account, country, UNIX_TIMESTAMP(start) AS date, type")[0]
					assert download_authz(ip, (l.account or l.country), l.date, l.type)
			except AssertionError:
				print >> sys.stderr, "Bad IP:", ip, (l.account or l.country)
			finally:
				sql.close()
			del sql
		except IOError as why:
			print(why,"-- Don't really need it if all is right")

		try:
			self.send_response(200)
		except error as why:
			print >> sys.stderr, why, "-- Timed out?"
			return
		import mimetypes
		mime, enc = mimetypes.guess_type(fname)
		if 0:	# debug
			self.send_header("Content-type", "text/plain")
			self.end_headers()
			print >> self.wfile, "Filename:", fname
			print >> self.wfile, "Mime:", mime, "encoding", enc
			print >> self.wfile, '\n'.join(paths)
		else:
			self.send_header("Content-type", mime)
			if enc:
				self.send_header("Content-encoding", enc)
			self.end_headers()
			send_archive(paths, format, fname, self.wfile)

class ReqHandler(BaseHTTPRequestHandler):
	def do_PING(self):
		print >> self.wfile, "PONG"

	def do_GET(self):
		GETorHEAD(self)

	def do_HEAD(self):
		GETorHEAD(self)

from SocketServer import BaseServer

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

class ThreadedHTTPServer(SocketServer.ThreadingMixIn, HTTPServer):
	def handle_error(self, request, client_address):
		print('ERROR:', client_address)

def arcname(path):
	return '/'.join(path.split('/')[-2:])

def send_archive(paths, format, fname, fout):
	if format in ('tar', 'tgz'):
		import tarfile
		import socket
		# python 2.6 needs 'w|' (instead of 'w') since it is a stream
		mode = format == 'tgz' and 'w|gz' or 'w|'
		packer = tarfile.open(name=fname, fileobj=fout, mode=mode)
		for path in paths:
			try:
				packer.add(path, arcname(path))
			except socket.error as why:
				print >> sys.stderr, why, "-- tar stream aborted"
				return
			except:
				print >> sys.stderr, "File disappered"
				return
	elif format == 'zip':
		import zipfile
		packer = zipfile.ZipFile(fout, "w", zipfile.ZIP_STORED)
		files = [ (x, arcname(x)) for x in paths ]
		while files:
			file, arc = files.pop()
			if os.path.isdir(file):
				content = os.listdir(file)
				files.extend([ (file+'/'+m, arc+'/'+m) for m in content ])
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
		import ssl

		# This won't work, we need valid certificates
		sslcontext=ssl.create_default_context(purpose=ssl.Purpose.CLIENT_AUTH, cafile='') # insert CA certificate path here
		sslcontext.load_cert_chain(certfile="/etc/ssl/certs/ssl-cert-snakeoil.pem", keyfile="/etc/ssl/private/ssl-cert-snakeoil.key") # insert paths to valid cert and key
		httpd.socket=sslcontext.wrap_socket(httpd.socket, server_side=True)
	httpd.serve_forever()

def testzipper(path):
	for fname in ('test.tar', 'test.tgz', 'test.zip'):
		fout = open(fname, 'w')
		send_archive([path], fname[-3:], fname, fout)
		fout.close()
	sys.exit()

if __name__ == '__main__':
	#testzipper(sys.argv[1])
	#run_from_inetd()
	run_as_server()
