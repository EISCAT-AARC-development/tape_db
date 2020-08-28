#!/usr/bin/python
from BaseHTTPServer import BaseHTTPRequestHandler,HTTPServer

import os, sys, cgi
import tapelib
from urlparse import urlparse
from BaseHTTPServer import *
import SocketServer
from SocketServer import BaseServer
import shutil

portno = 37009
certfile="/var/www/auth/public_key.pem"
keyfile="/var/www/auth/private_key.pem"

FILEPATH = "/tmp/tau1r_cp3_1.30_CP20070302.tar"

def touch(fname, times=None):
    with open(fname, 'a'):
        os.utime(fname, times)
        
# just making sure the file always exists...
touch(FILEPATH)

def permitted():
	return True

def validToken():
	return True

def GETorHEAD(self):
		# handle incoming token to validate if tis is a valid request
		if validToken():
			# Skipping part where data file gets analyzed

			# Skipping part where data gets pulled from DB as we do not need that for testing AuthZ

			# Fake the output file for now.
			if permitted():
				# simplified version of file sending algo
				with open(FILEPATH, 'rb') as f:
					self.send_response(200)
					self.send_header("Content-Type", 'application/octet-stream')
					self.send_header("Content-Disposition", 'attachment; filename="{}"'.format(os.path.basename(FILEPATH)))
					fs = os.fstat(f.fileno())
					self.send_header("Content-Length", str(fs.st_size))
					self.end_headers()
					shutil.copyfileobj(f, self.wfile)
			else:
				send404()
		else:
			send404()

def send404():
	print('HTTP/1.1 404 Not Found\r\n')
	print('Content-Type: text/html\r\n\r\n')
	print('<html><head></head><body><h1>404 Not Found</h1></body></html>')


#This class will handles any incoming request from
#the browser 
class ReqHandler(BaseHTTPRequestHandler):
	def do_PING(self):
		print >> self.wfile, "PONG"

	def do_GET(self):
		GETorHEAD(self)

	def do_HEAD(self):
		GETorHEAD(self)

try:
	#Create a web server and define the handler to manage the
	#incoming request
	httpd = HTTPServer(('', portno), ReqHandler)
	#if portno == 37009:
	#	import ssl
	#	sslcontext=ssl.create_default_context() # we use the default CA certificate path here
	#	sslcontext.load_cert_chain(certfile,keyfile) # insert paths to valid cert and key
	#	httpd.socket=sslcontext.wrap_socket(httpd.socket, server_side=True)
	print 'Started httpserver on port ' , portno
	
	#Wait forever for incoming htto requests
	httpd.serve_forever()

except KeyboardInterrupt:
	print '^C received, shutting down the web server'
	httpd.socket.close()
