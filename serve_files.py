#!/usr/bin/env python

# the purpose of this file is to listen on a tcp port and handle
# download requests by HTTP(S) query on the form
# id=3&id=5&id=6&format=tar
# the id is the resource_id and it is the
# responsibility for this script to select an appropriate
# source to fetch from.
# format is tar, tgz or zip

import os, sys, cgi

import tapelib

portno = 37009

if len(sys.argv)>1:
	portno=int(sys.argv[1])

HQ = "127.0.0.0/8 193.11.31.253/32 192.168.10.0/23 10.0.8.0/24"
TRO= "129.242.31.0/25"
ESR= "158.39.70.0/26 158.39.70.220/32"
KIR= "193.10.33.64/27 193.10.33.96/28"
SOD= "193.167.134.192/27"
permitted_ips = HQ+" "+TRO+" "+ESR+" "+KIR+" 1.1.1.1/32"
einfra_ips = "146.48.0.0/16/CNR"
affiliate_ips = "193.48.8.59/32/FR/2018 203.250.178.0/23/KR/2018"
blacklist = ""

def permitted(ip, country, date, type):
	owners='UK NI NO SW FI CN'
	common='CP UP AA IPY'
	if not country:	country='?'
	# check for ip numbers - taken from Madrigal trustedIPs.txt
	ip32=0
	for ippart in ip.split('.'): ip32=ip32*256+int(ippart)
	for known in permitted_ips.split():
		net,mask=known.split('/')
		net32=0
		for ippart in net.split('.'): net32=net32*256+int(ippart)
		if (ip32-net32)>>(32-int(mask)) == 0: return True
	for known in blacklist.split():
		net,mask=known.split('/')
		net32=0
		for ippart in net.split('.'): net32=net32*256+int(ippart)
		if (ip32-net32)>>(32-int(mask)) == 0: return False
	import time
	for known in einfra_ips.split():
		net,mask,eri=known.split('/')
		net32=0
		for ippart in net.split('.'): net32=net32*256+int(ippart)
		if (ip32-net32)>>(32-int(mask)) == 0 and time.time()>date+86400*(4*365+1): return True
	yr=time.gmtime().tm_year
	for known in affiliate_ips.split():
		net,mask,affiliate,year=known.split('/')
		net32=0
		for ippart in net.split('.'): net32=net32*256+int(ippart)
		if (ip32-net32)>>(32-int(mask)) == 0 and int(year) == yr:
			owners=owners+" "+affiliate
	import socket
	try:
		host,dd,d= socket.gethostbyaddr(ip)
		host=host.split('.')[-1]
		if len(host)>2: raise socket.error
	except socket.error:
		try:
			#No hostname found or not two letter country, try with whois
			#wserv='countries.blackholes.us'
			wserv='zz.countries.nerd.dk'
			import commands
			pi=ip.split('.')
			host=commands.getoutput('nslookup -q=txt '+pi[3]+'.'+pi[2]+'.'+pi[1]+'.'+pi[0]+'.'+wserv+' | grep text').split('"')[1]
		except:
			return False

        # Domain to EISCAT country code used in SQL  DB.
        # ge is Georgia, ni is Nicaragua. Block.
        if host=='ge': return False                
        if host=='ni': return False
        #  EISCAT codes differ for Germany, Sweden, Japan
	if host=='de': host='ge'
	if host=='se': host='sw'
	if host=='jp': host='ni'

	host=host.upper()
	if type=='info':
		# any country can download exp files
		return True
	elif host in country:
		# any country can download own data
		return True
	elif host in owners:
		# EISCAT countries can download old data
		if time.time()>date+86400*366: return True
		# EISCAT countries can download recent CP (UP. AA) data
		for cp in common.split():
			if cp in country: return True
	elif host=='NL' and (yr>2006 and yr<2009):
		# egi can download IPY data
		return True
	return False

from BaseHTTPServer import *
import SocketServer

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
		req = self.path
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
					assert permitted(ip, (l.account or l.country), l.date, l.type)
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
		sslcontext=ssl.create_default_context(cafile="/etc/ssl/certs/ssl-cert-snakeoil.pem",purpose=ssl.Purpose.CLIENT_AUTH)
		sslcontext.load_cert_chain(certfile="/etc/ssl/certs/ssl-cert-snakeoil.pem",keyfile="/etc/ssl/private/ssl-cert-snakeoil.key")
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
