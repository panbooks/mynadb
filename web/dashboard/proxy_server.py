#!/usr/bin/env python3
"""
Simple CORS proxy server for MynaDB Dashboard
Serves static files and proxies API requests to MynaDB with CORS headers
"""

import http.server
import socketserver
import urllib.request
import urllib.error
import json
import os
from urllib.parse import urlparse

PORT = 8080
MYNADB_URL = "http://localhost:7000"
STATIC_DIR = os.path.dirname(os.path.abspath(__file__))

class CORSProxyHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=STATIC_DIR, **kwargs)

    def end_headers(self):
        # Add CORS headers to all responses
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        super().end_headers()

    def do_OPTIONS(self):
        """Handle CORS preflight requests"""
        self.send_response(200)
        self.end_headers()

    def do_GET(self):
        """Handle GET requests - proxy API or serve static files"""
        if self.path.startswith('/api/'):
            self.proxy_request('GET')
        elif self.path.startswith('/health') or self.path.startswith('/stats') or self.path.startswith('/query/') or self.path.startswith('/tables/'):
            self.proxy_request('GET')
        else:
            super().do_GET()

    def do_POST(self):
        """Handle POST requests - proxy to MynaDB"""
        self.proxy_request('POST')

    def proxy_request(self, method):
        """Proxy request to MynaDB server"""
        try:
            # Build target URL
            target_url = MYNADB_URL + self.path

            # Read request body if present
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length) if content_length > 0 else None

            # Create request
            req = urllib.request.Request(target_url, data=body, method=method)

            # Copy relevant headers
            if self.headers.get('Content-Type'):
                req.add_header('Content-Type', self.headers.get('Content-Type'))

            # Make request to MynaDB
            with urllib.request.urlopen(req, timeout=30) as response:
                response_body = response.read()

                self.send_response(response.status)
                self.send_header('Content-Type', response.headers.get('Content-Type', 'application/json'))
                self.send_header('Content-Length', len(response_body))
                self.end_headers()
                self.wfile.write(response_body)

        except urllib.error.HTTPError as e:
            error_body = e.read()
            self.send_response(e.code)
            self.send_header('Content-Type', 'text/plain')
            self.send_header('Content-Length', len(error_body))
            self.end_headers()
            self.wfile.write(error_body)

        except urllib.error.URLError as e:
            error_msg = f"Connection error: {str(e.reason)}".encode()
            self.send_response(503)
            self.send_header('Content-Type', 'text/plain')
            self.send_header('Content-Length', len(error_msg))
            self.end_headers()
            self.wfile.write(error_msg)

        except Exception as e:
            error_msg = f"Proxy error: {str(e)}".encode()
            self.send_response(500)
            self.send_header('Content-Type', 'text/plain')
            self.send_header('Content-Length', len(error_msg))
            self.end_headers()
            self.wfile.write(error_msg)

    def log_message(self, format, *args):
        """Custom log format"""
        print(f"[{self.log_date_time_string()}] {args[0]}")


def main():
    with socketserver.TCPServer(("", PORT), CORSProxyHandler) as httpd:
        print(f"=" * 60)
        print(f"  MynaDB Dashboard Proxy Server")
        print(f"=" * 60)
        print(f"  Dashboard:  http://localhost:{PORT}")
        print(f"  MynaDB API: {MYNADB_URL}")
        print(f"  Static dir: {STATIC_DIR}")
        print(f"=" * 60)
        print(f"  CORS enabled - proxying API requests to MynaDB")
        print(f"  Press Ctrl+C to stop")
        print(f"=" * 60)
        print()

        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down...")


if __name__ == "__main__":
    main()
