"""
Retrieve a Google OAuth refresh token for YouTube API access to my channel data.
- Receive authorization URL from Google's oauth endpoint
- Spin up a one-shot http server to capture OAuth server response
- On callback, verify state and then exchange the code for a refresh token
- Persist credentials so other scripts can reuse them
"""

import google_auth_oauthlib.flow
from dotenv import load_dotenv
from pathlib import Path
import os
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler

# Load environment variables
ROOT = Path(__file__).parent.parent
REDIRECT_URI = 'http://localhost:8080'
load_dotenv(ROOT / '.env')

# Set the level of access that my application will have to my Google account (YT Reporting details only)
flow = google_auth_oauthlib.flow.Flow.from_client_secrets_file(os.environ.get('OAUTH_CLIENT_SECRET_PATH'),
    scopes=['https://www.googleapis.com/auth/yt-analytics.readonly',
            'https://www.googleapis.com/auth/yt-analytics-monetary.readonly'],
            redirect_uri = REDIRECT_URI)

authorization_url, state = flow.authorization_url(
    # Enable offline access so that you can refresh an access token without re-prompting the user for permission.
    access_type='offline',
    prompt='consent',
    # Optional, enable incremental authorization. Recommended as a best practice.
    include_granted_scopes='true',
    )

# Launch the browser to complete Google's OAuth web-based flow
webbrowser.open(authorization_url)

# Handle reception of GET request from OAuth server following Google account authorization
class OAuthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.server.oauth_response= self.path
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Received OAuth code. You can close this tab.")

# Spin up a simple HTTP server listening on port 8080 to accept callback
server_address = ('', 8080)
with HTTPServer(server_address, OAuthHandler) as httpd:
   httpd.handle_request()
   oauth_response = f'https://{httpd.oauth_response}'
   
# Fetch and store credentials in authorized_user.json file
print(oauth_response)
print(REDIRECT_URI)
flow.fetch_token(authorization_response=oauth_response)
creds_dest = ROOT / 'auth' / 'authorized_user.json'
with open(creds_dest, 'w') as f:
    f.write(flow.credentials.to_json())