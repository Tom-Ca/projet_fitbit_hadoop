import base64
import urllib.request
import urllib.parse
import urllib.error
import urllib


# 0646639653
#These are the secrets etc from Fitbit developer
OAuthTwoClientID = ""
ClientOrConsumerSecret = ""

#This is the Fitbit URL
TokenURL = "https://api.fitbit.com/oauth2/token"

#I got this from the first verifier part when authorising my application
AuthorisationCode = ""

#Form the data payload
BodyText = {'client_id' : OAuthTwoClientID,
            'code' : AuthorisationCode,
            'redirect_url' : 'http://127.0.0.1',
            'code_verifier' : "eeOBw8vdPnelImUAsRWkwCjsufQFQKJxKTglLUvCdXdyUfvpvu5YwQAC4Mw7QUoGvfc",
            'grant_type' : 'authorization_code'}

BodyURLEncoded = urllib.parse.urlencode(BodyText)
BodyURLEncoded = BodyURLEncoded.encode('utf-8')

test = OAuthTwoClientID + ":" + ClientOrConsumerSecret
test = test.encode('utf-8')
print(BodyURLEncoded)

#Start the request
req = urllib.request.Request(TokenURL,BodyURLEncoded)

#Add the headers, first we base64 encode the client id and client secret with a : inbetween and create the authorisation header
# req.add_header('Authorization', 'Basic ' + str(base64.b64encode(test)))
req.add_header('Content-Type', 'application/x-www-form-urlencoded')

#Fire off the request
try:
  response = urllib.request.urlopen(req)

  FullResponse = response.read()

  print("Output >>> " + str(FullResponse))
except urllib.error.URLError as e:
  print (e.code)
  print (e.read())
