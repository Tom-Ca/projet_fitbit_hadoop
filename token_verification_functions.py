import base64
import urllib.request
import urllib.parse
import urllib.error
import urllib
import sys
import json


def write_config(acc_token, ref_token, name_chose):
    replacement = ""
    with open("tokens.txt", "r") as file_in:
        line = file_in.readline()
        while line != "":
            if line == ("Name : " + name_chose + "\n"):
                replacement = replacement + line
                line = file_in.readline()
                change1 = line.replace(line, "AccToken : " + acc_token + "\n")
                replacement = replacement + change1
                line = file_in.readline()
                change2 = line.replace(line, "RefToken : " + ref_token + "\n")
                replacement = replacement + change2
            else:
                replacement = replacement + line
            line = file_in.readline()
    fout = open("tokens.txt", "w")
    fout.write(replacement)
    fout.close()


def get_new_access_token(ref_token, client_ID, secret_key, name_chose):
    # Form the data payload
    body_text = {'grant_type': 'refresh_token',
                'refresh_token': ref_token}

    body_url_encoded = urllib.parse.urlencode(body_text)
    body_url_encoded = body_url_encoded.encode('utf-8')

    # Start the request
    token_req = urllib.request.Request("https://api.fitbit.com/oauth2/token", body_url_encoded)

    basic = (client_ID + ":" + secret_key)
    basic = basic.encode('utf-8')
    basic = base64.b64encode(basic)
    basic = basic.decode('utf-8')

    token_req.add_header('Authorization', 'Basic ' + str(basic))
    token_req.add_header('Content-Type', 'application/x-www-form-urlencoded')

    # Fire off the request
    try:
        print(token_req)
        token_response = urllib.request.urlopen(token_req)

        # See what we got back.  If it's this part of  the code it was OK
        full_response = token_response.read()

        # Need to pick out the access token and write it to the config file.  Use a JSON manipulation module
        response_json = json.loads(full_response)

        # Read the access token as a string
        new_access_token = str(response_json['access_token'])
        new_refresh_token = str(response_json['refresh_token'])

        write_config(new_access_token, new_refresh_token, name_chose)

    except urllib.error.URLError as e:
        # Getting to this part of the code means we got an error
        print("An error was raised when getting the access token.  Need to stop here")
        print(e.code)
        print(e.read())
        sys.exit()


# This makes an API call. It also catches errors and tries to deal with them
def make_API_call(acc_token, ref_token, client_ID, secret_key, name_chose):
    # Start the request
    req = urllib.request.Request("https://api.fitbit.com/1/user/-/profile.json")
    # Add the access token in the header
    req.add_header('Authorization', 'Bearer ' + acc_token)

    # Fire off the request
    try:
        response = urllib.request.urlopen(req)
        full_response = response.read()
        return True, full_response

    # Catch errors, e.g. A 401 error that signifies the need for a new access token
    except urllib.error.HTTPError as e:
        # See what the error was
        if e.code == 401:
            get_new_access_token(ref_token, client_ID, secret_key, name_chose)
            return False, "Token refreshed OK"
        # Return that this didn't work, allowing the calling function to handle it
        return False, "Error when making API call that I couldn't handle"
