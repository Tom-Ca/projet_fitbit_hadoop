import json
from datetime import date
from os import listdir
from os.path import isfile, join
import requests


# Attention, si il n'a pas fais de sport depuis plus de 100 jours, c'est que ce n'est pas un vrais sportif, il n'a pas à voir ses données
def recover_number_of_files_since_today(nb_activities, client):
    today = date.today()
    today = today.strftime("%Y-%m-%d")
    today2 = "2021-08-04"
    all_data = []
    i = 0

    while i < nb_activities:
        url = f"{client.API_ENDPOINT}/1/user/-/activities/list.json?beforeDate={today2}&sort=desc&offset=0&limit=100"
        range_data = client.make_request(url)
        if not range_data["activities"]:
            i = nb_activities
        else:
            for activities in range_data["activities"]:
                # if activities['activityName'] == "Running":
                all_data.append(activities)
                i += 1
                today2 = activities['lastModified'][:10]
                if i >= nb_activities:
                    break
    return all_data

def recover_number_of_files_sleep_since_today(nb_activities, client):
    today = date.today()
    today = today.strftime("%Y-%m-%d")
    today2 = "2021-10-24"
    all_data = []
    i = 0

    while i < nb_activities:
        url = f"{client.API_ENDPOINT}/1/user/-/sleep/list.json?beforeDate={today2}&sort=desc&offset=0&limit=100"
        range_data = client.make_request(url)
        # print(range_data,'\n okokok')
        # all_data.append(range_data)
        if not range_data["sleep"]:
            i = nb_activities
        else:
            for activities in range_data["sleep"]:
                # if activities['activityName'] == "Running":
                all_data.append(activities['startTime'])
                i += 1
                today2 = activities['startTime'][:10]
                if i >= nb_activities:
                    break
    return all_data

def recover_number_of_files_activities_heart_since_today(client):
    all_data = []
    url = f"{client.API_ENDPOINT}/1/user/-/activities/heart/date/2022-06-11/today.json"
    range_data = client.make_request(url)
    print(range_data)
    if range_data["activities-heart"]:
        for activities in range_data["activities-heart"]:
            all_data.append(activities["dateTime"])

    return all_data

def recover_all_data_since_the_last_time(client):
    today = date.today()
    today = today.strftime("%Y-%m-%d")
    all_data = []
    files = [f for f in listdir("./Ressources/file/") if
             isfile(join("./Ressources/file/", f))]

    i = True
    while i:
        url = f"{client.API_ENDPOINT}/1/user/-/activities/list.json?beforeDate={today}&sort=desc&offset=0&limit=100"
        range_data = client.make_request(url)
        if not range_data["activities"]:
            i = False
        else:
            for activities in range_data["activities"]:
                # if activities['activityName'] == "Running":
                if compare_date(files[len(files) - 1], activities['lastModified']):
                    all_data.append(activities)
                    today = activities['lastModified'][:10]
                else:
                    i = False

    return all_data

def recover_all_data_sleep_since_the_last_time(client):
    today = date.today()
    today = today.strftime("%Y-%m-%d")
    all_data = []
    files = [f for f in listdir("./Ressources/sleep/") if
             isfile(join("./Ressources/sleep/", f))]

    i = True
    while i:
        url = f"{client.API_ENDPOINT}/1/user/-/sleep/list.json?beforeDate={today}&sort=desc&offset=0&limit=100"
        range_data = client.make_request(url)
        if not range_data["sleep"]:
            i = False
        else:
            for activities in range_data["sleep"]:
                # if activities['activityName'] == "Running":
                if compare_date(files[len(files) - 1], activities['startTime']):
                    all_data.append(activities['startTime'])
                    today = activities['startTime'][:10]
                else:
                    i = False

    return all_data

def recover_all_data_activities_heart_since_the_last_time(client):
    all_data = []

    files = [f for f in listdir("./Ressources/activities_heart/") if isfile(join("./Ressources/activities_heart/", f))]

    last_date = files[len(files) - 1][:-5]
    url = f"{client.API_ENDPOINT}/1/user/-/activities/heart/date/{last_date}/today.json"
    range_data = client.make_request(url)
    if range_data["activities-heart"]:
        for activities in range_data["activities-heart"]:
            all_data.append(activities["dateTime"])

    return all_data

def compare_date(file, last_modified):
    file2 = file[:4] + file[5:7] + file[8:10] + file[11:13] + file[14:16] + file[17:19]
    last_modified2 = last_modified[:4] + last_modified[5:7] + last_modified[8:10] + last_modified[
                                                                                    11:13] + last_modified[
                                                                                             14:16] + last_modified[
                                                                                                      17:19]
    if int(file2) < int(last_modified2):
        return True
    elif int(file2) >= int(last_modified2):
        return False


def retrieve_and_create_tcx(list_activities, config):
    i = 0
    all_tcx = []
    for data in list_activities:
        i += 1

        url = f"https://api.fitbit.com/1/user/-/activities/{data['logId']}.tcx?"
        header = {'Authorization': 'Bearer {}'.format(config["AccToken"])}
        response = requests.get(url, headers=header)
        range_data2 = response.text
        all_tcx.append(range_data2)

        # ini_file = "./Ressources/file/" + data['lastModified'][:10] + "_" + data['lastModified'][11:13] + "h" + data[
        #                                                                                                             'lastModified'][
        #                                                                                                         14:16] + "m" + \
        #            data['lastModified'][17:19] + "s" + ".tcx"
        # file_obj = open(ini_file, 'w')
        # file_obj.write(range_data2 + "\n")
        # file_obj.close()

        ini_file = "./Ressources/activity_log/" + data['lastModified'][:10] + "_" + data['lastModified'][11:13] + "h" + data['lastModified'][14:16] + "m" + \
                   data['lastModified'][17:19] + "s" + ".json"
        file_obj = open(ini_file, 'w')
        file_obj.write("[")
        file_obj.write(json.dumps(data))
        file_obj.write("]")
        file_obj.close()

def retrieve_sleep_and_create_json(list_sleep,client):
    i = 0
    for data in list_sleep:
        i += 1

        url = f"https://api.fitbit.com/1/user/-/sleep/date/{data[:10]}.json"
        range_data = client.make_request(url)

        ini_file = "./Ressources/sleep/" + data[:10] + "_" + data[11:13] + "h" + data[14:16] + "m" + \
                   data[17:19] + "s" + ".json"
        file_obj = open(ini_file, 'w')
        file_obj.write("[")
        file_obj.write(json.dumps(range_data))
        file_obj.write("]")

        file_obj.close()

def retrieve_activities_heart_and_create_json(list_activities_heart,client):
    i = 0
    for data in list_activities_heart:
        i += 1

        url = f"https://api.fitbit.com/1/user/-/activities/heart/date/{data}/1d.json"
        range_data = client.make_request(url)

        ini_file = "./Ressources/activities_heart/" + data + ".json"
        file_obj = open(ini_file, 'w')
        file_obj.write("[")
        file_obj.write(json.dumps(range_data))
        file_obj.write("]")

        file_obj.close()
