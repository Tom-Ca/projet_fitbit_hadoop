import xmltodict


from menu import *


def xml_to_json():

    list_dosier = [ f for f in os.listdir('./Ressources/activities/') if not os.path.isfile(os.path.join('./Ressources/activities/',f)) ]
    list_tcx = os.listdir('./Ressources/file/')

    for dissier in list_dosier:
        list_toXML = []
        list_file = os.listdir('./Ressources/activities/' + dissier + "/")
        for l1 in list_tcx:
            with open('./Ressources/file/' + l1, 'r') as myfile:
                content = myfile.read()
                obj = xmltodict.parse(content)
                activitie_of_file = obj["TrainingCenterDatabase"]["Activities"]["Activity"]["@Sport"]
            if l1[:-4] + '.json' not in list_file and activitie_of_file == dissier:
                list_toXML.append(l1)
        if list_toXML:
            for i in list_toXML:
                with open('Ressources/file/' + i, 'r') as myfile:
                    obj = xmltodict.parse(myfile.read())
                fichier = open('Ressources/activities/' + dissier + "/" + i[:-4] + '.json', "a")
                fichier.write('[')
                fichier.write(json.dumps(obj))
                fichier.write(']')
                fichier.close()
        print(len(list_toXML) , "fichier creer dans" , dissier)

    for file_tcx in list_tcx:
        with open('./Ressources/file/' + file_tcx, 'r') as myfile:
            content = myfile.read()
            obj = xmltodict.parse(content)
            activitie_of_file = obj["TrainingCenterDatabase"]["Activities"]["Activity"]["@Sport"]
            if activitie_of_file not in list_dosier:
                print(activitie_of_file)
                if not os.path.exists('Ressources/activities/' + activitie_of_file):
                    os.makedirs('Ressources/activities/' + activitie_of_file)
                    print("nouveaux dossier creer : ", activitie_of_file)

                fichier = open('Ressources/activities/' + activitie_of_file + "/" + file_tcx[:-4] + '.json', "a")
                fichier.write('[')
                fichier.write(json.dumps(obj))
                fichier.write(']')
                fichier.close()
                print("nouveaux fichier creer dans", activitie_of_file)


def modif_activities():
    list_file = os.listdir('./Ressources/activities/Running')
    # print(list_file)
    for file_tcx in list_file:
        with open('./Ressources/activities/Running/' + file_tcx, 'r') as myfile:
            # print(file_tcx)
            content = (json.loads(myfile.read()))
            try:
                for i in range(len(content[0]["TrainingCenterDatabase"]["Activities"]["Activity"]["Lap"]["Track"]["Trackpoint"])):
                    if "Position" in content[0]["TrainingCenterDatabase"]["Activities"]["Activity"]["Lap"]["Track"]["Trackpoint"][i]:
                        x = (content[0]["TrainingCenterDatabase"]["Activities"]["Activity"]["Lap"]["Track"]["Trackpoint"][i]["Position"]["LatitudeDegrees"])
                        y = (content[0]["TrainingCenterDatabase"]["Activities"]["Activity"]["Lap"]["Track"]["Trackpoint"][i]["Position"]["LongitudeDegrees"])
                        content[0]["TrainingCenterDatabase"]["Activities"]["Activity"]["Lap"]["Track"]["Trackpoint"][i]["LatitudeDegrees"] = x
                        content[0]["TrainingCenterDatabase"]["Activities"]["Activity"]["Lap"]["Track"]["Trackpoint"][i]["LongitudeDegrees"] = y
                        content[0]["TrainingCenterDatabase"]["Activities"]["Activity"]["Lap"]["Track"]["Trackpoint"][i].pop("Position")

                    if "HeartRateBpm" in content[0]["TrainingCenterDatabase"]["Activities"]["Activity"]["Lap"]["Track"]["Trackpoint"][i]:
                        z = (content[0]["TrainingCenterDatabase"]["Activities"]["Activity"]["Lap"]["Track"]["Trackpoint"][i]["HeartRateBpm"]["Value"])
                        content[0]["TrainingCenterDatabase"]["Activities"]["Activity"]["Lap"]["Track"]["Trackpoint"][i]["HeartRateBPM"] = z
                        content[0]["TrainingCenterDatabase"]["Activities"]["Activity"]["Lap"]["Track"]["Trackpoint"][i].pop("HeartRateBpm")
            except:
                if len(content[0]["TrainingCenterDatabase"]["Activities"]["Activity"]["Lap"]) != 7:
                    for j in range(len(content[0]["TrainingCenterDatabase"]["Activities"]["Activity"]["Lap"])):
                        for i in range(len(content[0]["TrainingCenterDatabase"]["Activities"]["Activity"]["Lap"][j]["Track"]["Trackpoint"])):
                            if "Position" in content[0]["TrainingCenterDatabase"]["Activities"]["Activity"]["Lap"][j]["Track"][ "Trackpoint"][i]:
                                x = (content[0]["TrainingCenterDatabase"]["Activities"]["Activity"]["Lap"][j]["Track"][
                                    "Trackpoint"][i]["Position"]["LatitudeDegrees"])
                                y = (content[0]["TrainingCenterDatabase"]["Activities"]["Activity"]["Lap"][j]["Track"][
                                    "Trackpoint"][i]["Position"]["LongitudeDegrees"])
                                content[0]["TrainingCenterDatabase"]["Activities"]["Activity"]["Lap"][j]["Track"][
                                    "Trackpoint"][i]["LatitudeDegrees"] = x
                                content[0]["TrainingCenterDatabase"]["Activities"]["Activity"]["Lap"][j]["Track"][
                                    "Trackpoint"][i]["LongitudeDegrees"] = y
                                content[0]["TrainingCenterDatabase"]["Activities"]["Activity"]["Lap"][j]["Track"][
                                    "Trackpoint"][i].pop("Position")
                            if "HeartRateBpm" in \
                                    content[0]["TrainingCenterDatabase"]["Activities"]["Activity"]["Lap"][j]["Track"][
                                        "Trackpoint"][i]:
                                z = (content[0]["TrainingCenterDatabase"]["Activities"]["Activity"]["Lap"][j]["Track"][
                                    "Trackpoint"][i]["HeartRateBpm"]["Value"])
                                content[0]["TrainingCenterDatabase"]["Activities"]["Activity"]["Lap"][j]["Track"][
                                    "Trackpoint"][i]["HeartRateBPM"] = z
                                content[0]["TrainingCenterDatabase"]["Activities"]["Activity"]["Lap"][j]["Track"][
                                    "Trackpoint"][i].pop("HeartRateBpm")
                pass
                # print(content)

        fichier = open('./Ressources/activities/Running/' + file_tcx, "w")
        fichier.write(json.dumps(content))
        fichier.close()

if __name__ == '__main__':
    path = user_choice()
    xml_to_json()
    modif_activities()

