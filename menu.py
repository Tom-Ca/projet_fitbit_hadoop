import os
from user_functions import *
from token_verification_functions import *
from activities_functions import *
import fitbit


def user_choice():
    client_ID = ''
    secret_key = ''
    path = "Ressources/"

    name_chose = "Tom Careghi"
    # print("Vous avez choisi :", name_chose)
    path += name_chose + "/"
    # Mise en place du user
    config = get_config(name_chose)

    # Verification du token du user
    API_call_OK, API_response = make_API_call(config["AccToken"], config["RefToken"], client_ID, secret_key, name_chose)
    if not API_call_OK:
        if API_response == "Token refreshed OK":
            config = get_config(name_chose)
        else:
            print("Error when making API call that I couldn't handle")

    # Connexion
    client = fitbit.Fitbit(
        client_ID,  # Application client ID
        secret_key,  # Application secret key
        access_token=config["AccToken"],  # Access_token here
        refresh_token=config["RefToken"]  # Refresh token here
    )

    # Récupère la liste des activités a telecharger
    # list_sleep = recover_number_of_files_sleep_since_today(1000, client)
    # list_activities = recover_number_of_files_since_today(1000,client)
    # list_activities_heart = recover_number_of_files_activities_heart_since_today(client)
    list_activities_heart =recover_all_data_activities_heart_since_the_last_time(client)
    list_activities = recover_all_data_since_the_last_time(client)
    # list_sleep =recover_all_data_sleep_since_the_last_time(client)
    # print('list heart : ', list_activities_heart)
    # Télécharge les TCX
    if list_activities != []:
        retrieve_and_create_tcx(list_activities, config)
    else:
        print("Aucune nouvelle activité")

    # if list_sleep != []:
    #     retrieve_sleep_and_create_json(list_sleep, client)
    # else:
    #     print("Aucune nouvelle donnée de someille")

    if list_activities_heart != []:
        retrieve_activities_heart_and_create_json(list_activities_heart,client)
    else:
        print("Aucune nouvelle donnée cardiac")

    return path

