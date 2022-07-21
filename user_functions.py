def get_config(name_chose):
    with open("tokens.txt", "r") as file_in:
        line = file_in.readline()
        while line != "":
            if line == ("Name : " + name_chose + "\n"):
                line = file_in.readline()
                access_token = line[11:len(line) - 1]
                line = file_in.readline()
                ref_token = line[11:len(line) - 1]
            line = file_in.readline()

    return {"AccToken": access_token, "RefToken": ref_token}
