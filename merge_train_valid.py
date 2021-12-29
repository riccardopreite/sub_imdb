from os import listdir
from os.path import isfile, join

only_train_files: list = [f for f in listdir("./") if isfile(join("./", f)) and "train" in f]
only_valid_files: list = [f for f in listdir("./") if isfile(join("./", f)) and "valid" in f]

def merge_file(name):
    files: list = [f for f in listdir("./") if isfile(join("./", f)) and name in f]
    merged: str = ""
    for file in files:
        with open(file, 'r+') as readfile:
            merged += readfile.read()
    output = open("relation_"+name+".txt","w+")
    output.writelines(merged)

merge_file("train")
merge_file("valid")
