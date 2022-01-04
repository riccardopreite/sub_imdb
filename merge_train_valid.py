from os import listdir
from os.path import isfile, join

only_train_files: list = [f for f in listdir("./") if isfile(join("./", f)) and "train" in f]
only_valid_files: list = [f for f in listdir("./") if isfile(join("./", f)) and "valid" in f]

def merge_file(name):
    files: list = [f for f in listdir("./") if isfile(join("./", f)) and name in f]
    merged: list = []
    for file in files:
        with open(file, 'r+') as readfile:
            merged += readfile.readlines()
    output = open("relation_"+name+".txt","w+")
    out = list(set(merged))
    output.writelines(out)

merge_file("train")
merge_file("valid")
