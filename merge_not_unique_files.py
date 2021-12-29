from os import listdir
from os.path import isfile, join

def print_merged_file(path_list: list, output: str):
    merged: str = ""
    for path in path_list:
        f = open(path,"r")
        merged+=f.read()
    unique = list(set(merged))
    out = open(output,"w+")
    out.writelines(unique)
    print("writed",output)

if __name__ == "__main__":
    
    relation: list = [f for f in listdir("./") if isfile(join("./", f)) and "relation" in f]
    entity: list = [f for f in listdir("./") if isfile(join("./", f)) and "entity" in f]
    print_merged_file(relation,"relation_new.tsv")
    print_merged_file(entity,"entity_new.tsv")