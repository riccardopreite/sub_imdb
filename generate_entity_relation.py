import pandas as pd
from os import listdir
from os.path import isfile, join
import multiprocessing as mp

from pandas.core.frame import DataFrame

CORE_NUMBER = mp.cpu_count()

'''
titleType?
AKAS
region[3]: regionName
tconst[0]: ttnumber

BASICS
genres[8]: genre1,genre2
runtimeMinutes[7]: int
endYear[6]: YYYY
startYear[5]: YYYY
tconst[0]: ttnumber
'''

relation_code: dict() = {
    "genres":           "P136", #film genre
    "runtimeMinutes":   "P3803", #size
    "endYear":          "P3416", #end period
    "startYear":        "P3415", #start period
    "region":           "P276" #region
}

relation_dict: dict = dict()
relation_list: list = list()

entity_dict: dict = dict()
entity_list: list = list()

i = 0
def get_id():
    global i
    i+=1
    return i

def run_sub_process(data, sub_process, save):
        step_size = len(data) // CORE_NUMBER
        pool = mp.Pool(mp.cpu_count())
        global i
        i = 0
        sub_data = [ (get_id(), data[index:index+step_size]) for index in range(0, len(data), step_size) ]
        print("Starting sub process:",sub_process)
        
        pool.starmap_async(sub_process, sub_data, callback=save)
        pool.close()
        pool.join()


def add_entity(id):
    if id not in entity_dict.values():
        key = str(len(entity_dict))

        rel_dict = {
            "entity_id":"IMDB"+key,
            "tt_id":id
        }
        entity_list.append(rel_dict)
        entity_dict[key] = id

def add_relation(relation: str):
    if relation not in relation_dict.values():
            key = str(len(relation_dict))
            splitted = relation.split('\t')
            rel_dict = {
                "tt_id":splitted[0],
                "relation_id":splitted[1],
                "attributes_id":splitted[2]
            }
            relation_list.append(rel_dict)
            relation_dict[key] = relation



def save_entity(self):
    print("saving entity",self)
    entity_df = pd.DataFrame(entity_list)
    entity_df.to_csv("entity.tsv",sep="\t", index=False, header=False)

def sub_entity(pid, data):
    print("\tSpawned sub entity with pid:",pid,"len:",len(data))
    gen_relation = relation_code["genres"]
    run_relation = relation_code["runtimeMinutes"]
    start_relation = relation_code["startYear"]
    end_relation = relation_code["endYear"]

    for index, row in data.iterrows():
        if not (index % 100000):
            print("Actual index in",pid,"is",index)
        genres_id: str = row["genres"]
        runtimeMinutes_id: int = row["runtimeMinutes"]
        endYear_id: str = row["endYear"]
        startYear_id: str = row["startYear"]
        tt_id: str = row["tconst"]
        if genres_id != "\\N":
            split = genres_id.split(",")
            for gen in split:
                add_entity(gen)
                relation_gen = tt_id + "\t" + gen_relation + "\t" + gen
                add_relation(relation_gen)

        if runtimeMinutes_id != "\\N":
            add_entity(runtimeMinutes_id)
            relation_run = tt_id + "\t" + run_relation + "\t" + runtimeMinutes_id
            add_relation(relation_run)

        if endYear_id != "\\N":
            add_entity(endYear_id)
            relation_end = tt_id + "\t" + end_relation + "\t" + endYear_id
            add_relation(relation_end)

        if startYear_id != "\\N":
            add_entity(startYear_id)
            relation_start = tt_id + "\t" + start_relation + "\t" + startYear_id
            add_relation(relation_start)
    print("\tFinished entity with pid",pid)
def create_attributes_entity():
    attributes_file: DataFrame = pd.read_csv('basics.tsv',sep='\t')
    del attributes_file["titleType"]
    del attributes_file["primaryTitle"]
    del attributes_file["originalTitle"]
    del attributes_file["isAdult"]
    print('Readed basics.tsv')

    run_sub_process(attributes_file, sub_entity, save_entity)



def save_region(self):
    print("Saving region",self)
    relation_df = pd.DataFrame(relation_list)
    relation_df.to_csv("relation.tsv",sep="\t", index=False, header=False)

def sub_region(pid, data):
    print("\tSpawned sub region with pid:",pid,"len:",len(data))
    relation_id: str = relation_code["region"]
    for index, row in data.iterrows():
        if not (index % 100000):
            print("Actual index in",pid,"is",index)
        region_id: str = row["region"]
        tt_id: str = row["titleId"]
        
        if region_id != "\\N":
            add_entity(region_id)
            relation_region = tt_id + "\t" + relation_id + "\t" + region_id
            add_relation(relation_region)
    print("\tFinished region with pid",pid)

def create_region_entity():
    region_file: DataFrame = pd.read_csv('akas.tsv',sep='\t')
    del region_file["ordering"]
    del region_file["title"]
    del region_file["language"]
    del region_file["types"]
    del region_file["attributes"]
    del region_file["isOriginalTitle"]
    print('Readed akas.tsv')

    run_sub_process(region_file, sub_region, save_region)
    

def end_film(self):
    print("Ended sub film",self)

def sub_film(id, tt_list):
    print("\tSpawned sub film with id",id)
    for url in tt_list:
        tt_id: str = url.replace("http://www.imdb.com/title/","").replace("/usercomments","")

        if tt_id != "\\N":
            add_entity(tt_id)

def create_film_entity(path: str):
    ttconst = open(path,'r')
    print('Readed ',path)
    tt_list: list = ttconst.readlines()
    run_sub_process(tt_list,sub_film,end_film)


            



def main():
    create_film_entity("train/urls_pos.txt")
    create_film_entity("train/urls_neg.txt")
    create_film_entity("test/urls_pos.txt")
    create_film_entity("test/urls_neg.txt")

    create_region_entity()
    create_attributes_entity()

if __name__ == "__main__":
    main()

def merge_directory(neg_directory_path: str, pos_directory_path: str, neg_label: int, pos_label: int, output_file: str):
    
    only_neg_files: list = [f for f in listdir(neg_directory_path) if isfile(join(neg_directory_path, f))]
    only_pos_files: list = [f for f in listdir(pos_directory_path) if isfile(join(pos_directory_path, f))]
    data: str = ""  
