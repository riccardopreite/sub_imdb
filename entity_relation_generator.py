import sys
import pandas as pd
import multiprocessing as mp
from pandas.core.frame import DataFrame
from tqdm import tqdm
CORE_NUMBER = mp.cpu_count()
GENRE_PREFIX = "genre_"
END_PREFIX = "end_"
START_PREFIX = "start_"
FILM_PREFIX = "film_"
REGION_PREFIX = "region_"
RUN_PREFIX = "run_"
HALF = 'film'
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

i = 0
def get_id():
    global i
    i+=1
    return str(i)

def run_sub_process(data, sub_process):
        step_size = len(data) // CORE_NUMBER
        pool = mp.Pool(mp.cpu_count())
        global i
        i = 0
        sub_data = [ {"id":get_id(), "data":data[index:index+step_size]} for index in range(0, len(data), step_size) ]

        for result in tqdm(pool.imap(func=sub_process, iterable=sub_data), total=len(data)):
            pass
        print("Starting sub process:",sub_process)
        
        pool.starmap(sub_process, sub_data)
        pool.close()
        pool.join()

def add_entity(prefix, entity_name):
    with open(HALF+"_new_entity_try.tsv","a+") as fd:
        unique_code = (prefix+entity_name).replace("\n","")
        fd.write(unique_code+"\t"+entity_name+"\n")
        return unique_code

def add_relation(relation: str):
     with open(HALF+"_new_relation_try.tsv","a+") as fd:
         fd.write(relation+"\n")

# def sub_entity(pid, data):
def sub_entity(dictionary):
    pid = dictionary["id"]
    full = dictionary["data"]
    print("\tSpawned sub entity with pid:",pid,"len:",len(full))
    
    gen_relation = relation_code["genres"]
    run_relation = relation_code["runtimeMinutes"]
    start_relation = relation_code["startYear"]
    end_relation = relation_code["endYear"]
    film_entity = open("film_entity.txt","r").readlines()
    array = [row for index, row in full.iterrows() if row["tconst"] in film_entity]
    data = pd.DataFrame(array)
    print("\tSub entity with pid:",pid,"now has len:",len(data))
    for index, row in data.iterrows():

        if not (index % (len(data)//4)):
            print("\t\tActual index in",pid,"is",index)

        genres_id: str = str(row["genres"])
        runtimeMinutes_id: int = str(row["runtimeMinutes"])
        endYear_id: str = str(row["endYear"])
        startYear_id: str = str(row["startYear"])
        tt_id: str = FILM_PREFIX+str(row["tconst"])
        # if tt_id in film_entity:
        if genres_id != "\\N":
            split = genres_id.split(",")
            for gen in split:
                gen_id = add_entity(GENRE_PREFIX, gen)
                relation_gen = tt_id + "\t" + gen_relation + "\t" + gen_id
                add_relation(relation_gen)

        if runtimeMinutes_id != "\\N":
            run_id = add_entity(RUN_PREFIX, runtimeMinutes_id)
            relation_run = tt_id + "\t" + run_relation + "\t" + run_id
            add_relation(relation_run)

        if endYear_id != "\\N":
            end_id = add_entity(END_PREFIX, endYear_id)
            relation_end = tt_id + "\t" + end_relation + "\t" + end_id
            add_relation(relation_end)

        if startYear_id != "\\N":
            start_id = add_entity(START_PREFIX, startYear_id)
            relation_start = tt_id + "\t" + start_relation + "\t" + start_id
            add_relation(relation_start)
    print("\tFinished entity with pid",pid)

def create_attributes_entity():
    attributes_file: DataFrame = pd.read_csv('basics.tsv',sep='\t')
    del attributes_file["titleType"]
    del attributes_file["primaryTitle"]
    del attributes_file["originalTitle"]
    del attributes_file["isAdult"]
    print('Readed basics.tsv')
    if HALF == 'first':
        start = 0
        end = len(attributes_file)//4
    elif HALF == 'second':
        start = len(attributes_file)//4
        end = len(attributes_file)//4*2
    elif HALF == 'third':
        start = len(attributes_file)//4*2
        end = len(attributes_file)//4*3
    elif HALF == 'fourth':
        start = len(attributes_file)//4*3
        end = len(attributes_file)
    print("Running entity",HALF,"half with size:",str(len(attributes_file[start:end])))
    run_sub_process(attributes_file[start:end], sub_entity)

# def sub_region(pid, data):
def sub_region(dictionary):
    pid = dictionary["id"]
    full = dictionary["data"]
    print("\tSpawned sub region with pid:",pid,"len:",len(full))
    relation_id: str = relation_code["region"]
    film_entity = open("film_entity.txt","r").readlines()
    array = [row for index, row in full.iterrows() if row["titleId"] in film_entity]
    data = pd.DataFrame(array)
    print("\tSub region with pid:",pid,"now has len:",len(data))

    for index, row in data.iterrows():
        region_id: str = str(row["region"])
        is_original: bool = bool(row["isOriginal"])
        tt_id: str = FILM_PREFIX+str(row["titleId"])
        if region_id != "\\N" and is_original:
            re_id = add_entity(REGION_PREFIX, region_id)
            relation_region = tt_id + "\t" + relation_id + "\t" + re_id
            add_relation(relation_region)



        # if not (index % (len(data)//4)):
        #     print("\t\tActual index in",pid,"is",index)
        # if tt_id in film_entity:
        #     if region_id != "\\N" and is_original:
        #         re_id = add_entity(REGION_PREFIX, region_id)
        #         relation_region = tt_id + "\t" + relation_id + "\t" + re_id
        #         add_relation(relation_region)
            
    print("\tFinished region with pid",pid)

def create_region_entity():
    region_file: DataFrame = pd.read_csv('akas.tsv',sep='\t')
    del region_file["ordering"]
    del region_file["title"]
    del region_file["language"]
    del region_file["types"]
    del region_file["attributes"]
    print('Readed akas.tsv')
    if HALF == 'first':
        start = 0
        end = len(region_file)//4
    elif HALF == 'second':
        start = len(region_file)//4
        end = len(region_file)//4*2
    elif HALF == 'third':
        start = len(region_file)//4*2
        end = len(region_file)//4*3
    elif HALF == 'fourth':
        start = len(region_file)//4*3
        end = len(region_file)
        print("Running region",HALF,"half with size:",str(len(region_file[start:end])))

    run_sub_process(region_file[start:end], sub_region)
film_set = set()

def create_film_entity(path: str):
    global HALF
    ttconst = open(path,'r')
    print('Readed ',path)
    tt_list: list = ttconst.readlines()
    print("Running film with size:",str(len(tt_list)))
    sub_film(1, tt_list)

def sub_film(id, tt_list):
    print("\tSpawned sub film with id",id)
    global film_set
    for url in tt_list:
        tt_id: str = url.replace("http://www.imdb.com/title/","").replace("/usercomments","")

        if tt_id != "\\N":
            film_set.update(FILM_PREFIX+tt_id+"\t"+tt_id)
            #add_entity(FILM_PREFIX, tt_id)

def main():
    global HALF
    HALF = sys.argv[1] if sys.argv[1] != None else "film"
    if HALF == "film":
        create_film_entity("train/urls_pos.txt")
        create_film_entity("train/urls_neg.txt")
        create_film_entity("test/urls_pos.txt")
        create_film_entity("test/urls_neg.txt")
        film_entity = open("film_entity.txt","w+")
        film_entity.writelines(film_set)
    else:
        create_attributes_entity()
        create_region_entity()
        
if __name__ == "__main__":
    main()
