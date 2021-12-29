import sys
import pandas as pd
import multiprocessing as mp
from pandas.core.frame import DataFrame

CORE_NUMBER = mp.cpu_count()
GENRE_PREFIX = "genre_"
END_PREFIX = "end_"
START_PREFIX = "start_"
FILM_PREFIX = "film_"
REGION_PREFIX = "region_"
RUN_PREFIX = "run_"
HALF = 'first'
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
        sub_data = [ (get_id(), data[index:index+step_size]) for index in range(0, len(data), step_size) ]
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

def sub_entity(pid, data):
    print("\tSpawned sub entity with pid:",pid,"len:",len(data))
    
    gen_relation = relation_code["genres"]
    run_relation = relation_code["runtimeMinutes"]
    start_relation = relation_code["startYear"]
    end_relation = relation_code["endYear"]

    for index, row in data.iterrows():

        if not (index % (len(data)//4)):
            print("\t\tActual index in",pid,"is",index)
        genres_id: str = str(row["genres"])
        runtimeMinutes_id: int = str(row["runtimeMinutes"])
        endYear_id: str = str(row["endYear"])
        startYear_id: str = str(row["startYear"])
        id: str = str(row["tconst"])
        tt_id = add_entity(FILM_PREFIX, id)
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

def sub_region(pid, data):
    print("\tSpawned sub region with pid:",pid,"len:",len(data))
    relation_id: str = relation_code["region"]

    for index, row in data.iterrows():
        
        if not (index % (len(data)//4)):
            print("\t\tActual index in",pid,"is",index)
        region_id: str = str(row["region"])
        id: str = str(row["titleId"])
        tt_id = add_entity(FILM_PREFIX, id)
        if region_id != "\\N":
            re_id = add_entity(REGION_PREFIX, region_id)
            
            relation_region = tt_id + "\t" + relation_id + "\t" + re_id
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


def main():
    global HALF
    HALF = sys.argv[1]
    create_attributes_entity()
    create_region_entity()
        
if __name__ == "__main__":
    main()
