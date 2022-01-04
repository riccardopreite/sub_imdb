import random
import multiprocessing as mp



CORE_NUMBER = mp.cpu_count()
i = 0
def get_id():
    global i
    i+=1
    return str(i)

def read_write(pid, data: list):
    length = len(data)
    print("Started pid",str(pid),"with len",str(length))
    copied = data.copy()
    valid_size = int(length*0.25)
    valid_list = []
    list_index = []

    for i in range(0,valid_size):
        if not (i % (valid_size//4)):
            print("\t\tPid",str(pid),"reached",str(i))
        valid_list.append(copied.pop(random.randrange(len(copied))))
    print("\tDone sampled",str(len(valid_list)))
    print("\tDone copied",str(len(copied)))
    train = open("relation_train_"+str(pid)+".txt","w+")
    valid = open("relation_valid_"+str(pid)+".txt","w+")

    train.writelines(copied)
    valid.writelines(valid_list)
    print("Writed pid",str(pid))


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

if __name__ == "__main__":
    f = open("relation_new.tsv","r")
    print("Relation opened")
    lines = f.readlines()
    print("Readed lines")
    run_sub_process(lines, read_write)
