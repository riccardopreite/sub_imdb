import random
from os import listdir
from os.path import isfile, join

OUTFILE: str = "imdb_labeled.txt"
NEGATIVE_LABEL: int = 0
POSITIVE_LABEL: int = 1
SPLIT_COUNTER: int = 50

TRAIN_MODE: str = "train"
TEST_MODE: str = "test"
DEV_MODE: str = "dev"

NEGATIVE_PREFIX: str = "_neg_"
POSITIVE_PREFIX: str = "_pos_"

NEGATIVE_TRAIN_DIRECTORY: str = "train/neg/"
POSITIVE_TRAIN_DIRECTORY: str = "train/pos/"

NEGATIVE_TEST_DIRECTORY: str = "test/neg/"
POSITIVE_TEST_DIRECTORY: str = "test/pos/"

def merge_directory(neg_directory_path: str, pos_directory_path: str, neg_label: int, pos_label: int, output_file: str, mode: str):

    only_neg_files: list = [f for f in listdir(neg_directory_path) if isfile(join(neg_directory_path, f))]
    only_pos_files: list = [f for f in listdir(pos_directory_path) if isfile(join(pos_directory_path, f))]
    url_pos: list = open(mode+"/urls_pos.txt", "r").readlines()
    url_neg: list = open(mode+"/urls_neg.txt", "r").readlines()
    data: str = ""
    neg_index: int = 0
    pos_index: int = 0
    for file in only_neg_files:
        with open(neg_directory_path+file, 'r+') as readfile:
            try:
                line: str = readfile.readline().replace("<br />","")
                len_lin: int = len(line)
                tt_id = url_neg[neg_index].replace("http://www.imdb.com/title/","").replace("/usercomments","")
                if len_lin > 510:
                    data += (str(neg_label) + "\t" + line[:128]+". "+ line[-382:] + "\t" + tt_id).replace("\n","") + "\n"
                else:
                    data += (str(neg_label) + "\t" + line + "\t" + tt_id).replace("\n","") + "\n"
                neg_index += 1
            except:
                print(neg_directory_path+file)

    for file in only_pos_files:
        with open(pos_directory_path+file, 'r+') as readfile:
            try:
                line: str = readfile.readline().replace("<br />","")
                len_lin: int = len(line)
                tt_id = url_pos[pos_index].replace("http://www.imdb.com/title/","").replace("/usercomments","")
                if len_lin > 510:
                    data += (str(pos_label) + "\t" + line[:128]+". "+ line[-382:] + "\t" + tt_id).replace("\n","") + "\n"
                else:
                    data += (str(pos_label) + "\t" + line + "\t" + tt_id).replace("\n","") + "\n"
                pos_index += 1
            except:
                print(pos_directory_path+file)

    with open(output_file, 'w+') as out_file:
        out_file.write(data)

    return

def shuffle_file(merged_file: str):
    lines = open(merged_file).readlines()
    random.shuffle(lines)
    open(merged_file, 'w').writelines(lines)

def merge_data(mode: str):
    merged_name: str = "replaced_"+mode+"_"+OUTFILE
    pos_directory: str = POSITIVE_TRAIN_DIRECTORY if mode == "train" else POSITIVE_TEST_DIRECTORY
    neg_directory: str = NEGATIVE_TRAIN_DIRECTORY if mode == "train" else NEGATIVE_TEST_DIRECTORY

    merge_directory(neg_directory, pos_directory, NEGATIVE_LABEL, POSITIVE_LABEL, merged_name, mode)

    shuffle_file(merged_name)

def split_test_validate(filename: str):
    with open(filename) as test_file:
        lines = test_file.readlines()
        total_lines = len(lines)
        validate_lines = int(total_lines * 0.1)

        with open("dev.txt",'w+') as validate_file:
            validate_file.writelines( lines[:validate_lines])
            with open("test.txt",'w+') as test:
                test.writelines( lines[validate_lines:])

def split_file(filename: str, dir: str):
    with open(filename,'r') as fd:
        lines = fd.readlines()
        lines_len = len(lines)
        sub_size = lines_len // 50
        start = 0
        end = sub_size
        for i in range(50):
            data = lines[start:end]
            start = end
            end = end + sub_size
            with open(dir+str(i)+"_"+filename,'w+') as sub:
                sub.writelines(data)





if __name__ == '__main__':
    print("prepering data for imdb")
    merge_data(TRAIN_MODE)
    merge_data(TEST_MODE)
    split_test_validate("replaced_"+TEST_MODE+"_"+OUTFILE)
