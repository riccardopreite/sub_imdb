import sys
name = sys.argv[1]
relation=open(name,"r").readlines()
print("readed",name)
new_relation = open("new_"+name,"w+")
to_remove = open("to_remove_"+name,"r").readlines()
for rem in to_remove:
    relation.remove(rem)
    #print("removed",rem)
new_relation.writelines(relation)
