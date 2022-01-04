VARIABLE="relation_train.txt"
grep --perl-regex "[A-Z|a-z|0-9]+[_][A-Z|a-z|0-9]+[\t][A-Z|a-z|0-9]+[\t][A-Z|a-z|0-9]+[_][A-Z|a-z|0-9]+" $VARIABLE > "new_"$VARIABLE
