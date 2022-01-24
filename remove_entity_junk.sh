VARIABLE="entity.txt"
grep --perl-regex "[A-Z|a-z|0-9]+[\t][A-Z|a-z|0-9]+" $VARIABLE > "new_"$VARIABLE
