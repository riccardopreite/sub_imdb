VARIABLE="entity.txt"
grep -v --perl-regex "[A-Z|a-z|0-9]+[\t][A-Z|a-z|0-9]+" $VARIABLE > "to_remove_"$VARIABLE
