#!/bin/bash

#delete the last line of the src file 
sed -i '$ d' $1

#delete all blank lines and add ',' at the end of each line
grep -v "^$" $1 | awk '{print $0 ","}' > $2

#delete the last ',' and use '[...]' to make it as json array
sed -i '$s/,$//' $2
sed -i '1i[' $2
echo ] >> $2
