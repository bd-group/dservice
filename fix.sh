#!/bin/bash

./mstool.sh -r localhost -p FIXME_PORT -ld | grep 'node' | sed -e 's/\(-node.*\)-prop -1/.\/mstool.sh -r localhost -p FIXME_PORT -md \1 -prop 0/g' > .TMP.fix
while read line
do
	echo $line
	$line
done < .TMP.fix
rm -rf .TMP.fix
