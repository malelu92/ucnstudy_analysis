#!/bin/sh
DATA=/home/apietila/work/dataprojects/ucnstudy/data/service-names-port-numbers.txt
PORT=$1
PROTO=$2

RES=`egrep ' '$PORT' .*'$PROTO -m 1 $DATA | awk --field-separator '\\\s{2,}' '{ print $4 }'`

echo $RES

exit 0
