#!/bin/bash
int=1
if test $# -ne 2
then
  echo "usage:./test.sh iteration lab_part"
else
  while(($int<=$1))
  do
    go test -run $2
    let "int++"
  done
fi    