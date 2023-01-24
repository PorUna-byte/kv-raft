#!/bin/bash
int=1
if test $# -ne 1
then
  echo "usage:./test.sh iteration"
else
  while(($int<=$1))
  do
    go test
    let "int++"
  done
fi    