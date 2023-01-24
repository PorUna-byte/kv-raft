#!/bin/bash
int=1
if test $# -ne 1
then
  echo "usage:./test.sh iteration"
else
  while(($int<=$1))
  do
    go test -run 2A
    go test -run 2B
    go test -run 2C
    go test -run 2D
    let "int++"
  done
fi    