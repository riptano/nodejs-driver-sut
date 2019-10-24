#!/usr/bin/env bash

########
#
# Usage sample
# ./charts/generate-throughput.sh sample-folder1 "Sample 1" sample-folder2 "Sample 2" 128,256,512,1024 a-vs-b
#
########

DIR1=${1:-sample1}
LABEL1=${2:-label 1}
DIR2=${3:-sample1}
LABEL2=${4:-label 2}
REQUESTS=${5}
FILENAME=${6:-throughput}
REQUEST_PARAMS=""

IFS=','
read -a REQUESTS_ARR <<< "$REQUESTS"
REQUESTS_LENGTH=${#REQUESTS_ARR[@]}

for (( i=0; i<${REQUESTS_LENGTH}; i++ ));
do
  REQUEST_PARAMS="${REQUEST_PARAMS}REQUESTS_${i}=${REQUESTS_ARR[i]};"
done

GNUPLOT_PARAMS="DIR1='$DIR1';DIR2='$DIR2';LABEL1='$LABEL1';LABEL2='$LABEL2';FILENAME='$FILENAME'"
GNUPLOT_PARAMS="${GNUPLOT_PARAMS};REQUESTS_LENGTH=${REQUESTS_LENGTH};${REQUEST_PARAMS}"

gnuplot -e $GNUPLOT_PARAMS charts/compare-throughput.gnuplot
