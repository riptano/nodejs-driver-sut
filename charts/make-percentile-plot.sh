#!/bin/sh
#
#  * Written by Gil Tene of Azul Systems, and released to the public domain,
#  * as explained at http://creativecommons.org/publicdomain/zero/1.0/
#
# This script uses gnuplot to plot the percentile distribution in the
# input files provided. run with "-h" option for an expected usage description.
#
# The script assumes the input files contain ".hgrm" formatted output such
# as the one provided by HdrHistogram. The 4th column in the input files is
# expected to be the value of 1/(1-percentile) (for a given percentile),
# and the 1st column in the input files is expected to be the value at the
# given percentile.
#

reading_SLA_NAME=0
reading_OUTPUT_NAME=0
helpFlagFound=0
SLA_NAME=
FILE1=
FILE2=
OUTPUT_FILENAME=
reading_maxvalue=0
maxvalue=

for var in $@; do
	if [ $reading_SLA_NAME -eq 1 ]; then
		SLA_NAME=$var
		reading_SLA_NAME=0
	elif [ $reading_OUTPUT_NAME -eq 1 ]; then
		OUTPUT_FILENAME=$var
		reading_OUTPUT_NAME=0
	elif [ $reading_maxvalue -eq 1 ]; then
		maxvalue="set yrange [0:$var]"
		reading_maxvalue=0
	elif [ $var = "-h" ]; then
		helpFlagFound=1
	elif [ $var = "-o" ]; then
		reading_OUTPUT_NAME=1
	elif [ $var = "-s" ]; then
		reading_SLA_NAME=1
	elif [ $var = "-m" ]; then
		reading_maxvalue=1
	else
	  if [ "$FILE1" = "" ] ; then
		  FILE1="$var"
	  else
	    FILE2="$var"
	  fi
	fi
done

message()
{
    echo "$@" >&2
}

if [ $helpFlagFound -eq 1 ]; then
	message "Usage: make-percentile-plot [-o output_file] [-s sla_file] histogram_file ..."
	exit 255
fi

IndividualFilePlotCommands="'./charts/xlabels.dat' with labels center offset 0, -1 point"
IndividualFilePlotCommands="$IndividualFilePlotCommands, '$FILE1' using 4:1 with lines title 'baseline' lw 2"
IndividualFilePlotCommands="$IndividualFilePlotCommands, '$FILE2' using 4:1 with lines title 'compare' lw 2"

if [ $SLA_NAME ]; then
	IndividualFilePlotCommands="$IndividualFilePlotCommands, '$SLA_NAME' with lines ls 1"
	message plotting "{ " $FILE1 $FILE2 " }" with SLA $SLA_NAME
else
	message plotting "{ " $FILE1 $FILE2" }"
fi

message command will be:
message $IndividualFilePlotCommands

(
    echo "#plot commands"
    echo "set terminal svg size 1280,720 enhanced font \"Arial, 16\""
    if [ $OUTPUT_FILENAME ]; then
        echo "set output '$OUTPUT_FILENAME'"
    fi
    echo "set logscale x"
    echo "unset xtics"
    echo "set title \"Latency per percentile distribution\""
    echo "$maxvalue"
    echo "set key top left"
    echo "set bmargin 3"
    echo "set xrange [*:10000]"
    echo "plot $IndividualFilePlotCommands"
) | gnuplot
