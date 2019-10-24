MIN_Y=10000
FILEPATH=sprintf("results/%s.png", FILENAME)

set term png size 1600,1200
set output FILEPATH
set title "Per-driver throughput vs. number of concurrent requests" font 'Arial,20'
set noxtics
set ytics nomirror
set yrange [MIN_Y:]
set bmargin 3
set style fill solid 0.25 border lt -1
set style data boxplot
set style boxplot nooutliers
set bars 0.2
set boxwidth 0.1
set border 2

set xrange [0:(REQUESTS_LENGTH + 0.8)]

do for [i=0:REQUESTS_LENGTH-1:1] {
    set label "".value(sprintf("REQUESTS_%d", i))." requests" at (i+1-0.1),(MIN_Y+3000) rotate center font 'Arial,14';
}

SOURCE1 = sprintf("results/%s/throughput.txt", DIR1)
SOURCE2 = sprintf("results/%s/throughput.txt", DIR2)

plot \
    SOURCE1 using (0.9):2:(0.1):1 title LABEL1 ,\
    SOURCE2 using (1):2:(0.1):1 title LABEL2

print "Chart saved in '".FILEPATH."'"