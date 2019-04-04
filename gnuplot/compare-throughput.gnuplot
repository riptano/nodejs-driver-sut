MIN_Y=40000

set term png size 1600,1200
set output 'boxplot-read-throughput.png'
set title "Per-driver throughput vs. number of concurrent requests" font 'Arial,20'
set noxtics
set ytics nomirror
set yrange [MIN_Y:]
set key left
set bmargin 2
set style fill solid 0.25 border lt -1
set style data boxplot
set style boxplot nooutliers
set bars 0.2
set boxwidth 0.1
set border 2
set label '100 requests' at 1,(MIN_Y+1000) rotate center font 'Arial,12'
set label '200 requests' at 2,(MIN_Y+1000) rotate center font 'Arial,12'
set label '300 requests' at 3,(MIN_Y+1000) rotate center font 'Arial,12'
set label '400 requests' at 4,(MIN_Y+1000) rotate center font 'Arial,12'
set label '500 requests' at 5,(MIN_Y+1000) rotate center font 'Arial,12'
set label '600 requests' at 6,(MIN_Y+1000) rotate center font 'Arial,12'
set xrange [0:6.5]
plot \
    'a.txt' using (0.9):2:(0.1):1 title '4.0.0' ,\
    'a.txt' using (1):2:(0.1):1 title '3.5.
