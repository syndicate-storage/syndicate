set title ""
set style line 3 lt 3 lc rgb "black"
set style fill solid 1 border lt -1
set style histogram errorbars gap 2 lw 2
set style data histogram
#set boxwidth 0.2
set yrange [0.0:1.0]
set xlabel "Filesystem"
set ylabel "Milliseconds"
set ytics 0.1
set bars front
set xtics nomirror
set ytics nomirror
#set xtic scale 0

set size 0.4,0.4
#set style line 1 lt 1 lw 50
set terminal postscript eps enhanced color "Helvetica" 7 
set out 'latency.ps'

plot "latency.dat" using 2:3:xtic(1) notitle
#plot "latency.dat" using 2:xtic(1) notitle
