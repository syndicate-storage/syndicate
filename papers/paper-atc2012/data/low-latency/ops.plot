set title ""
set style line 3 lt 3 lc rgb "black"
set style fill solid 1 border lt -1
set style histogram errorbars gap 2 lw 2
set style data histogram
#set boxwidth 0.2
set ytics 1000
set yrange [0.0:14000.0]
set xlabel "Filesystem"
set ylabel "Ops/s"
set bars front
set xtics nomirror
set ytics nomirror
#set xtic scale 0

set size 0.4,0.4
set terminal postscript eps enhanced color "Helvetica" 7 
set out 'ops.ps'

plot "ops.dat" using 2:3:xtic(1) notitle
