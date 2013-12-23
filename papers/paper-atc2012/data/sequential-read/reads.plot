set title ""

set style data histogram
set style histogram errorbars gap 2 lw 2 
set style fill solid 1 border -1
set yrange [1.0:1200]
set ytic 0.1
set xlabel "File Size"
set ylabel "Total Transfer Time"
set bars front
set xtics nomirror
set ytics nomirror

set logscale y 10
set size 0.4,0.4
set terminal postscript eps enhanced color "Helvetica" 7 
set out 'sequential-read.ps'


#plot for [COL=2:10:2] "reads.dat" using COL:COL+1:xticlabels(1) notitle
#plot for [COL=2:6:2] "reads.dat" using COL:COL+1:xticlabels(1) notitle
plot  'reads.dat' using 2:3:xtic(1) title "No CDN" lt -1 fs pattern 1, \
      "reads.dat" using 4:5:xtic(1) title "CoBlitz" lt -1      fs       pattern 0, \
      "reads.dat" using 6:7:xtic(1) title "Hypercache" lt -1 fs       pattern 2
