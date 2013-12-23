set title ""

set style data histogram
set style histogram errorbars gap 2 lw 2 
set style fill solid 1 border -1
set yrange [0.01:10.0]
set ytic 0.1
set xlabel "Number of Files"
set ylabel "Processing Time (s)"
set bars front
set xtics nomirror
set ytics nomirror

set logscale y 10
set size 0.4,0.65
set terminal postscript eps enhanced color "Helvetica" 7 
set out 'metadata.ps'


#plot for [COL=2:10:2] "reads.dat" using COL:COL+1:xticlabels(1) notitle
#plot for [COL=2:4:2] "metadata.dat" using COL:COL+1:xticlabels(1) notitle
#plot "metadata.dat" using 2:3:xtic(1) notitle
plot  'metadata.dat' using 2:3:xtic(1) title "Metadata reads on server" lt -1 fs pattern 1, \
      "metadata.dat" using 4:5:xtic(1) title "Metadata writes on server" lt -1 fs pattern 0
