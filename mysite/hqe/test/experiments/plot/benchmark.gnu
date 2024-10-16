set term postscript eps enhanced color solid font "Helvetica,20"
set ytics font "Helvetica,28"
set xtics font "Helvetica,15"
set key font "Helvetica,20"
set output "figs/comparison.eps"
set auto x
set style data histogram
set style histogram cluster gap 2
set style fill solid
set boxwidth 0.9
set ytics 2
set size 1,0.6  # Reduces the height by 50%
set grid
show grid
set ylabel "Extraction Time (m)"
set key horizontal
plot 'data/languages.dat' using ($4/60):xtic(1) ti col fc rgb "#FF0000", '' u ($2/60) ti col fc rgb "#00FF00", '' u ($3/60) ti col fc rgb "#0000FF"