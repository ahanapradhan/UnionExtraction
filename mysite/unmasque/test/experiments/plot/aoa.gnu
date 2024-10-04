set term eps font "Helvetica,22"
set ytics font "Helvetica,28"
set ylabel font "Helvetica,28"
set output "figs/aoa_plot.eps"
set boxwidth 0.5
set style fill solid
set ytics 2
set grid
show grid
set ylabel "Extraction Time (m)"
plot "data/aoa.dat" using 1:($3/60):xtic(2) with boxes notitle
