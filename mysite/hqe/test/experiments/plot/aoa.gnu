set term eps font "Helvetica,22"
set ytics font "Helvetica,28"
set ylabel font "Helvetica,28"
set output "figs/aoa_plot.eps"
set boxwidth 0.5
set ytics 10
set style fill solid
set grid
show grid
plot "data/aoa.dat" using 1:($3/($4+$5)/2):xtic(2) with boxes notitle
