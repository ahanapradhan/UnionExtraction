set term eps font "Helvetica,22"
set ytics font "Helvetica,28"
set ylabel font "Helvetica,28"
set output "figs/union.eps"
set boxwidth 0.5
set style fill solid
set ytics 10
set grid
show grid
plot "data/union.dat" using 1:($3/($4+$5)/2):xtic(2) with boxes notitle
