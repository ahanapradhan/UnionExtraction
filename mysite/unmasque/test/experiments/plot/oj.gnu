set term eps font "Helvetica,22"
set ytics font "Helvetica,28"
set ylabel font "Helvetica,28"
set output "../figs/oj.eps"
set boxwidth 0.5
set style fill solid
set ytics 2
set grid
show grid
set size 1,0.8
set ylabel "Extraction Time (m)"
plot "../data/oj.dat" using 1:($3/60):xtic(2) with boxes notitle
