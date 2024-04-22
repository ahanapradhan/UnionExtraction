set term eps
set output "union_plot.eps"
set style data histograms
set style histogram rowstacked
set boxwidth 1 relative
set style fill solid 1.0 border -1
set ylabel 'Extraction Time (ms)'
plot 'day1_union.dat' using 2 t "Union (algo1)", '' using 3:xticlabels(1) t "cs2", '' using 4:xticlabels(1) t "View min", '' using 5:xticlabels(1) t "where", '' using 6:xticlabels(1) t "projection", '' using 7:xticlabels(1) t "group by",  '' using 8:xticlabels(1) t "agg", '' using 9:xticlabels(1) t "order by", '' using 10:xticlabels(1) t "limit" lc "coral"