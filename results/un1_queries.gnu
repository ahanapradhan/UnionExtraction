set term eps
set output "un1_queries_plot.eps"
set style data histograms
set style histogram rowstacked
set boxwidth 0.4 relative
set style fill solid 1.0 border -1
set ylabel 'Extraction Time (ms)'
plot 'un1_queries.dat' using 2 t "exe ",'' using 3:xticlabels(1) t "Union", '' using 4:xticlabels(1) t "From", '' using 5:xticlabels(1) t "cs2", '' using 6:xticlabels(1) t "View min", '' using 7:xticlabels(1) t "where", '' using 8:xticlabels(1) t "projection", '' using 9:xticlabels(1) t "group by",  '' using 10:xticlabels(1) t "agg", '' using 11:xticlabels(1) t "order by", '' using 12:xticlabels(1) t "limit" lc "coral"
