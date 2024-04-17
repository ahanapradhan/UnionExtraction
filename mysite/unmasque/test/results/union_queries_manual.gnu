set term eps
set output "union_queries_plot.eps"
set style data histograms
set style histogram rowstacked
set boxwidth 0.4 relative
set style fill solid 1.0 border -1
set ylabel 'Extraction Time (ms)'
plot 'union_queries_Q3.dat' using 2 t "exe" lc "black" ,'' using 3:xticlabels(1) t "Union (algo1)" lc "goldenrod", '' using 4:xticlabels(1) t "cs2" lc "magenta", '' using 5:xticlabels(1) t "View min" lc "dark-yellow", '' using 6:xticlabels(1) t "where" lc "blue", '' using 7:xticlabels(1) t "projection" lc "red", '' using 8:xticlabels(1) t "group by" lc "web-green",  '' using 9:xticlabels(1) t "agg" lc "aquamarine", '' using 10:xticlabels(1) t "order by" lc "brown", '' using 11:xticlabels(1) t "limit" lc "coral"
