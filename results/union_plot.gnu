set term eps
set output "union_plot.eps"
set style data histograms
set style histogram rowstacked
set boxwidth 0.3 absolute
set style fill solid 1.0 border -1
set ylabel 'Extraction Time (minutes)'
plot 'day1_union.dat' using ($2/60000) t "Union (algo1)", '' using ($3/60000):xticlabels(1) t "cs2", '' using ($4/60000):xticlabels(1) t "View min", '' using ($5/60000):xticlabels(1) t "where", '' using ($6/60000):xticlabels(1) t "projection", '' using ($7/60000):xticlabels(1) t "group by",  '' using ($8/60000):xticlabels(1) t "agg", '' using ($9/60000):xticlabels(1) t "order by", '' using ($10/60000):xticlabels(1) t "limit" lc "coral"