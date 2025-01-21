set term eps font "Helvetica,12"
set ytics font "Helvetica,18"
set ylabel font "Helvetica,18"
set xlabel font "Helvetica,18"
set output 'figs/barchart_with_qid.eps'

# Title and labels
set xlabel "QID"
set ylabel "Difficulty Metric"

# Grid settings
set grid ytics lc rgb "#bbbbbb" lw 1 lt 0

# Style for bar chart
set style data histogram
set style histogram cluster gap 1
set style fill solid
set boxwidth 0.7 relative

# X-axis settings
set xtics rotate by -45 font ",8"
set xtics nomirror
set xtic auto

set yrange [0:50]

# Key/legend
set key outside top center horizontal

# Plotting the data
plot 'data/gpt_dat.dat' using 2:xticlabels(stringcolumn(1)) title "Total" with histogram, \
     '' using 4 title "Diff" with histogram, \
     '' using 5:xtic(1) title "#Prompts" with linespoints lw 2 pt 7 lc rgb "red"