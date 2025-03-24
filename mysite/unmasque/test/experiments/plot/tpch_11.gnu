
set terminal pngcairo size 1000,500 enhanced font 'Arial,20'
set output 'stacked_barplot.png'

set xlabel "QID" font ", 24"
set ylabel "Extraction Time (Minutes)" font ", 24"
set key top center
set style data histogram
set style histogram rowstacked
set style fill solid border -1
set style fill pattern 4 border -1  # Set a texture for the first group
set boxwidth 0.5
set grid ytics

set xtics rotate by -45
set xtics nomirror
set ytics nomirror

# Define colors and styles
set style line 1 lc rgb "#8a2be2"  # FROM (Violet) - with texture
set style line 2 lc rgb "#ff7f0e"  # DB Minimization (Orange) - with texture
set style line 3 lc rgb "#2ca02c"  # XRE-others (Green) - with texture
set style line 4 lc rgb "#d62728"  # XFE-LLM (Red) - different texture
set style line 5 lc rgb "#FFD700"  # XFE-Comb (Yellow) - different texture

# Define inline data
$DATA << EOD
2   5   5    10  240  126
7   5   313  5   240  0
8   17  19.5 20  240  0
11  13  15   8   240  0
12  8.5 9.5  7   240  0
13  12  8    3   136  32
15  8   27   6   240  0
16  13  50   4   120  0
17  10  21   5   120  0
18  15  150  40  240  0
20  5   54   25  160  0
21  2   27   15  120  0
22  8   425  30  240  0
EOD

# Plot the stacked bars
# Plot the stacked bars (Dividing values by 60 to convert seconds to minutes)
plot $DATA using ($2/60):xtic(1) title "FROM" ls 1 with histogram fill pattern 25, \
     '' using ($3/60) title "DB Minimization" ls 2 with histogram fill pattern 25, \
     '' using ($4/60) title "XRE-others" ls 3 with histogram fill pattern 25, \
     '' using ($5/60) title "XFE-LLM" ls 4 with histogram fill pattern 2, \
     '' using ($6/60) title "XFE-Comb" ls 5 with histogram fill pattern 2
