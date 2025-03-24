reset
set terminal pngcairo size 1000,750 enhanced font 'Arial,22'
set output 'dual_barplot.png'

set multiplot layout 5,1

### TOP PLOT: Stacked Bar Chart ###
set size 1, 0.55
set origin 0, 0.5
set title "(a) Overall Extraction Times" font ", 24"
set xlabel "QID" font ",24"
set ylabel "Time (Minutes)" font ",24"
set key top right
set style data histogram
set style histogram rowstacked
set style fill pattern 4 border -1
set boxwidth 0.5
set grid ytics

set xtics rotate by -45
set xtics nomirror
set ytics nomirror
set ytics 15

# Define colors
set style line 1 lc rgb "#8a2be2"  # FROM (Violet) - with texture
set style line 2 lc rgb "#ff7f0e"  # DB Minimization (Orange) - with texture
set style line 3 lc rgb "#2ca02c"  # XRE-others (Green) - with texture
set style line 4 lc rgb "#d62728"  # XFE-LLM (Red) - different texture
set style line 5 lc rgb "#FFD700"  # XFE-Comb (Yellow) - different texture

# Inline data
$DATA << EOD
1   22  18   6   120   0    13
2   5   5    10  240   126  15.2
3   36  24   20  240   0    1.3
4   2   14   10  120   0    2.5
5   2   50   30  120   0    2.5
6   19  6    3   120   0    2
7   10  726  10  240   0    4
8   17  39   20  240   0    11
9   17  82   10  120   0    7
10  15  41   6   120   0    2
11  26  48   15  240   0    2
12  17  19   12  240   0    1.8
13  12  8    3   136   32   4
14  17  37   7   240   0    1.9
15  16  52   10  240   0    2
16  13  50   4   120   0    1
17  10  21   5   120   0    1
18  15  150  40  240   0    7
19  12  343  35  0     0    1
20  5   54   25  160   0    3
21  2   27   15  120   0    1.3
22  8   425  30  240   0    2
23  11  69   30  120   0    2
24  30  221  78  120   0    3.1
EOD

# First plot (stacked bar)
plot $DATA using (($2+$3+$4)/60):xtic(1) title "XRE" ls 1 with histogram fill pattern 25, \
     '' using (($5+$6)/60) title "XFE" ls 2 with histogram fill pattern 2

### BOTTOM PLOT: New Computed Bar Chart ###
set size 1, 0.45
set origin 0, 0
set title "(b) Normalized XRE Times wrt Q_H execution times" font ",24"
set xlabel "QID" font ",24"
unset ylabel
unset key
set style fill solid
set style data histogram
set style histogram clustered gap 0.5
set boxwidth 0.5
set ytics 100
set grid ytics

# Compute and plot the new metric
plot $DATA using (($2+$3+$4)/$7):xtic(1) title "Normalized Value" with histogram lc rgb "#1f77b4"
unset multiplot
