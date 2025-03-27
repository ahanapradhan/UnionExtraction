# Set terminal to PNG format for saving the output as an image
set terminal pngcairo size 800,300 enhanced font 'Arial,18'
set output 'calls_plot.png'

# Set title and labels
set xlabel "QID"
set ylabel "# Q_H Executions"

# Set grid for better readability
set grid
# Set legend (key) in the top-left corner
unset key

# Set box width and style for bars with gaps (set boxwidth to a value less than 1)
set style data histograms
set style fill solid 1.0 border -1
set boxwidth 0.8 relative  # Use 0.8 relative to default, creating space between bars

# Set y-axis range to start from 0
set yrange [0:*]
set ytics 400

# Rotate x-axis labels by 45 degrees and offset them downward
set xtics rotate by 45 offset 0,-1

# Plot the data with red-colored bars
plot '-' using ($2+$3+750):xtic(1) title "Database Minimization"  with boxes ls 1

# Data to plot
1	52  13
2	48  14
3	52  13
4	96  14
5	176 18
6	52  13
7	420 17
8	114 16
9	210 18
10	146 16
11	44  12
12	94  14
13	33  12
14	85  14
15	44  14
16	170 12
17	23  11
18	30  12
19	724 11
20	67  14
21	91  11
22	806 11
23	127 10
24	191 11