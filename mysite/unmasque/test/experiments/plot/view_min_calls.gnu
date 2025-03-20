# Set terminal to PNG format for saving the output as an image
set terminal pngcairo size 800,400 enhanced font 'Arial,18'
set output 'view_plot.png'

# Set title and labels
set xlabel "QID"
set ylabel "# Q_H Executions"

# Set grid for better readability
set grid
# Set legend (key) in the top-left corner
set key top left

# Set box width and style for bars with gaps (set boxwidth to a value less than 1)
set style data histograms
set style fill solid 1.0 border -1
set boxwidth 0.8 relative  # Use 0.8 relative to default, creating space between bars

# Set y-axis range to start from 0
set yrange [0:*]

# Rotate x-axis labels by 45 degrees and offset them downward
set xtics rotate by 45 offset 0,-1

# Plot the data with red-colored bars
plot '-' using 2:xtic(1) title "Database Minimization"  with boxes ls 1

# Data to plot
1	52
2	65
3	52
4	96
5	176
6	52
7	420
8	114
9	210
10	146
11	44
12	94
13	46
14	85
15	44
16	170
17	23
18	30
19	724
20	83
21	91
22	806
23	127
24	191