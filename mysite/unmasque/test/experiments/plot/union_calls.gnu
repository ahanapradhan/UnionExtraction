# Set terminal to PNG format for saving the output as an image
set terminal pngcairo size 800,300 enhanced font 'Arial,18'
set output 'bar_plot.png'

# Set title and labels
set xlabel "QID"
set ylabel "# Q_H Executions"

# Set grid for better readability
set grid

# Set box width and style for bars with gaps (set boxwidth to a value less than 1)
set style data histograms
set style fill solid 1.0 border -1
set boxwidth 0.8 relative  # Use 0.8 relative to default, creating space between bars

# Set y-axis range to start from 0
set yrange [0:10]

# Rotate x-axis labels by 45 degrees and offset them downward
set xtics rotate by 45 offset 0,-1

# Define the data inline
plot '-' using ($2-9):xtic(1) title "Union Algorithm" with boxes

# Data to plot
1 13
2 14
3 13
4 14
5 18
6 13
7 17
8 16
9 18
10 16
11 12
12 14
13 12
14 14
15 14
16 12
17 11
18 12
19 11
20 14
21 11
22 11
23 10
24 12