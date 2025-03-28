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
1	431
2	331
3	963
4	1266
5	848
6	589
7	9700
8	450
9	900
10	746
11	200
12	1397
13	166
14	400
15	500
16	700
17	651
18	850
19	17500
20	450
21  500
22	36000
23	127 10
24	191 11
