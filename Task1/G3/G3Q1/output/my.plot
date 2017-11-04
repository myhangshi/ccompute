set terminal png
set output "airport_rank.png"
set autoscale 

set title "Popularity Distribution of Airports";
set ylabel "Rank of Airports";
set xlabel "Airports Passengers";
set key left top
unset log y
unset log x

plot "new_data" using 1:2 title "Passengers" with linespoints


