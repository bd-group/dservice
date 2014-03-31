reset

set terminal png size 1600,900
set origin 0.0, 0.0
set size 1, 1
set output "FIXME_LOCATION/space.png"
set auto x
set xtics out
set auto y

set title "Space Consumption On Day FIXME_REPORT_DAY"
set xlabel "TimeStamp # (1 tic is about 65s)"
set ylabel "Space Comsumption Rate (MB/s)"
set y2label "Free Space Ratio (%)"
set xdata time
set timefmt "%s"
set format x "%H:%S"
set ytics nomirror
set y2tics nomirror
set key out
set grid

plot "< awk -F, 'BEGIN{lasts=0;lastt=0;cts=0;} {if (cts == 0) cts = $42; if (lasts > 0 && ($4 - lasts >= 0) && (($4 - lasts) /1000000.0/($1-lastt) < 3000)) print ($42 - cts) \" \" ($4-lasts)/1000000.0/($1-lastt); lastt=$1;lasts=$4;}' FIXME_REPORT_FILE" \
	using 1:2 t "Space Consumption Rate" w linesp ls 1 axes x1y1, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \" $5/$3*100;}' FIXME_REPORT_FILE" \
	using 1:2 t "Free Space Ratio" w linesp ls 2 axes x1y2
	
