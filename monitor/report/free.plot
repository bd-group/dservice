reset

set terminal png size 1600,900
set origin 0.0, 0.0
set size 1, 1
set output "FIXME_LOCATION/free.png"
set auto x
set xtics out
set auto y

set title "True Disk Free Statis On Day FIXME_REPORT_DAY"
set xlabel "TimeStamp # (1 tic is about 65s)"
set y2label "Free Space Ratio (%)"
set ylabel "Space (GB)"
set xdata time
set timefmt "%s"
set format x "%H:%S"
set ytics nomirror
set y2tics nomirror
set key out
set grid

plot "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \" $5/$3*100;}' FIXME_REPORT_FILE" \
	using 1:2 t "Total Free Space Ratio Y2" w linesp ls 2 axes x1y2, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; if ($59 > 0) print ($42 - cts) \" \" $60/$59*100;}' FIXME_REPORT_FILE" \
	using 1:2 t "True  Free Space Ratio Y2" w linesp ls 1 axes x1y2, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; if ($61 >= 0) print ($42 - cts) \" \" $61/1000000000;}' FIXME_REPORT_FILE" \
	using 1:2 t "Offline Device Space   Y1" w linesp ls 3 axes x1y1

	
