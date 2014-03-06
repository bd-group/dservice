reset

set terminal png size 1600,900
set origin 0.0, 0.0
set size 1, 1
set output "FIXME_LOCATION/fops.png"
set auto x
set xtics out
set auto y

set title "File Operations and Per-Disk Statis On Day FIXME_REPORT_DAY"
set xlabel "TimeStamp # (1 tic is about 65s)"
set ylabel "File Operation Number (#)"
set xdata time
set timefmt "%s"
set format x "%H:%S"
set y2label "Per-disk Ratio (%)"
set ytics nomirror
set y2tics nomirror
set key out
set grid

plot "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \" ($11+$13+$14-$16);}' FIXME_REPORT_FILE" \
	using 1:2 t "Openned Files Y1" w linesp ls 1 axes x1y1, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \" ($46/$3*$9*100);}' FIXME_REPORT_FILE" \
	using 1:2 t "Free Stdev %  Y2" w linesp ls 2 axes x1y2