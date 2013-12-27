reset

set terminal png size 1600,900
set origin 0.0, 0.0
set size 1, 1
set output "FIXME_LOCATION/Queue.png"
set auto x
set xtics out
set auto y

set title "Queue Stat On Day FIXME_REPORT_DAY"
set xlabel "TimeStamp # (1 tic is about 65s)"
set ylabel "Queue Depth (#)"
set xdata time
set timefmt "%s"
set format x "%H:%S"
set y2label "Latency (s)"
set ytics nomirror
set y2tics nomirror
set key out
set grid

plot "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \" $31;}' FIXME_REPORT_FILE" \
	using 1:2 t "closeRepLimit Y1" w linesp ls 1 axes x1y1, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \" $32 ;}' FIXME_REPORT_FILE" \
	using 1:2 t "fixRepLimit   Y1" w linesp ls 2 axes x1y1, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \" $41 ;}' FIXME_REPORT_FILE" \
	using 1:2 t "avgRptLat     Y2" w linesp ls 3 axes x1y2
