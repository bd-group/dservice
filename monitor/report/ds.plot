reset

set terminal png size 1600,900
set origin 0.0, 0.0
set size 1, 1
set output "FIXME_LOCATION/ds.png"
set auto x
set xtics out
set auto y

set title "DService Operations On Day FIXME_REPORT_DAY"
set xlabel "TimeStamp # (1 tic is about 65s)"
set ylabel "Operation Number (#)"
set xdata time
set timefmt "%s"
set format x "%H:%S"
set y2label "Operation Number (#)"
set ytics nomirror
set y2tics nomirror
set logscale y 10
set logscale y2 10
set key out
set grid

plot "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \" ($49);}' FIXME_REPORT_FILE" \
	using 1:2 t "Queued Rep (DS) Y1" w linesp ls 1 axes x1y1, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \" ($50);}' FIXME_REPORT_FILE" \
	using 1:2 t "Handle Rep (DS) Y1" w linesp ls 2 axes x1y1, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \" ($51);}' FIXME_REPORT_FILE" \
	using 1:2 t "Done   Rep (DS) Y1" w linesp ls 3 axes x1y1, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \" ($52);}' FIXME_REPORT_FILE" \
	using 1:2 t "Queued Del (DS) Y1" w linesp ls 4 axes x1y1, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \" ($53);}' FIXME_REPORT_FILE" \
	using 1:2 t "Handle Del (DS) Y1" w linesp ls 5 axes x1y1, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \" ($54);}' FIXME_REPORT_FILE" \
	using 1:2 t "Done   Del (DS) Y1" w linesp ls 6 axes x1y1
