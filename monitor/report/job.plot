reset

set terminal png size 1600,900
set origin 0.0, 0.0
set size 1, 1
set output "FIXME_LOCATION/job.png"
set auto x
set xtics out
set auto y

set title "Job Status On Day FIXME_REPORT_DAY"
set xlabel "TimeStamp # (1 tic is about 65s)"
set ylabel "Number (#)"
set xdata time
set timefmt "%s"
set format x "%H:%S"
set y2label "Number (#)"
set ytics nomirror
set y2tics nomirror
#set logscale y2 10
set key out
set grid

plot "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \" $37;}' FIXME_REPORT_FILE" \
	using 1:2 t "Total File Rep  Y1" w linesp ls 1 axes x1y1, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \" $38;}' FIXME_REPORT_FILE" \
	using 1:2 t "Total File Del  Y1" w linesp ls 2 axes x1y1, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \" $39;}' FIXME_REPORT_FILE" \
	using 1:2 t "Queued File Rep Y2" w linesp ls 3 axes x1y2, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \" $40;}' FIXME_REPORT_FILE" \
	using 1:2 t "Queued File Del Y2" w linesp ls 4 axes x1y2

