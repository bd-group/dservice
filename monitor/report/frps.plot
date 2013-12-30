reset

set terminal png size 1600,900
set origin 0.0, 0.0
set size 1, 1
set output "FIXME_LOCATION/frps.png"
set auto x
set xtics out
set auto y

set title "File Operation Rate On Day FIXME_REPORT_DAY"
set xlabel "TimeStamp # (1 tic is about 65s)"
set ylabel "File Operation Rate (#/min)"
set xdata time
set timefmt "%s"
set format x "%H:%S"
set y2label "File Operation Rate (#/min)"
set ytics nomirror
set y2tics nomirror
set key out
set grid

plot "< awk -F, 'BEGIN{lasts=0;lastt=0;cts=0;} {if (cts == 0) cts = $42; if (lasts >= 0 && ($11 - lasts) >= 0) print ($42 - cts) \" \" ($11-lasts)/($1-lastt)*60; lastt=$1;lasts=$11;}' FIXME_REPORT_FILE" \
	using 1:2 t "Create1 Rate Y1" w linesp ls 1 axes x1y1, \
     "< awk -F, 'BEGIN{lasts=0;lastt=0;cts=0;} {if (cts == 0) cts = $42; if (lasts >= 0 && ($13 - lasts) >= 0) print ($42 - cts) \" \" ($13-lasts)/($1-lastt)*60; lastt=$1;lasts=$13;}' FIXME_REPORT_FILE" \
	using 1:2 t "Create2 Rate Y1" w linesp ls 2 axes x1y1, \
     "< awk -F, 'BEGIN{lasts=0;lastt=0;cts=0;} {if (cts == 0) cts = $42; if (lasts >= 0 && ($14 - lasts) >= 0) print ($42 - cts) \" \" ($14-lasts)/($1-lastt)*60; lastt=$1;lasts=$14;}' FIXME_REPORT_FILE" \
	using 1:2 t "Reopen  Rate Y2" w linesp ls 3 axes x1y2
