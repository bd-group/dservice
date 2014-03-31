reset

set terminal png size 1600,900
set origin 0.0, 0.0
set size 1, 1
set output "FIXME_LOCATION/mm1.png"
set auto x
set xtics out
set auto y

set title "MM Request Response Rate On Day FIXME_REPORT_DAY"
set xlabel "TimeStamp"
set ylabel "Request Response Rate (#/s)"
set y2label "Request Response Rate (#/s)"
set xdata time
set timefmt "%s"
set format x "%H:%S"
set ytics nomirror
set y2tics nomirror
set key out
set grid

plot "< awk -F, 'BEGIN{lasts=0;lastt=0;cts=0;} {if (cts == 0) lasts = cts = $1; if (($1 - lasts) > 0 && ($8 - lasts) >= 0) print ($1 - cts) \" \" ($8-lasts)/($1-lastt); lastt=$1;lasts=$8;}' FIXME_REPORT_FILE" \
	using 1:2 t "Read  Rate Y1" w linesp ls 1 axes x1y1, \
     "< awk -F, 'BEGIN{lasts=0;lastt=0;cts=0;} {if (cts == 0) lasts = cts = $1; if ((41 - lasts) > 0 && ($10 - lasts) >= 0) print ($1 - cts) \" \" ($10-lasts)/($1-lastt); lastt=$1;lasts=$10;}' FIXME_REPORT_FILE" \
	using 1:2 t "Write Rate Y2" w linesp ls 2 axes x1y2
	
