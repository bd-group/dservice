reset

set terminal png size 1600,900
set origin 0.0, 0.0
set size 1, 1
set output "FIXME_LOCATION/sfl.png"
set auto x
set xtics out
set auto y

set title "File Location Status On Day FIXME_REPORT_DAY"
set xlabel "TimeStamp # (1 tic is about 65s)"
set ylabel "SFL Operation Rate (#/min)"
set xdata time
set timefmt "%s"
set format x "%H:%S"
set y2label "SFL Operation Rate (#/min)"
set ytics nomirror
#set logscale y 10
#set logscale y2 10
set y2tics nomirror
set key out
set grid

plot "< awk -F, 'BEGIN{lasts=0;lastt=0;cts=0;} {if (cts == 0) {lastt = cts = $42}; if (lasts >= 0 && ($42 - lastt) > 0 && ($22 - lasts) >= 0) print ($42 - cts) \" \" ($22-lasts)/($42-lastt)*60; lastt=$42;lasts=$22;}' FIXME_REPORT_FILE" \
	using 1:2 t "SFL Create Rate Y1" w linesp ls 1 axes x1y1, \
     "< awk -F, 'BEGIN{lasts=0;lastt=0;cts=0;} {if (cts == 0) {lastt = cts = $42}; if (lasts >= 0 && ($42 - lastt) > 0 && ($26 - lasts) >= 0) print ($42 - cts) \" \" ($26-lasts)/($42-lastt)*60; lastt=$42;lasts=$26;}' FIXME_REPORT_FILE" \
	using 1:2 t "SFL Delete Rate Y1" w linesp ls 2 axes x1y1, \
     "< awk -F, 'BEGIN{lasts=0;lastt=0;cts=0;} {if (cts == 0) {lastt = cts = $42}; if (lasts >= 0 && ($42 - lastt) > 0 && ($23 - lasts) >= 0) print ($42 - cts) \" \" ($23-lasts)/($42-lastt)*60; lastt=$42;lasts=$23;}' FIXME_REPORT_FILE" \
	using 1:2 t "SFL Online Rate Y2" w linesp ls 3 axes x1y2, \
     "< awk -F, 'BEGIN{lasts=0;lastt=0;cts=0;} {if (cts == 0) {lastt = cts = $42}; if (lasts >= 0 && ($42 - lastt) > 0 && ($24 - lasts) >= 0) print ($42 - cts) \" \" ($24-lasts)/($42-lastt)*60; lastt=$42;lasts=$24;}' FIXME_REPORT_FILE" \
	using 1:2 t "SFL Offline Rate Y2" w linesp ls 4 axes x1y2, \
     "< awk -F, 'BEGIN{lasts=0;lastt=0;cts=0;} {if (cts == 0) {lastt = cts = $42}; if (lasts >= 0 && ($42 - lastt) > 0 && ($25 - lasts) >= 0) print ($42 - cts) \" \" ($25-lasts)/($42-lastt)*60; lastt=$42;lasts=$25;}' FIXME_REPORT_FILE" \
	using 1:2 t "SFL Suspect Rate Y2" w linesp ls 5 axes x1y2
