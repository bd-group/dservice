reset

set terminal png size 1600,900
set origin 0.0, 0.0
set size 1, 1
set output "FIXME_LOCATION/node.png"
set auto x
set xtics out
set auto y

set title "Node Statis and Per-Disk Space Statis On Day FIXME_REPORT_DAY"
set xlabel "TimeStamp # (1 tic is about 65s)"
set ylabel "Node Number (#)"
set xdata time
set timefmt "%s"
set format x "%H:%S"
set y2label "Disk Space (GiB)"
set ytics nomirror
set y2tics nomirror
set key out
set grid

plot "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \" $6;}' FIXME_REPORT_FILE" \
	using 1:2 t "Total  Node  Y1" w linesp ls 1 axes x1y1, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \" $7;}' FIXME_REPORT_FILE" \
	using 1:2 t "Active Node  Y1" w linesp ls 2 axes x1y1, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \" $46/1024/1024/1024;}' FIXME_REPORT_FILE" \
        using 1:2 t "Free Spc Stdev Y2" w linesp ls 3 axes x1y2
#     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \" $46/$3*$9*100 \" \" ($47-$46)/$3*$9*100 \" \" ($47+$46)/%3*$9*100;}' FIXME_REPORT_FILE" \
#        using 1:2:3:4 t "Free Stdev % Y2" w errorbars ls 3 axes x1y2
