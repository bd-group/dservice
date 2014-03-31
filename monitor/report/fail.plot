reset

set terminal png size 1600,900
set origin 0.0, 0.0
set size 1, 1
set output "FIXME_LOCATION/fail.png"
set auto x
set xtics out
set auto y

set title "DService Fail/Verify Operations On Day FIXME_REPORT_DAY"
set xlabel "TimeStamp # (1 tic is about 65s)"
set ylabel "Operation per Second (#/s)"
set xdata time
set timefmt "%s"
set format x "%H:%S"
set y2label "Operation per Second (#/s)"
set ytics nomirror
set y2tics nomirror
set logscale y 10
set key out
set grid

plot "< awk -F, 'BEGIN{cts=0;xts=0;lts=0;} {if (cts == 0) cts = $42; if (lts == 0) lts=$43; if ($43-lts >= 0) print ($42 - cts) \" \" ($43-lts)/($42-xts);lts=$43;xts=$42;}' FIXME_REPORT_FILE" \
	using 1:2 t "Total VER(MS) Y1" w linesp ls 1 axes x1y1, \
     "< awk -F, 'BEGIN{cts=0;xts=0;lts=0;} {if (cts == 0) cts = $42; if (lts == 0) lts=$55; if ($55-lts >= 0) print ($42 - cts) \" \" ($55-lts)/($42-xts);lts=$55;xts=$42;}' FIXME_REPORT_FILE" \
	using 1:2 t "Total VER(DS) Y1" w linesp ls 2 axes x1y1, \
     "< awk -F, 'BEGIN{cts=0;xts=0;lts=0;} {if (cts == 0) cts = $42; if (lts == 0) lts=$56; if ($56-lts >= 0) print ($42 - cts) \" \" ($56-lts)/($42-xts);lts=$56;xts=$42;}' FIXME_REPORT_FILE" \
	using 1:2 t "Total VYR(DS) Y2" w linesp ls 3 axes x1y2, \
     "< awk -F, 'BEGIN{cts=0;xts=0;lts=0;} {if (cts == 0) cts = $42; if (lts == 0) lts=$44; if ($44-lts >= 0) print ($42 - cts) \" \" ($44-lts)/($42-xts);lts=$44;xts=$42;}' FIXME_REPORT_FILE" \
        using 1:2 t "Failed Rep    Y2" w linesp ls 4 axes x1y2, \
     "< awk -F, 'BEGIN{cts=0;xts=0;lts=0;} {if (cts == 0) cts = $42; if (lts == 0) lts=$45; if ($45-lts >= 0) print ($42 - cts) \" \" ($45-lts)/($42-xts);lts=$45;xts=$42;}' FIXME_REPORT_FILE" \
        using 1:2 t "Failed Del    Y2" w linesp ls 5 axes x1y2

