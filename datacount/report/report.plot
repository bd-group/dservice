reset

set terminal png size 3200,1800
set origin 0.0, 0.0
set size 1, 1
set output "FIXME_LOCATION/report.png"
set auto x
set xtics out
set auto y

set title "MetaStore Report on Day FIXME_REPORT_DAY"
set xlabel "TimeStamp # (1 tic is about 65s)"
set xdata time
set timefmt "%s"
set format x "%H:%S"
set ylabel "#"
set ytics nomirror
set y2label "#"
set y2tics
set y2tics nomirror
set logscale y2 10
set key out
set grid

plot "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \" $1;}' FIXME_REPORT_FILE" \
	using 1:2 t "TimeStamp        Y2" w linesp ls 1 axes x1y2, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \" $3/1000000000000.0;}' FIXME_REPORT_FILE" \
	using 1:2 t "Total Space (TB) Y1" w linesp ls 2 axes x1y1, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \" $4/1000000000000.0;}' FIXME_REPORT_FILE" \
	using 1:2 t "Used  Space (TB) Y1" w linesp ls 3 axes x1y1, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \" $5/1000000000000.0;}' FIXME_REPORT_FILE" \
	using 1:2 t "Free  Space (TB) Y1" w linesp ls 4 axes x1y1, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \" $6;}' FIXME_REPORT_FILE" \
	using 1:2 t "Total  Node      Y1" w linesp ls 5 axes x1y1, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \" $7;}' FIXME_REPORT_FILE" \
	using 1:2 t "Active Node      Y1" w linesp ls 6 axes x1y1, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \" $8;}' FIXME_REPORT_FILE" \
	using 1:2 t "Total Device     Y1" w linesp ls 7 axes x1y1, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \" $9;}' FIXME_REPORT_FILE" \
	using 1:2 t "Active Device    Y1" w linesp ls 8 axes x1y1, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \"$10;}' FIXME_REPORT_FILE" \
	using 1:2 t "Create Issued    Y1" w linesp ls 9 axes x1y1, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \"$11;}' FIXME_REPORT_FILE" \
	using 1:2 t "Create Succeed   Y1" w linesp ls 10 axes x1y1, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \"$12;}' FIXME_REPORT_FILE" \
	using 1:2 t "Create2 Issued   Y1" w linesp ls 11 axes x1y1, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \"$13;}' FIXME_REPORT_FILE" \
	using 1:2 t "Create2 Succeed  Y1" w linesp ls 12 axes x1y1, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \"$14;}' FIXME_REPORT_FILE" \
	using 1:2 t "Reopen Issued    Y1" w linesp ls 13 axes x1y1, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \"$15;}' FIXME_REPORT_FILE" \
	using 1:2 t "GetFile Issued   Y2" w linesp ls 14 axes x1y2, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \"$16;}' FIXME_REPORT_FILE" \
	using 1:2 t "Close Issued     Y1" w linesp ls 15 axes x1y1, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \"$17;}' FIXME_REPORT_FILE" \
	using 1:2 t "Replicated       Y1" w linesp ls 16 axes x1y1, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \"$18;}' FIXME_REPORT_FILE" \
	using 1:2 t "RM Logical       Y1" w linesp ls 17 axes x1y1, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \"$19;}' FIXME_REPORT_FILE" \
	using 1:2 t "RM Physical      Y1" w linesp ls 18 axes x1y1, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \"$20};' FIXME_REPORT_FILE" \
	using 1:2 t "Restore File     Y1" w linesp ls 19 axes x1y1, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \"$21;}' FIXME_REPORT_FILE" \
	using 1:2 t "Delete File      Y1" w linesp ls 20 axes x1y1, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \"$22;}' FIXME_REPORT_FILE" \
	using 1:2 t "SFL Create       Y1" w linesp ls 21 axes x1y1, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \"$23;}' FIXME_REPORT_FILE" \
	using 1:2 t "SFL Online       Y1" w linesp ls 22 axes x1y1, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \"$24;}' FIXME_REPORT_FILE" \
	using 1:2 t "SFL Offline      Y1" w linesp ls 23 axes x1y1, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \"$25;}' FIXME_REPORT_FILE" \
	using 1:2 t "SFL Suspect      Y1" w linesp ls 24 axes x1y1, \
     "< awk -F, 'BEGIN{cts=0;} {if (cts == 0) cts = $42; print ($42 - cts) \" \"$26;}' FIXME_REPORT_FILE" \
	using 1:2 t "SFL DEL          Y1" w linesp ls 25 axes x1y1

