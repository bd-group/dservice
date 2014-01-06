reset

set terminal png size 1600,900
set origin 0.0, 0.0
set size 1, 1
set output "FIXME_LOCATION/disk1_ALL.png"
set auto x
set xtics out
set auto y

set title "I/O Statistics On Day FIXME_REPORT_DAY @ FIXME_NODE"
set xlabel "TimeStamp # (1 tic is about 65s)"
set ylabel "Number (#/s)"
set y2label "Latency (ms)"
set xdata time
set timefmt "%s"
set format x "%H:%S"
set ytics nomirror
set y2tics nomirror
set logscale y2 10
set key out
set grid

plot "< awk 'BEGIN{cts=0;} {if ($1 == \"RPT_NODE\") {print $3}}' FIXME_REPORT_FILE | sed 's/,/ /g'" \
	using ($2+8*3600):($5/$4) t "Read       # Y1" w linesp ls 1 axes x1y1, \
     "< awk 'BEGIN{cts=0;} {if ($1 == \"RPT_NODE\") {print $3}}' FIXME_REPORT_FILE | sed 's/,/ /g'" \
	using ($2+8*3600):($6/$4) t "Read Merge # Y1" axes x1y1, \
     "< awk 'BEGIN{cts=0;} {if ($1 == \"RPT_NODE\") {print $3}}' FIXME_REPORT_FILE | sed 's/,/ /g'" \
	using ($2+8*3600):($8/($5 == 0 ? 1 : $5)) t "Read Lat   # Y2" axes x1y2, \
     "< awk 'BEGIN{cts=0;} {if ($1 == \"RPT_NODE\") {print $3}}' FIXME_REPORT_FILE | sed 's/,/ /g'" \
	using ($2+8*3600):($9/$4) t "Write      # Y1" w linesp ls 4 axes x1y1, \
     "< awk 'BEGIN{cts=0;} {if ($1 == \"RPT_NODE\") {print $3}}' FIXME_REPORT_FILE | sed 's/,/ /g'" \
	using ($2+8*3600):($10/$4) t "Write Merge # Y1" axes x1y1, \
     "< awk 'BEGIN{cts=0;} {if ($1 == \"RPT_NODE\") {print $3}}' FIXME_REPORT_FILE | sed 's/,/ /g'" \
	using ($2+8*3600):($12/($9 == 0 ? 1 : $9)) t "Write Lat  # Y2" axes x1y2, \
     "< awk 'BEGIN{cts=0;} {if ($1 == \"RPT_NODE\") {print $3}}' FIXME_REPORT_FILE | sed 's/,/ /g'" \
	using ($2+8*3600):($13) t "Pending IO     # Y1" w linesp ls 7 axes x1y1, \
     "< awk 'BEGIN{cts=0;} {if ($1 == \"RPT_NODE\") {print $3}}' FIXME_REPORT_FILE | sed 's/,/ /g'" \
	using ($2+8*3600):($14/($13 == 0 ? 1 : $13)) t "A LAT    # Y2" axes x1y2, \
     "< awk 'BEGIN{cts=0;} {if ($1 == \"RPT_NODE\") {print $3}}' FIXME_REPORT_FILE | sed 's/,/ /g'" \
	using ($2+8*3600):($15/($13 == 0 ? 1 : $13)) t "WT LAT     # Y2" axes x1y2
