reset

set terminal png size 1600,900
set origin 0.0, 0.0
set size 1, 1
set output "FIXME_LOCATION/mms_bw.png"
set auto x
set xtics out
set auto y

set title "MM Request Bandwidth On Day FIXME_REPORT_DAY"
set xlabel "TimeStamp"
set ylabel "Request Bandwidth (MB/s)"
set y2label "Request Bandwidth (MB/s)"
set xdata time
set timefmt "%s"
set format x "%H:%S"
set ytics nomirror
set y2tics nomirror
#set logscale y 2
set key out
set grid

NODES = "FIXME_NODE"
plot for [node in NODES] \
     node using ($2+8*3600):($6/1024) t "Read  Bandwidth Y1 @ ".node with lines axes x1y1, \
     node using ($2+8*3600):($5/1024) t "Write Bandwidth Y2 @ ".node with lines axes x1y2