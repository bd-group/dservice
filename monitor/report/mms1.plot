reset

set terminal png size 1600,900
set origin 0.0, 0.0
set size 1, 1
set output "FIXME_LOCATION/mms_rate.png"
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
#set logscale y 2
set key out
set grid

NODES = "FIXME_NODE"
plot for [node in NODES] node \
        using ($2+8*3600):($11/$4) t "Read  Rate Y1 @ ".node with lines axes x1y1, \
        node using ($2+8*3600):($13/$4) t "Write Rate Y2 @ ".node with lines axes x1y2