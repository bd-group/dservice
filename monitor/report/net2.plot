reset

set terminal png size 1600,900
set origin 0.0, 0.0
set size 1, 1
set output "FIXME_LOCATION/net_tx.png"
set auto x
set xtics out
set auto y

set title "Network I/O Statistics On Day FIXME_REPORT_DAY"
set xlabel "TimeStamp"
set ylabel "I/O Bandwidth (MB/s)"
#set y2label "Number (#) / Latency (ms)"
set xdata time
set timefmt "%s"
set format x "%H:%S"
#set ytics nomirror
#set y2tics nomirror
#set logscale y2 10
set key out
set grid

NODES = "FIXME_NODE"
plot for [node in NODES] node \
        using ($2+8*3600):($13/1024/1024/$4) t "TX(out) BW Y1 @ ".node with lines axes x1y1
