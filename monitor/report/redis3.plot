reset

set terminal png size 1600,900
set origin 0.0, 0.0
set size 1, 1
set output "FIXME_LOCATION/redis_cmd.png"
set auto x
set xtics out
set auto y

set title "Redis Accumulated Request Response Rate On Day FIXME_REPORT_DAY"
set xlabel "TimeStamp"
set ylabel "Request Response Rate (#/s)"
set y2label "Request Response Rate (#/s)"
set xdata time
set timefmt "%s"
set format x "%H:%S"
set ytics nomirror
set y2tics nomirror
#set logscale y2 10
set key out
set grid

NODES = "FIXME_NODE"
plot for [node in NODES] \
         node using ($1+8*3600):($2*$3) t "Accumulated RPS @ ".node with lines axes x1y1