reset

set terminal png size 1600,900
set origin 0.0, 0.0
set size 1, 1
set output "FIXME_LOCATION/redis_link.png"
set auto x
set xtics out
set auto y

set title "Redis Master Link Status On Day FIXME_REPORT_DAY"
set xlabel "TimeStamp"
set xdata time
set ylabel "Link Status (1: up, 0: down, -1: none)"
set y2label "Link Status (1: up, 0: down, -1: none)"
set timefmt "%s"
set format x "%H:%S"
set ytics nomirror
set y2tics nomirror
#set logscale y2 10
set key out
set grid
set yrange [-2:2]

NODES = "FIXME_NODE"
plot for [node in NODES] \
        node using ($2+8*3600):($18) t "Link Status @ ".node with lines axes x1y1
