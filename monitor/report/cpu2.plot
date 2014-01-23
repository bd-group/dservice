reset

set terminal png size 1600,900
set origin 0.0, 0.0
set size 1, 1
set output "FIXME_LOCATION/cpu_sys.png"
set auto x
set xtics out
set auto y

set title "SYS CPU Statistics On Day FIXME_REPORT_DAY"
set xlabel "TimeStamp"
set ylabel "CPU Ratio (%)"
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
	using ($2+8*3600):($7/$4/$10) t "SYS  Y1 @ ".node with lines axes x1y1
