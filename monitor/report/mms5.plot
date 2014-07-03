reset

set terminal png size 1600,900
set origin 0.0, 0.0
set size 1, 1
set output "FIXME_LOCATION/mms_misc.png"
set auto x
set xtics out
set auto y

set title "MM Request Response Misc On Day FIXME_REPORT_DAY"
set xlabel "TimeStamp"
set ylabel "Average Request Length (bytes)"
#set y2label "Average Request Length (bytes)"
set xdata time
set timefmt "%s"
set format x "%H:%S"
set ytics mirror
#set y2tics nomirror
set logscale y 10
set key out
set grid

NODES = "FIXME_NODE"
plot for [node in NODES] \
     node using ($2+8*3600):($8/$13) t "Average Write Length Y1 @ ".node with points pt 2 axes x1y1, \
     node using ($2+8*3600):($9/$11) t "Average Read  Length Y1 @ ".node with points pt 1 axes x1y1
