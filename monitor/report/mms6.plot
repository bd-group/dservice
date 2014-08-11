reset

set terminal png size 1600,900
set origin 0.0, 0.0
set size 1, 1
set output "FIXME_LOCATION/mms_XXX.png"
set auto x
set xtics out
set auto y

set title "Active MMServers On Day FIXME_REPORT_DAY"
set xlabel "TimeStamp"
set ylabel "Active MMServer (#)"
set y2label "Active MMServer (#)"
set xdata time
set timefmt "%s"
set format x "%H:%S"
set ytics mirror
#set y2tics nomirror
#set logscale y 10
set yrange [0:7]
set key out
set grid

plot "mm_sites" using ($1+8*3600):2 t "Active MMS" w linesp ls 1 axes x1y1
