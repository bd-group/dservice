#!/bin/bash

if [ "x$1" == "x" ]; then
    echo Usage: doplot.sh datafile target_location sysinfofile
	exit
fi

if [ "x$2" == "x" ]; then
    echo Usage: doplot.sh datafile target_location sysinfofile
    exit
fi

if [ "x$3" == "x" ]; then
    echo Usage: doplot.sh datafile target_location sysinfofile
    exit
fi

if [ "x$4" == "x" ]; then
    MODE="SINGLE"
else
    MODE="ALL"
fi

mkdir -p $2;
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/report.plot > .tmp_plot_file2
gnuplot .tmp_plot_file2
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/space.plot > .tmp_plot_file2
gnuplot .tmp_plot_file2
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/frps.plot > .tmp_plot_file2
gnuplot .tmp_plot_file2
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/query.plot > .tmp_plot_file2
gnuplot .tmp_plot_file2
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/Queue.plot > .tmp_plot_file2
gnuplot .tmp_plot_file2
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/job.plot > .tmp_plot_file2
gnuplot .tmp_plot_file2
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/sfl.plot > .tmp_plot_file2
gnuplot .tmp_plot_file2
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/node.plot > .tmp_plot_file2
gnuplot .tmp_plot_file2

echo "Plot DISK Info ..."
if [ "x$MODE" == "xSINGLE" ]; then
    NODES="ALL_DEVS"
elif [ "x$MODE" == "xALL" ]; then
    NODES=`cat $3 | awk '{if ($1 == "RPT_NODE") print $3}' | awk -F, '{print $1}' | sort | uniq`
fi
NODES=`echo $NODES`
for n in $NODES; do
	cat $3 | awk '{if ($1 == "RPT_NODE") print $3}' | awk -F, "{if (\$1 == \"$n\") print}" | sed -e 's/,/ /g' > $n
done
if [ "xMODE" == "xALL" ]; then
    sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$3|g;s|FIXME_REPORT_FILE|$3|g;s|FIXME_NODE|$NODES|g" report/disk1.plot > .tmp_plot_file2
    gnuplot .tmp_plot_file2
fi
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$3|g;s|FIXME_REPORT_FILE|$3|g;s|FIXME_NODE|$NODES|g" report/disk2.plot > .tmp_plot_file2
gnuplot .tmp_plot_file2
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$3|g;s|FIXME_REPORT_FILE|$3|g;s|FIXME_NODE|$NODES|g" report/disk3.plot > .tmp_plot_file2
gnuplot .tmp_plot_file2
for n in $NODES; do
	rm -rf $n
done

echo "Plot CPU Info ..."
if [ "x$MODE" == "xSINGLE" ]; then
    NODES="ALL_CPUS"
elif [ "x$MODE" == "xALL" ]; then
    NODES=`cat $3 | awk '{if ($1 == "RPT_CPU") print $3}' | awk -F, '{print $1}' | sort | uniq`
fi
NODES=`echo $NODES`
for n in $NODES; do
        cat $3 | awk '{if ($1 == "RPT_CPU") print $3}' | awk -F, "{if (\$1 == \"$n\") print}" | sed -e 's/,/ /g' > $n
done
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$3|g;s|FIXME_REPORT_FILE|$3|g;s|FIXME_NODE|$NODES|g" report/cpu1.plot > .tmp_plot_file2
gnuplot .tmp_plot_file2
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$3|g;s|FIXME_REPORT_FILE|$3|g;s|FIXME_NODE|$NODES|g" report/cpu2.plot > .tmp_plot_file2
gnuplot .tmp_plot_file2
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$3|g;s|FIXME_REPORT_FILE|$3|g;s|FIXME_NODE|$NODES|g" report/cpu3.plot > .tmp_plot_file2
gnuplot .tmp_plot_file2
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$3|g;s|FIXME_REPORT_FILE|$3|g;s|FIXME_NODE|$NODES|g" report/cpu4.plot > .tmp_plot_file2
gnuplot .tmp_plot_file2
for n in $NODES; do
        rm -rf $n
done

echo "Plot NET Info ..."
if [ "x$MODE" == "xSINGLE" ]; then
    NODES="ALL_NETS"
elif [ "x$MODE" == "xALL" ]; then
    NODES=`cat $3 | awk '{if ($1 == "RPT_NET") print $3}' | awk -F, '{print $1}' | sort | uniq`
fi
NODES=`echo $NODES`
for n in $NODES; do
        cat $3 | awk '{if ($1 == "RPT_NET") print $3}' | awk -F, "{if (\$1 == \"$n\") print}" | sed -e 's/,/ /g' > $n
done
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$3|g;s|FIXME_REPORT_FILE|$3|g;s|FIXME_NODE|$NODES|g" report/net1.plot > .tmp_plot_file2
gnuplot .tmp_plot_file2
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$3|g;s|FIXME_REPORT_FILE|$3|g;s|FIXME_NODE|$NODES|g" report/net2.plot > .tmp_plot_file2
gnuplot .tmp_plot_file2
for n in $NODES; do
        rm -rf $n
done

rm -rf .tmp_plot_file2