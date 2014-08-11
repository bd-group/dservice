#!/bin/bash

if [ "x$1" == "x" ]; then
    echo Usage: doplot.sh target_location redisinfofile
    exit
fi

if [ "x$2" == "x" ]; then
    echo Usage: doplot.sh target_location redisinfofile
    exit
fi

if [ "x$3" == "x" ]; then
    MODE="ALL"
else
    MODE="SINGLE"
fi

TLOC=.
SYSINFOP=../$2
mkdir -p $1;
cd $1;

echo "Plot Redis Info ..."
if [ "x$MODE" == "xSINGLE" ]; then
    NODES="$3"
elif [ "x$MODE" == "xALL" ]; then
    NODES=`cat $SYSINFOP | awk -F, '{print $1}' | sort | uniq`
fi
NODES=`echo $NODES`
NODES_ACCU=""

for n in $NODES; do
	cat $SYSINFOP | awk -F, "{if (\$1 == \"$n\") print}" | sed -e 's/,/ /g' > $n
	cat $n | awk 'BEGIN{ts=0;cmd=0;}{if(ts==0)ts=$2;if(cmd==0)cmd=$4; if($2-ts>0) print $2" "$3" "($4-cmd)/($2-ts);ts=$2;cmd=$4}' > $n.accu
	NODES_ACCU+="$NODES_ACCU $n.accu"
done

if [ "x$MODE" == "xALL" ]; then
    sed -e "s|FIXME_LOCATION|$TLOC|g;s|FIXME_REPORT_DAY|$2|g;s|FIXME_REPORT_FILE|$2|g;s|FIXME_NODE|$NODES|g" ../report/redis1.plot > .tmp_plot_file2
    gnuplot .tmp_plot_file2
    sed -e "s|FIXME_LOCATION|$TLOC|g;s|FIXME_REPORT_DAY|$2|g;s|FIXME_REPORT_FILE|$2|g;s|FIXME_NODE|$NODES|g" ../report/redis2.plot > .tmp_plot_file2
    gnuplot .tmp_plot_file2
    sed -e "s|FIXME_LOCATION|$TLOC|g;s|FIXME_REPORT_DAY|$2|g;s|FIXME_REPORT_FILE|$2|g;s|FIXME_NODE|$NODES_ACCU|g" ../report/redis3.plot > .tmp_plot_file2
    gnuplot .tmp_plot_file2
    sed -e "s|FIXME_LOCATION|$TLOC|g;s|FIXME_REPORT_DAY|$2|g;s|FIXME_REPORT_FILE|$2|g;s|FIXME_NODE|$NODES|g" ../report/redis4.plot > .tmp_plot_file2
    gnuplot .tmp_plot_file2
    sed -e "s|FIXME_LOCATION|$TLOC|g;s|FIXME_REPORT_DAY|$2|g;s|FIXME_REPORT_FILE|$2|g;s|FIXME_NODE|$NODES|g" ../report/redis5.plot > .tmp_plot_file2
    gnuplot .tmp_plot_file2
    sed -e "s|FIXME_LOCATION|$TLOC|g;s|FIXME_REPORT_DAY|$2|g;s|FIXME_REPORT_FILE|$2|g;s|FIXME_NODE|$NODES|g" ../report/redis6.plot > .tmp_plot_file2
    gnuplot .tmp_plot_file2
    sed -e "s|FIXME_LOCATION|$TLOC|g;s|FIXME_REPORT_DAY|$2|g;s|FIXME_REPORT_FILE|$2|g;s|FIXME_NODE|$NODES|g" ../report/redis7.plot > .tmp_plot_file2
    gnuplot .tmp_plot_file2
    sed -e "s|FIXME_LOCATION|$TLOC|g;s|FIXME_REPORT_DAY|$2|g;s|FIXME_REPORT_FILE|$2|g;s|FIXME_NODE|$NODES|g" ../report/redis8.plot > .tmp_plot_file2
    gnuplot .tmp_plot_file2
fi
for n in $NODES; do
	rm -rf $n
	rm -rf $n.accu
done

rm -rf .tmp_plot_file2
