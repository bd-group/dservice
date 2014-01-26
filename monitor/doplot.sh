#!/bin/bash

if [ "x$1" == "x" ]; then
    echo Usage: doplot.sh datafile target_location
	exit
fi

if [ "x$2" == "x" ]; then
    echo Usage: doplot.sh datafile target_location
    exit
fi

mkdir -p $2;
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/report.plot > .tmp_plot_file
gnuplot .tmp_plot_file
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/space.plot > .tmp_plot_file
gnuplot .tmp_plot_file
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/frps.plot > .tmp_plot_file
gnuplot .tmp_plot_file
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/query.plot > .tmp_plot_file
gnuplot .tmp_plot_file
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/Queue.plot > .tmp_plot_file
gnuplot .tmp_plot_file
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/job.plot > .tmp_plot_file
gnuplot .tmp_plot_file
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/sfl.plot > .tmp_plot_file
gnuplot .tmp_plot_file
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/node.plot > .tmp_plot_file
gnuplot .tmp_plot_file

rm -rf .tmp_plot_file
