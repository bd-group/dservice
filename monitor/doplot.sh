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
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/report.plot > $2/.tmp_plot_file
gnuplot $2/.tmp_plot_file
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/space.plot > $2/.tmp_plot_file
gnuplot $2/.tmp_plot_file
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/frps.plot > $2/.tmp_plot_file
gnuplot $2/.tmp_plot_file
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/query.plot > $2/.tmp_plot_file
gnuplot $2/.tmp_plot_file
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/Queue.plot > $2/.tmp_plot_file
gnuplot $2/.tmp_plot_file
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/job.plot > $2/.tmp_plot_file
gnuplot $2/.tmp_plot_file
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/sfl.plot > $2/.tmp_plot_file
gnuplot $2/.tmp_plot_file
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/node.plot > $2/.tmp_plot_file
gnuplot $2/.tmp_plot_file
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/fops.plot > $2/.tmp_plot_file
gnuplot $2/.tmp_plot_file
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/fail.plot > $2/.tmp_plot_file
gnuplot $2/.tmp_plot_file
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/diskfrees.plot > $2/.tmp_plot_file
gnuplot $2/.tmp_plot_file
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/ds.plot > $2/.tmp_plot_file
gnuplot $2/.tmp_plot_file
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/loads.plot > $2/.tmp_plot_file
gnuplot $2/.tmp_plot_file
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/free.plot > $2/.tmp_plot_file
gnuplot $2/.tmp_plot_file

rm -rf $2/.tmp_plot_file
