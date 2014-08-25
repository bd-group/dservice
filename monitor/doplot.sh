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
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/report.plot > $2/.tmp_plot_file1
gnuplot $2/.tmp_plot_file1 &
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/space.plot > $2/.tmp_plot_file2
gnuplot $2/.tmp_plot_file2 &
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/frps.plot > $2/.tmp_plot_file3
gnuplot $2/.tmp_plot_file3 &
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/query.plot > $2/.tmp_plot_file4
gnuplot $2/.tmp_plot_file4 &
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/Queue.plot > $2/.tmp_plot_file5
gnuplot $2/.tmp_plot_file5 &
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/job.plot > $2/.tmp_plot_file6
gnuplot $2/.tmp_plot_file6 &
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/sfl.plot > $2/.tmp_plot_file7
gnuplot $2/.tmp_plot_file7 &
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/node.plot > $2/.tmp_plot_file8
gnuplot $2/.tmp_plot_file8 &
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/fops.plot > $2/.tmp_plot_file9
gnuplot $2/.tmp_plot_file9 &
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/fail.plot > $2/.tmp_plot_file10
gnuplot $2/.tmp_plot_file10 &
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/diskfrees.plot > $2/.tmp_plot_file11
gnuplot $2/.tmp_plot_file11 &
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/ds.plot > $2/.tmp_plot_file12
gnuplot $2/.tmp_plot_file12 &
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/loads.plot > $2/.tmp_plot_file13
gnuplot $2/.tmp_plot_file13 &
sed -e "s|FIXME_LOCATION|$2|g;s|FIXME_REPORT_DAY|$1|g;s|FIXME_REPORT_FILE|$1|g" report/free.plot > $2/.tmp_plot_file14
gnuplot $2/.tmp_plot_file14 &

wait

rm -rf $2/.tmp_plot_file1
rm -rf $2/.tmp_plot_file2
rm -rf $2/.tmp_plot_file3
rm -rf $2/.tmp_plot_file4
rm -rf $2/.tmp_plot_file5
rm -rf $2/.tmp_plot_file6
rm -rf $2/.tmp_plot_file7
rm -rf $2/.tmp_plot_file8
rm -rf $2/.tmp_plot_file9
rm -rf $2/.tmp_plot_file10
rm -rf $2/.tmp_plot_file11
rm -rf $2/.tmp_plot_file12
rm -rf $2/.tmp_plot_file13
rm -rf $2/.tmp_plot_file14
