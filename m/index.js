$(display_all);

function display_all() {
    display_two('#container1', "BW", "Read/Write Bandwidth", "Bandwidth (KB/s)", 
                ["Write", "Read"], ['WBW', 'RBW']);
    //display_one('#container2', "WBW", "Write Bandwidth", "Write Bandwidth (KB/s)");
    //display_one('#container3', "RBW", "Read Bandwidth", "Read Bandwidth (KB/s)");
    //display_one('#container4', "RLT", "Read Latency", "Read Latency (ms)");
}

function display_one(ctr, jname, tname, yname) {
    var datestr = document.getElementById("dateid").value;
    if (datestr == null) {
        datestr = new Date().format("yyyy-MM-dd");
    }
    $.getJSON('http://127.0.1.1:30303/m/d/' + jname + '.json.' + datestr + '?date=' + datestr + '&callback=?', function (data) {

        $(ctr).highcharts({
            chart: {
                zoomType: 'x'
            },
            title: {
                text: 'MMS ' + tname + ' Over Time'
            },
            subtitle: {
                text: document.ontouchstart === undefined ?
                        'Click and drag in the plot area to zoom in' : 'Pinch the chart to zoom in'
            },
            xAxis: {
                type: 'datetime'
            },
            yAxis: {
                floor: 0,
                title: {
                    text: yname
                }
            },
            legend: {
                enabled: false
            },
            plotOptions: {
                area: {
                    fillColor: {
                        linearGradient: {
                            x1: 0,
                            y1: 0,
                            x2: 0,
                            y2: 1
                        },
                        stops: [
                            [0, Highcharts.getOptions().colors[0]],
                            [1, Highcharts.Color(Highcharts.getOptions().colors[0]).setOpacity(0).get('rgba')]
                        ]
                    },
                    marker: {
                        radius: 2
                    },
                    lineWidth: 1,
                    states: {
                        hover: {
                            lineWidth: 1
                        }
                    },
                    threshold: null
                }
            },

            series: [{
                type: 'area',
                name: tname,
                data: data
            }]
        });
    });
}

function display_two(ctr, jname, tname, yname, snames, names) {
    var datestr = document.getElementById("dateid").value;
    if (datestr == null) {
        datestr = new Date().format("yyyy-MM-dd");
    }
    var seriesOptions = [],
    seriesCounter = 0,
    createChart = function () {
        $(ctr).highcharts({
            chart: {
                type: 'areaspline',
                zoomType: 'x'
            },
            title: {
                text: 'MMS ' + tname + ' Over Time'
            },
            subtitle: {
                text: document.ontouchstart === undefined ?
                        'Click and drag in the plot area to zoom in' : 'Pinch the chart to zoom in'
            },
            xAxis: {
                type: 'datetime',
            },
            yAxis: {
                floor: 0,
                title: {
                    text: yname
                }
            },
            legend: {
                enabled: true
            },
            plotOptions: {
                spline: {
                    marker: {
                        enabled: true
                    }
                },
                area: {
                    fillColor: {
                        linearGradient: {
                            x1: 0,
                            y1: 0,
                            x2: 0,
                            y2: 1
                        },
                        stops: [
                            [0, Highcharts.getOptions().colors[0]],
                            [1, Highcharts.Color(Highcharts.getOptions().colors[0]).setOpacity(0).get('rgba')]
                        ]
                    },
                    marker: {
                        radius: 2
                    },
                    lineWidth: 1,
                    states: {
                        hover: {
                            lineWidth: 1
                        }
                    },
                    threshold: null
                }
            },

            series: seriesOptions
        });
    };

    $.each(names, function (i, name) {

        $.getJSON('http://127.0.1.1:30303/m/d/' + name + '.json.' + datestr + '?date=' + datestr + '&callback=?', function (data) {

            seriesOptions[i] = {
                type: 'area',
                name: snames[i],
                data: data
            };

            seriesCounter += 1;

            if (seriesCounter === names.length) {
                createChart();
            }
        });
    });
}
