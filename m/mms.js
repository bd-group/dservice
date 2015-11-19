$(display_all);

function display_all() {
    Highcharts.setOptions({
        global: {
                useUTC: false
        }
    });

    display_two('#container1', "Read/Write Bandwidth", 
                ["Bandwidth (KB/s)", "Bandwidth (KB/s)"], 
                ["Write", "Read"], ['WBW', 'RBW'], [0, 0]);
    display_two("#container2", "Read/Write Request Rate", 
                ["Request per Second (#)", "Request per Second (#)"],
                ["Write", "Read"], ["WRN", "RDN"], [0, 0]);
    display_two('#container3', "Read Latency/Length", 
                ["Latency (ms)", "Object Length (B)"],
                ["Read Latency", "Read Length"], ["RLT", "RBL"], [0, 1]);
    //display_one('#container3', "RBW", "Read Bandwidth", "Read Bandwidth (KB/s)");
    //display_one('#container4', "RLT", "Read Latency", "Read Latency (ms)");
}

function display_one(ctr, jname, tname, yname) {
    var datestr = document.getElementById("dateid").value;
    if (!datestr || datestr.length === 0) {
        today = new Date();
        datestr = today.toLocaleFormat("%Y-%m-%d");
    }
    $.getJSON(window.location.protocol + "//" + window.location.host + 
              '/m/' + mms_dir_type + '/' + jname + '.json.' + datestr + 
              '?date=' + datestr + '&callback=?', function (data) {

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

function display_two(ctr, tname, yname, snames, names, usey) {
    var datestr = document.getElementById("dateid").value;
    if (!datestr || datestr.length === 0) {
        today = new Date();
        datestr = today.toLocaleFormat("%Y-%m-%d");
    }
    var last = [0, 0],
    len = [0, 0],
    seriesOptions = [],
    seriesCounter = 0,
    createChart = function () {
        $(ctr).highcharts({
            chart: {
                type: 'spline',
                zoomType: 'x',
                animation: Highcharts.svg, // don't animate in old IE
                marginRight: 60,
                events: {
                    load: function() {
                        var dseries = this.series;

                        setInterval(function () {
                            $.each(names, function (i, name) {
                                $.getJSON(window.location.protocol + "//" + window.location.host + 
                                          '/m/' + mms_dir_type + '/' + name + '.json.' + datestr + 
                                          '?date=' + datestr + '&last=' + last[i] + '&callback=?', 
                                          function (data) {
                                              var isShift = false;
                                              var j = 0;
                                              if (data.length > 0 && data[data.length - 1][0] != null) {
                                                  last[i] = data[data.length - 1][0] / 1000;
                                                  if (len[i] > 200)
                                                      isShift = true;
                                                  else
                                                      len[i] += data.length;
                                                  data.forEach(function(entry) {
                                                      if (j == data.length - 1)
                                                          dseries[i].addPoint(entry, true, isShift);
                                                      else
                                                          dseries[i].addPoint(entry, false, isShift);
                                                  });
                                              }
                                          });
                            });
                        }, 10000);
                    }
                }
            },
            title: {
                text: 'MMS ' + tname + ' Over Time'
            },
            subtitle: {
                text: document.ontouchstart === undefined ?
                    'Click and drag in the plot area to zoom in' : 
                    'Pinch the chart to zoom in'
            },
            xAxis: {
                type: 'datetime',
            },
            yAxis: [{
                floor: 0,
                title: {
                    text: yname[0]
                },
                plotLines: [{
                    value: 0,
                    width: 1,
                    color: '#808080'
                }]
            },{
                floor: 0,
                opposite: true,
                title: {
                    text: yname[1]
                }
            }],
            legend: {
                enabled: true
            },
            plotOptions: {
                spline: {
                    marker: {
                        enabled: true
                    }
                },
                areaspline: {
                    fillOpacity: 0.3,
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
        $.getJSON(window.location.protocol + "//" + window.location.host + 
                  '/m/' + mms_dir_type + '/' + name + '.json.' + datestr + 
                  '?date=' + datestr + '&last=' + last[i] + '&callback=?', 
                  function (data) {

                      seriesOptions[i] = {
                          type: 'areaspline',
                          name: snames[i],
                          yAxis: usey[i],
                          data: data
                      };
                      
                      if (data.length > 0 && data[data.length -1][0] != null)
                          last[i] = data[data.length - 1][0] / 1000;
                      len[i] = data.length;
                      
                      seriesCounter += 1;
                      
                      if (seriesCounter === names.length) {
                          createChart();
                      }
                  });
    });
}
