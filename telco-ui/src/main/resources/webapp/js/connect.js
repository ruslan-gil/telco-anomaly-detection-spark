var source = new EventSource('/talk');

source.addEventListener('init', function(e) {
    console.log('init');
    console.log(JSON.parse(e.data));
    onInit(JSON.parse(e.data));
}, false);


var svgContainer = d3.select("#universe").append("svg")
                                        .attr("width", 200)
                                        .attr("height", 200);

function onInit(data) {
    var display = [];
    for (var theta = 0; theta < 2 * Math.PI; theta += 0.01) {
        var x = Math.cos(theta) + data.x0;
        var y = Math.sin(theta) + data.y0;
        var local_power = Math.pow(10, power(data, x, y) / 20);
//        console.log({"x": local_power*x, "y": local_power*y});
        display.push({"x": local_power*x, "y": local_power*y});
    }
//    console.log(display);

     //This is the accessor function we talked about above
    var lineFunction = d3.svg.line()
                              .x(function(d) { return d.x; })
                              .y(function(d) { return d.y; })
                             .interpolate("linear");


    //The line SVG Path we draw
    var lineGraph = svgContainer.append("path")
                                .attr("id", data.towerId)
                                .attr("d", lineFunction(display))
                               .attr("stroke", "blue")
                               .attr("stroke-width", 2)
                                .attr("fill", "none");
}


function power(data, x, y) {
    var rSquared = (x - data.x0) * (x - data.x0) + (y - data.y0) * (y - data.y0);
    if (rSquared <= data.r0Squared) {
        return dbm(data.p0);
    } else {
        var theta = Math.atan2(y - data.y0, x - data.x0) - data.theta0;
        return dbm(antennaGain(data, theta) * data.r0Squared / rSquared * data.p0);
    }
}

function antennaGain(data, theta) {
    var directivity = data.scale * (1 - data.eccentricity * data.eccentricity) / (1 - data.eccentricity * Math.cos(theta));
    var cardiod = Math.abs(Math.cos(data.lobes * theta));
    return cardiod * directivity;
}

function dbm(power) {
    return 20 * Math.log10(power / 1e-3);
}
