var source = new EventSource('/talk');

source.addEventListener('init', function(e) {
    console.log('init');
    console.log(JSON.parse(e.data));
    onInit(JSON.parse(e.data));
}, false);

source.addEventListener('move', function(e) {
//    console.log('move');
//    console.log(JSON.parse(e.data));
    onMove(JSON.parse(e.data));
}, false);

source.addEventListener('status', function(e) {
//    console.log('status');
//    console.log(JSON.parse(e.data));
    onStatus(JSON.parse(e.data));
}, false);

source.addEventListener('cdr', function(e) {
//    console.log('cdr');
//    console.log(JSON.parse(e.data));
    onCdr(JSON.parse(e.data));
}, false);

source.addEventListener('event', function (e) {
    console.log('event');
    console.log(JSON.parse(e.data));
    callers.clear();
    for( var [el, cur] of towers.entries()) {
        document.getElementById(`tower-info${cur.towerId}`).remove();
    }
    towers.clear();
    calls.clear();
    for( var [el, cur] of sessions.entries()) {
        document.getElementById(`session-info${cur.sessionId}`).remove();
    }
    sessions.clear();
    svgContainer.selectAll("*").remove();
},false);

var FIELD = {
    input: {
        x: 20000,
        y: 20000
   },
    output: {
        x: 900,
        y: 900
    }
};

var callers = new Map();
var towers = new Map();
var calls = new Map();
var sessions = new Map();

d3.select("#universe").attr("width", FIELD.output.x)
                       .attr("height", FIELD.output.y);

var svgContainer = d3.select("#universe").append("svg")
                                        .attr("width", FIELD.output.x)
                                        .attr("height", FIELD.output.y)
                                        .style("border", "1px solid black");

var ramp=d3.scale.quantile().domain([0,0.1, 0.6, 0.9, 1]).range(["green","orange", "red", "black"]);
var x=d3.scale.linear().domain([0, FIELD.input.x]).range([0, FIELD.output.x]);
var y=d3.scale.linear().domain([0, FIELD.input.y]).range([0, FIELD.output.y]);

function onMove(data) {
    data.x = x(data.x);
    data.y = y(data.y);
    callers.set(data.callerId, data);
}

function onStatus(data) {
    d3.select("#tower"+data.towerId)
        .attr("fill",  ramp(data.fails/data.total));
    if (data.fails/data.total > 0.6) {
        addAlert(data);
    }
}

function onCdr(d) {
   if (d.state == "FINISHED") {
        calls.delete(d.callerId);
        sessions.delete(d.callerId);
        document.getElementById(`session-info${d.sessionId}`).remove();
   } else {
        var tower = towers.get(d.towerId);
        var connection = [{
            x: x(d.x),
            y: y(d.y)
        },{
            x: tower.x0*FIELD.output.x,
            y: tower.y0*FIELD.output.y
        }];
        calls.set(d.callerId, connection);
        var session = {
                    sessionId: d.sessionId,
                    towerId: d.towerId,
                    callerId: d.callerId
                };
        sessions.set(d.callerId, session);
   }
}

function addCalls() {

    var line = d3.svg.line()
        .x(function (d) { return d.x; })
        .y(function (d) { return d.y; });

    var  data = Array.from(calls.values());

    for (var i=0; i < data.length; i++) {
        svgContainer.append("path")
          .attr("class", "call")
          .datum(data[i])
          .attr("d", line);
    }
}

function addAlert(data){
    var fails = data.fails/data.total * 100;
    var alert = `<div class="fragment">
                     <div>\
                         <span class='close' onclick='this.parentNode.parentNode.parentNode.removeChild(this.parentNode.parentNode); return false;'>x</span>
                         <h2>Alert</h2>
                         <p class="text">
                             Tower(${data.towerId}) has ${fails.toFixed(2)}% fails
                         </p>
                     </div>
                 </div>`;
    var element = document.getElementById("alerts");

    var div = document.createElement('div');
        div.style.color = "white";
    if (fails>90) {
        div.style.background = "black";
    } else {
        div.style.background = "#BD4343";
    }

    div.innerHTML = alert;
    element.appendChild(div);
    setTimeout((() => element.removeChild(div)), 1000*10);
}

function addCallers() {
    var circles = svgContainer.selectAll("circle").data(Array.from(callers.values()));

    circles.attr("cx", function (d) {return d.x; })
           .attr("cy", function (d) { return d.y; });

    var new_circles = circles.enter()
                          .append("circle");

    new_circles.attr("cx", function (d) {return d.x; })
        .attr("cy", function (d) { return d.y; })
        .attr("r", 4)
        .attr("id", function (d) { return `caller${d.callerId}`; })
        .style("fill", "blue")
        .style("stroke", "black");

}


function onInit(data) {
    var display = [];
    data.x0 = data.x0/FIELD.input.x;
    data.y0 = data.y0/FIELD.input.y;
    towers.set(data.towerId, data);
    for (var theta = 0; theta < 2 * Math.PI; theta += 0.01) {
        var x = Math.cos(theta) + data.x0;
        var y = Math.sin(theta) + data.y0;
        var local_power = Math.pow(10, power(data, x, y) / 20);
        display.push({"x": (data.x0 + local_power * (x - data.x0))*FIELD.output.x,
                       "y": (data.y0 + local_power * (y - data.y0))*FIELD.output.y});
    }

    var lineFunction = d3.svg.line()
                              .x(function(d) { return d.x; })
                              .y(function(d) { return d.y; })
                             .interpolate("basis-closed");

    var lineGraph = svgContainer.append("path")
                                .attr("id", "tower" + data.towerId)
                                .attr("d", lineFunction(display))
                               .attr("stroke", "#006600")
                               .attr("stroke-width", 2)
                               .style("opacity", 0.5)
                                .attr("fill", "green");
}

setInterval(function(){
    addCallers();
    addCalls();
}, 1000);



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
