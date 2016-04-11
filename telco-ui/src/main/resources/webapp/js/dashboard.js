function getStats() {
    $.get( "/telco/statistics", function( data ) {
      storeStatistics(data);
      visualizeStatistics();
    });
}

setInterval(getStats, 2000);

var statistics = new Map();

var HISTORY = 2;

function storeStatistics(data) {
    for( var el in data) {
        var curStats = data[el];
        if (statistics.has(curStats._id)) {
            var updatedStats = statistics.get(curStats._id);
            updatedStats.push(curStats);
            updatedStats = updatedStats.slice(-HISTORY);
            statistics.set(curStats._id, updatedStats);
        } else {
            statistics.set(curStats._id, [curStats]);
        }
    }
}

function visualizeStatistics() {
     for( var [el, curStats] of statistics.entries()) {
        var lastInfo = curStats[curStats.length-1];
        var info = `<table>
                         <tr>
                             <td rowspan="2">Tower ${el}</td>
                             <td>Status: On | Off | Unknown</td>
                             <td>%of failure: ${(lastInfo.towerFails/lastInfo.towerAllInfo).toFixed(2)}%</td>
                         </tr>
                         <tr>
                             <td>Active Sessions: ${lastInfo.towerAllInfo}</td>
                             <td>Nb of coll per mn/s: ${lastInfo.towerAllInfo}</td>
                             <td>Session Duration: ${(lastInfo.towerDurations/lastInfo.towerAllInfo).toFixed(2)}</td>
                         </tr>
                     </table>`;
        if (document.getElementById(`tower-info${el}`) == undefined) {
            addNewInfo(el, info, lastInfo);
        } else {
            updateInfo(el, info, lastInfo);
        }

    }
}

function addNewInfo(el, info, curStats){
    var element = document.getElementById("towers-info");

    var div = document.createElement('div');
        div.className='block';
        div.style.background = ramp(curStats.towerFails/curStats.towerAllInfo);
        div.style.color = 'white';
        div.id = `tower-info${el}`;
    div.innerHTML = info;
    element.appendChild(div);
}

function updateInfo(el, info, curStats){
    var element = document.getElementById(`tower-info${el}`);
    element.style.background = ramp(curStats.towerFails/curStats.towerAllInfo);
    element.style.color = 'white';
    element.innerHTML = info;
}