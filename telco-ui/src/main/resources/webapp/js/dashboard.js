function getStats() {
    $.get( "/telco/statistics", function( data ) {
      storeStatistics(data);
      visualizeStatistics();
    });
    updateSessions();
}

function updateSessions() {
    for( var [el, curStats] of sessions.entries()) {

            var info = `<div class="session-element">
                            <table>

                                <tr>
                                    <td>
                                        <span class="underline">${curStats.sessionId}</span>
                                    </td>
                                    <td>
                                        Tower: <span class="underline">${curStats.towerId}</span>
                                    </td>
                                    <td>
                                        User Id: <span class="underline"> ${curStats.callerId}</span>
                                    </td>
                                </tr>
                            </table>
                         </div>`;
            if (document.getElementById(`session-info${curStats.sessionId}`) == undefined) {
                addNewSession(info, curStats);
            } else {
                updateSession(info, curStats);
            }
        }
}

function addNewSession(info, curStats){
    var element = document.getElementById("sessions");

    var div = document.createElement('div');
        div.className='block';
        div.id = `session-info${curStats.sessionId}`;
    div.innerHTML = info;
    element.appendChild(div);
}

function updateSession(info, curStats){
    var element = document.getElementById(`session-info${curStats.sessionId}`);
    element.innerHTML = info;
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
     var globalStat = new Map();
     var fails = 0;
     var duration = 0;
     var time = 0;
     var total = 0;
     var sessions = 0;
     var resultFailure;
     var resultDuration;

     for( var [el, curStats] of statistics.entries()) {
        var lastInfo = curStats[curStats.length-1];
        fails += lastInfo.towerFails;
        duration += lastInfo.towerDurations;
        time += lastInfo.time;
        total += lastInfo.towerAllInfo;
        sessions += lastInfo.sessions;
        resultFailure = ((lastInfo.towerFails/lastInfo.towerAllInfo)*100);
         if (isNaN(resultFailure)) {
             resultFailure = 0;
         }
        resultDuration = (lastInfo.towerDurations/lastInfo.sessions);
         if (isNaN(resultDuration)) {
             resultDuration = 0;
         }
         
        var info = `<table>
                         <tr>
                             <td rowspan="2">Tower ${lastInfo.towerId}</td>
                             <td>Status: On | Off | Unknown</td>
                             <td>%of failure: ${resultFailure.toFixed(2)}%</td>
                         </tr>
                         <tr>
                             <td>Active Sessions: 0</td>
                             <td>Nb of coll per mn/s: ${(lastInfo.sessions/lastInfo.time).toFixed(2) || 0}</td>
                             <td>Session Duration: ${resultDuration.toFixed(2)}</td>
                         </tr>
                     </table>`;
        if (document.getElementById(`tower-info${lastInfo.towerId}`) == undefined) {
            addNewInfo(el, info, lastInfo);
        } else {
            updateInfo(el, info, lastInfo);
        }
    }

    updateGlobal(fails, duration, time, total, sessions);
}

function addNewInfo(el, info, curStats){
    var element = document.getElementById("towers-info");

    var div = document.createElement('div');
        div.className='block';
        div.style.background = ramp(curStats.towerFails/curStats.towerAllInfo);
        div.style.color = 'white';
        div.id = `tower-info${curStats.towerId}`;
    div.innerHTML = info;
    element.appendChild(div);
}

function updateInfo(el, info, curStats){
    var element = document.getElementById(`tower-info${curStats.towerId}`);
    element.style.background = ramp(curStats.towerFails/curStats.towerAllInfo);
    element.style.color = 'white';
    element.innerHTML = info;
}

function updateGlobal(fails, duration, time, total, sessions){
    var avg_duration = duration/sessions;
    if (isNaN(avg_duration)){
        avg_duration = 0;
    }
    var failures = fails/total;
    if (isNaN(failures)) {
        failures = 0;
    }
    var avg_calls = sessions/time;
    if (isNaN(avg_calls)) {
        avg_calls = 0;
    }
    var element = document.getElementById("avg_duration");
    element.innerHTML = avg_duration.toFixed(2);
    element = document.getElementById("failures");
    element.innerHTML = failures.toFixed(2);
    element = document.getElementById("avg_calls");
    element.innerHTML = avg_calls.toFixed(2);
}