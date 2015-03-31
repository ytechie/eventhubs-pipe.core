/*

This is a sample output adapter that just prints to the console 

Output adapters need to handle:
 * Transient failures
 * Rate limiting - report? delay?
 * Significant failures - raise exception
 
*/

var Q = require("q");

function initialize(config) {
    var deferred = Q.defer();
    deferred.resolve();

    console.log('Console output adapter initialized with configuration: ' + JSON.stringify(config));

    return deferred.promise;
}

function newEvents(events) {
    if (!events) {
        console.warn('newEvents called with no events');
        return;
    }
    
    if (Array.isArray(events)) {
        events.forEach(function(event) {
            newEvent(event);
        });
    } else {
        newEvent(events);
    }
}

function newEvent(event) {
    console.log('Event received: ' + JSON.stringify(event));
}

exports.newEvents = newEvents;
exports.initialize = initialize;