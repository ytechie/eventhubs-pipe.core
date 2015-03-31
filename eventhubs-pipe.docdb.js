var Q = require("q"),
    DocumentClient = require('documentdb-q-promises').DocumentClientWrapper,
    client,
    database,
    docCollection;

function initialize(config) {
    console.log('DocDB adapter initialized with configuration: ' + JSON.stringify(config));

    var deferred = Q.defer();
    
    var databaseDefinition,
        collectionDefinition;
    
    client = new DocumentClient(config.hostName, { masterKey: config.masterKey });
    
    databaseDefinition = { id: config.databaseName }
    collectionDefinition = { id: config.tableName };
    
    initDatabase(databaseDefinition)
        .then(function (dbRef) {
        database = dbRef;
        console.log('Database Initialized');
        return initCollection(database, collectionDefinition);
    })
        .then(function (collRef) {
        docCollection = collRef;
        console.log('Collection Initialized');
        deferred.resolve();
    })
        .fail(function (error) {
        console.log('Error occurred during DocumentDB initialization: ' + error.body);
        deferred.reject(error);
    });
    
    return deferred.promise;
}

function initDatabase(databaseDefinition) {
    var deferred = Q.defer();
    
    client.queryDatabases('SELECT * FROM root r WHERE r.id="' + databaseDefinition.id + '"').toArrayAsync()
        .then(function (results) {
        if (results.feed.length === 0) {
            client.createDatabaseAsync(databaseDefinition)
                    .then(function (database) {
                deferred.resolve(database.resource);
            });
        } else {
            deferred.resolve(results.feed[0]);
        }
    });
    
    return deferred.promise;
}

function initCollection(database, collectionDefinition) {
    var deferred = Q.defer();
    
    client.queryCollections(database._self, 'SELECT * FROM root r WHERE r.id="' + collectionDefinition.id + '"').toArrayAsync()
        .then(function (results) {
        if (results.feed.length === 0) {
            client.createCollectionAsync(database._self, collectionDefinition)
                    .then(function (collection) {
                deferred.resolve(collection.resource);
            });
        } else {
            deferred.resolve(results.feed[0]);
        }
    });
    
    return deferred.promise;
}

function newEvents(events) {
    if (!events) {
        console.warn('newEvents called with no events');
        return;
    }
    
    if (Array.isArray(events)) {
        events.forEach(function (event) {
            newEvent(event);
        });
    } else {
        newEvent(events);
    }
}

function newEvent(event) {
    var deferred = Q.defer();

    event.id = (new Date()).getTime().toString(); //temporary until I figure out how to ID these

    client.createDocumentAsync(docCollection._self, event).then(function(result) {
        console.log('document inserted');
    }).fail(function(error) {
        console.error('Failed to insert document: ' + JSON.stringify(error));
    });

    return deferred.promise;
}

exports.newEvents = newEvents;
exports.initialize = initialize;