var easyConfig = require('easy-config');
var Sbus = require('sbus-amqp10');
var Q = require("q");

var config = easyConfig.loadConfig();
var eventHubConfiguration = config.eventHubConfiguration;

var hub = Sbus.EventHubClient(eventHubConfiguration.namespace, eventHubConfiguration.hubName, eventHubConfiguration.sasKeyName, eventHubConfiguration.sasKey);
var outputAdapter = require(config.outputAdapter.module);

outputAdapter.initialize(config.outputAdapter)
.then(function() {
    hub.getEventProcessor(eventHubConfiguration.consumerGroup, function (conn_err, processor) {
        if (conn_err) {
            console.log('Error connecting: ' + conn_err);
        } else {
            processor.set_storage(eventHubConfiguration.tableStorageName, eventHubConfiguration.tableStorageKey);
            processor.init(function (rx_err, partition, payload) {
                if (rx_err) {
                    console.log('Error initalizing processor:' + rx_err);
                } else {
                    // Process the JSON payload
                    outputAdapter.newEvents(payload);
                }
            }, function (init_err) {
                if (init_err) {
                    console.log('Error initalizing:' + init_err);
                } else {
                    processor.receive();
                }
            });
        }
    });
});