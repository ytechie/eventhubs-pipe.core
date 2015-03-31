var easyConfig = require('easy-config');
var Sbus = require('sbus-amqp10');

config = easyConfig.loadConfig().eventHubConfiguration;

var hub = Sbus.EventHubClient(config.namespace, config.hubName, config.sasKeyName, config.sasKey);

hub.getEventProcessor(config.consumerGroup, function (conn_err, processor) {
    if (conn_err) {
        console.log('Error connecting: ' + conn_err);
    } else {
        processor.set_storage(config.tableStorageName, config.tableStorageKey);
        processor.init(function(rx_err, partition, payload) {
            if (rx_err) {
                console.log('Error initalizing processor:' + rx_err);
            } else {
                // Process the JSON payload
                console.log('JSON Payload: ' + JSON.stringify(payload));
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