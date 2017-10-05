module.exports = function (context, eventHubMessages) {
    // context.log(`JavaScript eventhub trigger function called for message array ${eventHubMessages}`);
    
    // eventHubMessages.forEach(message => {
    //     context.log(`Processed message ${message}`);
    // });
    var azure                   = require('azure-storage');
    var TableName               = 'EventsRawDataTable';   
    var connectionString        = 'DefaultEndpointsProtocol=https;AccountName=spectralqualstorage;AccountKey=+aiJXDKs9RrGNu1/XXLglqw8ihm5pNVVHqXCmZ8Om6u47OVWCfy18PuP4D99Ez6zOigh1WpWlHLSKLRrTGRZzw==;EndpointSuffix=core.windows.net';
    var tableService            = azure.createTableService(connectionString);
    var date                    = Date.now();


    if(typeof eventHubMessages === 'string')
       var event_msg = JSON.parse("[" + eventHubMessages + "]");

    else
        var event_msg = [eventHubMessages];


    tableService.createTableIfNotExists(TableName, function(error, result, response) {
        if (error) {
            context.log("Error Creating ", TableName );
        }
    });

    function tablestrg_add_msg(msg, table)
    {
        var date = Date.now();
        var partitionKey = Math.floor(date / (24 * 60 * 60 * 1000)) + '';
        var rowKey       = date + '';

        var entGen = azure.TableUtilities.entityGenerator;
    
        var tableentr = {
            PartitionKey  : entGen.String(partitionKey),
            RowKey        : entGen.String(rowKey),
            devID         : entGen.String(msg.ID),
            status        : msg.Pin1,
            message       : JSON.stringify(msg)
    
        };

        tableService.insertEntity(table, tableentr, function (error, result, response) {
            if(!error){
                context.log('Entity inserted');
                context.done();
            }
        });
    }

    event_msg.forEach(function(item){
        // if("battery_level" in item)
        if(item.batt_level )
            return;

        tablestrg_add_msg(item, TableName);
    });                            
    
    
    context.done();
};