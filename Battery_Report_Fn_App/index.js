var connectionString    = 'DefaultEndpointsProtocol=https;AccountName=butterflystorageaccount;AccountKey=M2fwzoGsZ+nlxeKY8wRDlCjXr/YUPkJHFG9cuX0ve3DVYyvugi0lNNOamWV+E45WXQn4kCyigCT9i1+oFbI1QQ==;EndpointSuffix=core.windows.net';
var azure               = require('azure-storage');
var tableService        = azure.createTableService(connectionString);


function add_tableentr(item, table)
{
    var date = Date.now();
    var partitionKey = Math.floor(date / (24 * 60 * 60 * 1000)) + '';
    var rowKey       = date + '';

    var entGen = azure.TableUtilities.entityGenerator;

    var entr = {
        PartitionKey  : entGen.String(partitionKey),
        RowKey        : entGen.String(item.ID),
        devID         : entGen.String(item.ID),
        BatteryVolt   : entGen.String(item.batt_volt),
        BatteryLevel  : entGen.String(item.batt_level),
    };
    
    table.push(entr);
}



function table_update_currentstatus(table, entries, context)
{
    tableService.createTableIfNotExists(table, function(error, result, response) {
        if (error) {
          context.log("Error Creating ", table );
        }
     }); 

    var query = new azure.TableQuery()

    tableService.queryEntities(table, query, null, function(error, result, response) 
    {
        if(!error) 
        {    
            var batch = new azure.TableBatch();

            //delete all entries
            var queryentr     =  result.entries; 
            queryentr.forEach(function(item)
            {
                batch.deleteEntity(item, {echoContent: true});
            });

            if(batch.hasOperations())
            {
                tableService.executeBatch(table, batch, function (error, result, response) {
                    if(!error) 
                        context.log('Entities Deleted');
            
                    else
                        context.log(table,'**** Error Deleting Entries***', error.code);
                });

                batch.clear();
            }   

            //Insert the new table
            entries.forEach(function(item){
                batch.insertEntity(item, {echoContent: true});
            });    

            tableService.executeBatch(table, batch, function (error, result, response) {
                if(!error) 
                    context.log('Entities inserted');
        
                else
                    context.log(table,'**** Error Inserting Entries***', error.code);
            });
        }
    });
}

module.exports = function (context, eventHubMessages) {

    var batterytable        = 'BatteryTable';  
    var date                = Date.now();
    
    if(typeof eventHubMessages === 'string')
        var event_msg = JSON.parse("[" + eventHubMessages + "]");
    
    else
        var event_msg = [eventHubMessages];     


    //Constract the new table
    var tableentr = [];
    event_msg.forEach(function(item){
        if(item.batt_level )//ON/OFF antenna alarm packet
            add_tableentr(item, tableentr);     
        else
            return;
    });

    
    if(tableentr.length)
        table_update_currentstatus(batterytable, tableentr, context);        

    context.done();
};
