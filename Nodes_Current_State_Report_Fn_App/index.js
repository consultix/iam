var azure               = require('azure-storage');
var connectionString    = 'DefaultEndpointsProtocol=https;AccountName=butterflystorageaccount;AccountKey=M2fwzoGsZ+nlxeKY8wRDlCjXr/YUPkJHFG9cuX0ve3DVYyvugi0lNNOamWV+E45WXQn4kCyigCT9i1+oFbI1QQ==;EndpointSuffix=core.windows.net';
var tableService        = azure.createTableService(connectionString);


function add_tableentr(item, table)
{
    var date = Date.now();
    var partitionKey = Math.floor(date / (24 * 60 * 60 * 1000)) + '';

    var entGen = azure.TableUtilities.entityGenerator;

    var entr = {
        PartitionKey  : entGen.String(partitionKey),
        RowKey        : entGen.String(item.ID),
        devID         : entGen.String(item.ID),
        Status        : entGen.Boolean(item.Pin1)
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

    var tablename           = 'NodesCurrentStatusTable';  
    var date                = Date.now();

    if(typeof eventHubMessages === 'string')
        var event_msg = JSON.parse("[" + eventHubMessages + "]");
    
    else
        var event_msg = [eventHubMessages];
 
        //var event_msg = [{"projectname":"Butterfly","ID":"403d9c26e44f4078","Pin0":1,"Pin1":1}];


    //Constract the new table
    var tableentr = [];
    event_msg.forEach(function(item){
        if(item.batt_level )//ON/OFF antenna alarm packet
            return;
        else
            add_tableentr(item, tableentr);     
    });

    if(tableentr.length)
        table_update_currentstatus(tablename, tableentr, context);        

    context.done();
};