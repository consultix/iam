module.exports = function (context, eventHubMessages) {
    context.log(`JavaScript eventhub trigger function called for message array ${eventHubMessages}`);
    
    // eventHubMessages.forEach(message => {
    //     context.log(`Processed message ${message}`);
    // });

    var azure               = require('azure-storage');
    var tablename           = 'NodesCurrentStatusTable';  
    var connectionString    = 'DefaultEndpointsProtocol=https;AccountName=butterflystorageaccount;AccountKey=M2fwzoGsZ+nlxeKY8wRDlCjXr/YUPkJHFG9cuX0ve3DVYyvugi0lNNOamWV+E45WXQn4kCyigCT9i1+oFbI1QQ==;EndpointSuffix=core.windows.net';
    var tableService        = azure.createTableService(connectionString);
    var date                = Date.now();

    if(typeof eventHubMessages === 'string')
        var event_msg = JSON.parse("[" + eventHubMessages + "]");
    
    else
        var event_msg = [eventHubMessages];
 
        //var event_msg = [{"projectname":"Butterfly","ID":"403d9c26e44f4078","Pin0":"high","Pin1":"high"}];

    tableService.createTableIfNotExists(tablename, function(error, result, response) {
       if (error) {
         context.log("Error Creating ", tablename );
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
            Status        : msg.Pin1,
            Date          : date
        };

        tableService.insertEntity(table, tableentr, function (error, result, response) {
            if(!error){
                context.log('Entity inserted');
            }
        });
    }

    tableService.queryEntities(tablename, null, null, function(error, result, response) 
    {
       if(!error) 
       {          
            var queryentr     =  result.entries; 
            queryentr.reverse(); 

            event_msg.forEach(function(item){

                if(item.batt_level )//ON/OFF antenna alarm packet
                    return;

                var indx = queryentr.findIndex(function (entr)
                {
                    return entr.devID._ == item.ID;
                });
                
                if(indx < 0)
                {
                    context.log('Entry Doesnt exist, we will add it');
                    tablestrg_add_msg(item, tablename);
                }
                else
                {
                    queryentr[indx].Status._  =  item.Pin1;
                    queryentr[indx].Date._    =  date;
                    
                    tableService.replaceEntity(tablename, queryentr[indx], function(error, result, response)
                    {
                        if(!error) {
                            context.log('Entry updated ' );
                        }
                    });
                }
            });
        }
    });   

    context.done();
};