var azure = require('azure-storage');

module.exports = function (context, eventHubMessages) {
    //context.log(`JavaScript eventhub trigger function called for message array ${eventHubMessages}`);
    
    // eventHubMessages.forEach(message => {
    //     context.log(`Processed message ${message}`);
    // });

    var Events_History_table    = 'EventsHistoryTable';
    var Failuire_Table          = 'FailureTable';   
    var connectionString        = 'DefaultEndpointsProtocol=https;AccountName=spectralqualstorage;AccountKey=+aiJXDKs9RrGNu1/XXLglqw8ihm5pNVVHqXCmZ8Om6u47OVWCfy18PuP4D99Ez6zOigh1WpWlHLSKLRrTGRZzw==;EndpointSuffix=core.windows.net';
    var tableService            = azure.createTableService(connectionString);
    var date                    = Date.now();
    var failure_state           = 'high';
    
    if(typeof eventHubMessages === 'string')
        var event_msg = JSON.parse("[" + eventHubMessages + "]");
    
    else
        var event_msg = [eventHubMessages];


    tableService.createTableIfNotExists(Events_History_table, function(error, result, response) {
       if (error) {
           context.log("Error Creating ", Events_History_table );
       }
    });

    tableService.createTableIfNotExists(Failuire_Table, function(error, result, response) {
        if (error) {
            context.log("Error Creating ", Failuire_Table );
        }
     });    

  

    function tablestrg_add_msg(msg, table)
    {
        var date = Date.now();
        var partitionKey = Math.floor(date / (24 * 60 * 60 * 1000)) + '';
        var rowKey       = date + '';
        var starttime    = date;  

        var entGen = azure.TableUtilities.entityGenerator;
    
        var tableentr = {
            PartitionKey  : entGen.String(partitionKey),
            RowKey        : entGen.String(rowKey),
            devID         : entGen.String(msg.ID),
            status        : msg.Pin1,
            start         : starttime,
            period        : msg.timeperiod,
            message       : JSON.stringify(msg)
    
        };

        tableService.insertEntity(table, tableentr, function (error, result, response) {
            if(!error){
                context.log('Entity inserted');
            }
        });
    }

    var findentr = 
    {
         devid : {'_': 0},
         status: {'_': 'low'}
     };


    var query = new azure.TableQuery()
        //.top(30)
        //.where('devID eq ?', `${msgparse.ID}`);
        //.where('PartitionKey eq ?', '17359');

    function ishere(entr)
    {
      return JSON.stringify(entr.devID) == JSON.stringify(findentr.devid);
    } 


    tableService.queryEntities(Failuire_Table, query, null, function(error, result, response) 
    {
       if(!error) 
       {          
          var queryentr     =  result.entries; 
           queryentr.reverse(); 


            event_msg.forEach(function(item){

                // if("battery_level" in item)
                if(item.batt_level )
                    return;

                findentr.devid._  = item.ID;
            
                if(item.Pin1 != failure_state)// Not Failure status, then return
                    return;

                var indx = queryentr.findIndex(ishere);
                
                if(indx < 0)
                {
                    context.log('Entry Doesnt exist, we will add it');
                    item.timeperiod = 0 ;
                    tablestrg_add_msg(item, Failuire_Table);
                }
                else
                {                        
                    queryentr[indx].period._ = date - queryentr[indx].start._ ;
                    tableService.replaceEntity(Failuire_Table, queryentr[indx], function(error, result, response)
                    {
                        if(!error) {
                            context.log(' Entity updated ' );
                        }
                    });
                }
            });
        }
    });

    tableService.queryEntities(Events_History_table, query, null, function(error, result, response) 
    {
       if(!error) 
       {          
          var queryentr     =  result.entries; 
           queryentr.reverse(); 


            event_msg.forEach(function(item){

                // if("battery_level" in item)
                if(item.batt_level )
                    return;

                findentr.devid._  = item.ID;
                findentr.status._ = item.Pin1;

                var indx = queryentr.findIndex(ishere);
                
                if(indx < 0)
                {
                    context.log('Entry Doesnt exist, we will add it');
                    item.timeperiod = 0 ;
                    tablestrg_add_msg(item, Events_History_table);
                }
                else
                {
                    if( queryentr[indx].status._ == findentr.status._ )
                    { 
                        queryentr[indx].period._ = date - queryentr[indx].start._ ;
                        tableService.replaceEntity(Events_History_table, queryentr[indx], function(error, result, response)
                        {
                            if(!error) {
                                context.log(' Entity updated ' );
                            }
                        });
                    }
                    else
                    {
                        context.log('Status Changed, add new entry');
                        item.timeperiod = 0 ;
                        tablestrg_add_msg(item, Events_History_table);                     
                    }
                }
            });
        }
    });

    context.done();
};