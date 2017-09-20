
module.exports = function (context, eventHubMessages) {
    //context.log(`JavaScript eventhub trigger function called for message array ${eventHubMessages}`);
    
    var azure = require('azure-storage');
    
    var Failuire_Table          = 'FailureTable'; 
    var Events_History_table    = 'EventsHistoryTable';    
    var connectionString        = 'DefaultEndpointsProtocol=https;AccountName=spectralqualstorage;AccountKey=+aiJXDKs9RrGNu1/XXLglqw8ihm5pNVVHqXCmZ8Om6u47OVWCfy18PuP4D99Ez6zOigh1WpWlHLSKLRrTGRZzw==;EndpointSuffix=core.windows.net';
    var tableService            = azure.createTableService(connectionString);
    var date                    = Date.now();
    var failure_state           = 'high';
    
    if(typeof eventHubMessages === 'string')
        var event_msg = JSON.parse("[" + eventHubMessages + "]");
    
    else
        var event_msg = [eventHubMessages];


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

        var entGen = azure.TableUtilities.entityGenerator;
    
        var tableentr = {
            PartitionKey  : entGen.String(partitionKey),
            RowKey        : entGen.String(rowKey),
            devID         : entGen.String(msg.ID),
            status        : msg.Pin1,
            start         : msg.start,
            period        : msg.timeperiod,
            lastseen      : msg.lastseen,   
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
         status: {'_': 'high'}
     };


    var query = new azure.TableQuery()

    function ishere(entr)
    {
      return JSON.stringify(entr.devID) == JSON.stringify(findentr.devid);
    } 

    function devID_search(entrID)
    {
      return (entrID == findentr.devid._);
    }


    tableService.queryEntities(Failuire_Table, query, null, function(error, result, response) 
    {
       if(!error) 
       {      
            var failure_queryentr     =  result.entries; 
            failure_queryentr.reverse(); 

            var filter_devID_buff = [];
            event_msg.forEach(function(item){

                // if("battery_level" in item)
                if(item.batt_level )
                    return;

                findentr.devid._  = item.ID;
                findentr.status._ = item.Pin1;

                //filter repeated IDs//////////////////////////
                if(filter_devID_buff.findIndex(devID_search) >= 0)
                    return;
                else
                    filter_devID_buff.push( item.ID );
                ///////////////////////////////////////////////
            
                if(item.Pin1 == failure_state)// Not Failure status, then return
                {
                    var failindx = failure_queryentr.findIndex(ishere);
                    
                    if(failindx < 0)
                    {
                        context.log('Entry Doesnt exist, we will add it');
                        item.timeperiod = 0 ;
                        item.lastseen   = date;
                        item.start      = date;
                        
                        tablestrg_add_msg(item, Failuire_Table);
                    }
                    else
                    {   
                        //check last status at history table
                        tableService.queryEntities(Events_History_table, query, null, function(error, result, response) 
                        {
                           if(!error) 
                           {          
                                var historyqueryentr = result.entries; 
                                historyqueryentr.reverse(); 
                                var hist_indx = historyqueryentr.findIndex(ishere);
                                if(hist_indx >= 0)
                                {
                                    if(historyqueryentr[hist_indx].status._ == failure_state)
                                    {
                                        //update 
                                        failure_queryentr[failindx].period._     = date - failure_queryentr[failindx].start._ ; 
                                        failure_queryentr[failindx].lastseen._   = date;                  
                                        
                                        tableService.replaceEntity(Failuire_Table, failure_queryentr[failindx], function(error, result, response)
                                        {
                                            if(!error) {
                                                context.log(' Entity updated ' );
                                            }
                                        });
                                    }
                                    else
                                    {
                                        //add new entry
                                        context.log('Status Changed, add new entry');
                                        item.timeperiod = 0 ;
                                        item.lastseen   = date; 
                                        item.start      = date;                       
                                        tablestrg_add_msg(item, Failuire_Table);                                          
                                    }
                                }
                                else
                                {
                                    context.log('Error, didnt found at history table');
                                    return;//error, how come
                                }
                           }
                        });
                    }
                }
            }); 
        }
    });

    context.done();
};