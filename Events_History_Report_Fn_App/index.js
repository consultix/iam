
module.exports = function (context, eventHubMessages) {
    //context.log(`JavaScript eventhub trigger function called for message array ${eventHubMessages}`);
    
    var azure = require('azure-storage');
    
    var Events_History_table    = 'EventsHistoryTable';
    var connectionString        = 'DefaultEndpointsProtocol=https;AccountName=butterflystorageaccount;AccountKey=M2fwzoGsZ+nlxeKY8wRDlCjXr/YUPkJHFG9cuX0ve3DVYyvugi0lNNOamWV+E45WXQn4kCyigCT9i1+oFbI1QQ==;EndpointSuffix=core.windows.net';
    var tableService            = azure.createTableService(connectionString);
    var date                    = Date.now();
    
    if(typeof eventHubMessages === 'string')
        var event_msg = JSON.parse("[" + eventHubMessages + "]");
    
    else
        var event_msg = [eventHubMessages];


    tableService.createTableIfNotExists(Events_History_table, function(error, result, response) {
       if (error) {
           context.log("Error Creating ", Events_History_table );
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
         status: {'_': 0}
     };


    var query = new azure.TableQuery()
        //.top(30)
        //.where('devID eq ?', `${msgparse.ID}`);
        //.where('PartitionKey eq ?', '17359');

    function ishere(entr)
    {
      return JSON.stringify(entr.devID) == JSON.stringify(findentr.devid);
    } 

    function devID_search(entrID)
    {
      return (entrID == findentr.devid._);
    }

    
    tableService.queryEntities(Events_History_table, query, null, function(error, result, response) 
    {
       if(!error) 
       {          
            var queryentr = result.entries; 
            queryentr.reverse(); 
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
                    filter_devID_buff.push( findentr.devid._ );
                ///////////////////////////////////////////////
                var indx = queryentr.findIndex(ishere);
                
                if(indx < 0)
                {
                    context.log('Entry Doesnt exist, we will add it');
                    item.timeperiod = 0 ;
                    item.lastseen   = date;
                    item.start      = date;
                    tablestrg_add_msg(item, Events_History_table);
                }
                else
                {
                    if( queryentr[indx].status._ == findentr.status._ )
                    { 
                        queryentr[indx].period._    = date - queryentr[indx].start._ ;
                        queryentr[indx].lastseen._  = date; 
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
                        item.lastseen   = date; 
                        item.start      = date;                       
                        tablestrg_add_msg(item, Events_History_table);                     
                    }
                }
            });
        }
    });

    context.done();
};