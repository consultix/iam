
module.exports = function (context, eventHubMessages) {
    // context.log(`JavaScript eventhub trigger function called for message array ${eventHubMessages}`);
    
    // eventHubMessages.forEach(message => {
    //     context.log(`Processed message ${message}`);
    // });
    var azure               = require('azure-storage');
    var tablename           = 'BatteryTable';  
    var alarm_tablename     = 'BatteryAlarmTable';  
    var connectionString    = 'DefaultEndpointsProtocol=https;AccountName=spectralqualstorage;AccountKey=+aiJXDKs9RrGNu1/XXLglqw8ihm5pNVVHqXCmZ8Om6u47OVWCfy18PuP4D99Ez6zOigh1WpWlHLSKLRrTGRZzw==;EndpointSuffix=core.windows.net';
    var alarm_batt_level    = 95;                       
    var tableService        = azure.createTableService(connectionString);
    var date                = Date.now();
    
    if(typeof eventHubMessages === 'string')
        var event_msg = JSON.parse("[" + eventHubMessages + "]");
    
    else
        var event_msg = [eventHubMessages];
 

    tableService.createTableIfNotExists(tablename, function(error, result, response) {
       if (error) {
         context.log("Error Creating ", tablename );
       }
    }); 

    tableService.createTableIfNotExists(alarm_tablename, function(error, result, response) {
        if (error) {
          context.log("Error Creating ", alarm_tablename );
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
            BatteryVolt   : entGen.String(msg.batt_volt),
            BatteryLevel  : entGen.String(msg.batt_level),
        };

        tableService.insertEntity(table, tableentr, function (error, result, response) {
            if(!error){
                context.log('Entity inserted');
                context.done();
            }
        });
    }


    var findentr = 
    {
         devid : {'_': 0},
    };

    var query = new azure.TableQuery()


    function ishere(entr)
    {
      return JSON.stringify(entr.devID) == JSON.stringify(findentr.devid);
    } 


    tableService.queryEntities(tablename, query, null, function(error, result, response) 
    {
       if(!error) 
       {          
            var queryentr     =  result.entries; 
            queryentr.reverse(); 

            event_msg.forEach(function(item){

                if(item.batt_level )//ON/OFF antenna alarm packet
                ;  
                else
                  return;

                findentr.devid._  = item.ID;

                var indx = queryentr.findIndex(ishere);
                
                if(indx < 0)
                {
                    context.log('Entry Doesnt exist, we will add it');
                    tablestrg_add_msg(item, tablename);
                }
                else
                {
                    queryentr[indx].BatteryVolt._  =  item.batt_volt;
                    queryentr[indx].BatteryLevel._ =  item.batt_level;
                    
                    tableService.replaceEntity(tablename, queryentr[indx], function(error, result, response)
                    {
                        if(!error) {
                            context.log('Entry updated ' );
                            context.done();
                        }
                    });
                }
            });
        }
    });   
    
    
    tableService.queryEntities(alarm_tablename, query, null, function(error, result, response) 
    {
       if(!error) 
       {          
            var queryentr     =  result.entries; 
            queryentr.reverse(); 

            event_msg.forEach(function(item){

                if(item.batt_level )//ON/OFF antenna alarm packet
                ;  
                else
                  return;
                
                // if below alarm battery threshold so battery level is low
                // and need to update alarm battery table
                if(item.batt_level > alarm_batt_level)
                   return;

                findentr.devid._  = item.ID;

                var indx = queryentr.findIndex(ishere);
                
                if(indx < 0)
                {
                    context.log('Entry Doesnt exist, we will add it');
                    tablestrg_add_msg(item, alarm_tablename);
                }
                else
                {
                    queryentr[indx].BatteryVolt._  =  item.batt_volt;
                    queryentr[indx].BatteryLevel._ =  item.batt_level;
                    
                    tableService.replaceEntity(alarm_tablename, queryentr[indx], function(error, result, response)
                    {
                        if(!error) {
                            context.log('Entry updated ' );
                            context.done();
                        }
                    });
                }
            });
        }
    });    
    

    context.done();
};