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



function table_update_currentstatus(table, entries)
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
    var alarm_tablename     = 'BatteryAlarmTable';  
    var alarm_batt_level    = 95;                       
    var date                = Date.now();
    
    if(typeof eventHubMessages === 'string')
        var event_msg = JSON.parse("[" + eventHubMessages + "]");
    
    else
        var event_msg = [eventHubMessages];     


    //Constract the new table
    var tableentr = [];
    var lowbatt_entrs = [];

    event_msg.forEach(function(item){
        if(item.batt_level )//ON/OFF antenna alarm packet
        {
            add_tableentr(item, tableentr);     
            if(item.batt_level < alarm_batt_level)
                add_tableentr(item, lowbatt_entrs);
        }
        else
            return;
    });

    
    if(tableentr.length)
        table_update_currentstatus(batterytable, tableentr);
    
    // if(lowbatt_entrs.length)
    //     table_update_currentstatus(alarm_tablename, lowbatt_entrs);
        

    context.done();
};



// function tablestrg_add_msg(msg, table)
// {
//     var date = Date.now();
//     var partitionKey = Math.floor(date / (24 * 60 * 60 * 1000)) + '';
//     var rowKey       = date + '';

//     var entGen = azure.TableUtilities.entityGenerator;

//     var tableentr = {
//         PartitionKey  : entGen.String(partitionKey),
//         RowKey        : entGen.String(rowKey),
//         devID         : entGen.String(msg.ID),
//         BatteryVolt   : entGen.String(msg.batt_volt),
//         BatteryLevel  : entGen.String(msg.batt_level),
//     };

//     tableService.insertEntity(table, tableentr, function (error, result, response) {
//         if(!error){
//             context.log('Entity inserted');
//         }
//     });
// }

    // tableService.createTableIfNotExists(alarm_tablename, function(error, result, response) {
    //     if (error) {
    //       context.log("Error Creating ", alarm_tablename );
    //     }
    //  });


    // var findentr = 
    // {
    //      devid : {'_': 0},
    // };

    // function ishere(entr)
    // {
    //   return JSON.stringify(entr.devID) == JSON.stringify(findentr.devid);
    // } 

    // tableService.queryEntities(batterytable, query, null, function(error, result, response) 
    // {
    //    if(!error) 
    //    {      
    //         var queryentr     =  result.entries; 
    //         queryentr.reverse(); 

    //         event_msg.forEach(function(item){

    //             if(item.batt_level )//ON/OFF antenna alarm packet
    //             ;  
    //             else
    //               return;

    //             findentr.devid._  = item.ID;

    //             var indx = queryentr.findIndex(ishere);
                
    //             if(indx < 0)
    //             {
    //                 context.log('Entry Doesnt exist, we will add it');
    //                 tablestrg_add_msg(item, batterytable);
    //             }
    //             else
    //             {
    //                 queryentr[indx].BatteryVolt._  =  item.batt_volt;
    //                 queryentr[indx].BatteryLevel._ =  item.batt_level;
                    
    //                 tableService.replaceEntity(batterytable, queryentr[indx], function(error, result, response)
    //                 {
    //                     if(!error) {
    //                         context.log('Entry updated ' );
    //                     }
    //                 });
    //             }
    //         });
    //     }
    // });  

    //deleete entries which is not updated
    // tableService.queryEntities(batterytable, query, null, function(error, result, response) 
    // {
    //    if(!error) 
    //    { 
    //         var queryentr     =  result.entries;  
    //         queryentr.forEach(function(item)
    //         {
    //             var entrtime = Math.floor(Date.parse(item.Timestamp._)/1000/60);
    //             var currtime = Math.floor(Date.now()/1000/60);
    //             if(entrtime != currtime)
    //                  tableService.deleteEntity('batterytable', item, function(error, response){
    //                     if(!error) {
    //                         context.log('Entry not updated so deleted' );
    //                     }
    //                 });

    //         });
    //    }
    // });
    
    // tableService.queryEntities(alarm_tablename, query, null, function(error, result, response) 
    // {
    //    if(!error) 
    //    {          
    //         var queryentr     =  result.entries; 
    //         queryentr.reverse(); 

    //         event_msg.forEach(function(item){

    //             if(item.batt_level )//ON/OFF antenna alarm packet
    //             ;  
    //             else
    //               return;
                
    //             // if below alarm battery threshold so battery level is low
    //             // and need to update alarm battery table
    //             if(item.batt_level > alarm_batt_level)
    //                return;

    //             findentr.devid._  = item.ID;

    //             var indx = queryentr.findIndex(ishere);
                
    //             if(indx < 0)
    //             {
    //                 context.log('Entry Doesnt exist, we will add it');
    //                 tablestrg_add_msg(item, alarm_tablename);
    //             }
    //             else
    //             {
    //                 queryentr[indx].BatteryVolt._  =  item.batt_volt;
    //                 queryentr[indx].BatteryLevel._ =  item.batt_level;
                    
    //                 tableService.replaceEntity(alarm_tablename, queryentr[indx], function(error, result, response)
    //                 {
    //                     if(!error) {
    //                         context.log('Entry updated ' );
    //                     }
    //                 });
    //             }
    //         });
    //     }
    // });    
    