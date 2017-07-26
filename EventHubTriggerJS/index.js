var azure = require('azure-storage');


module.exports = function (context, eventHubMessages) {
    //context.log(`JavaScript eventhub trigger function called for message array ${eventHubMessages}`);
    
    var tablename = 'deviceTable4';   
    var connectionString = 'DefaultEndpointsProtocol=https;AccountName=iamstorage1;AccountKey=5JW2kV/ZPx5DvvTLpE8NTAUxbOEfWYrSMogXsgs/LquqtrnhiquY++tr41KqPfF5oCTezp7ckT9m7w7KVWAHbg==;EndpointSuffix=core.windows.net';
    var tableService = azure.createTableService(connectionString);
    var date         = Date.now();
    
    if(typeof eventHubMessages === 'string')
        var event_msg = JSON.parse("[" + eventHubMessages + "]");
    
    else
        var event_msg = [eventHubMessages];


    tableService.createTableIfNotExists(tablename, function(error, result, response) {
       if (!error) {
        // result contains true if created; false if already exists
         //context.log(`deviceTable create : `,result.created);
       }
    });

  

    function tablestrg_add_msg(msg)
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

        tableService.insertEntity(tablename, tableentr, function (error, result, response) {
            if(!error){
                context.log('Entity inserted', result);
            }
        });
    }


     var findentr = {
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

    

    tableService.queryEntities(tablename, query, null, function(error, result, response) 
    {
       if(!error) 
       {          
          var queryentr     =  result.entries; 
           queryentr.reverse(); 


            event_msg.forEach(function(item){

                findentr.devid._  = item.ID;
                findentr.status._ = item.Pin1;

                var indx = queryentr.findIndex(ishere);
                
                if(indx < 0)
                {
                    context.log('Entry Doesnt exist, we will add it');
                    item.timeperiod = 0 ;
                    tablestrg_add_msg(item);
                }
                else
                {
                    context.log('Entry exist');                        

                    if( queryentr[indx].status._ == findentr.status._ )
                    { 
                        context.log('Update Period');                     
                        queryentr[indx].period._ = date - queryentr[indx].start._ ;
                        tableService.replaceEntity(tablename, queryentr[indx], function(error, result, response)
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
                        tablestrg_add_msg(item);                     

                    }
                }
            });
        }
    });

     
    context.done();
};