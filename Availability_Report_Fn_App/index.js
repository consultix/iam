module.exports = function (context, myTimer) {
    // var timeStamp = new Date().toISOString();
    
    var azure               = require('azure-storage');
    //var queryString         = require('query-string');

    var timeStamp           = Date.now();
    var avail_period_in_hr  = 24 * 3;
    var avail_period_in_ms  = avail_period_in_hr * 60 * 60 * 1000.0;
    var start_time_to_check = timeStamp - avail_period_in_ms ;//in ms
  
  
    var Events_History_table    = 'EventsHistoryTable';
    var Availability_table      = 'AvailabilityTable';
    var connectionString        = 'DefaultEndpointsProtocol=https;AccountName=spectralqualstorage;AccountKey=+aiJXDKs9RrGNu1/XXLglqw8ihm5pNVVHqXCmZ8Om6u47OVWCfy18PuP4D99Ez6zOigh1WpWlHLSKLRrTGRZzw==;EndpointSuffix=core.windows.net';
    var tableService            = azure.createTableService(connectionString);

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

    function groupby_devID(groups, item)
    {
        var devid = item.devID._;
        groups[devid] = groups[devid] || [];
        groups[devid].push(item);
        return groups;
    }


    function update_start(entr)
    {
        if(entr.start._ < start_time_to_check)
        {
            entr.start._ = start_time_to_check;
            entr.period._ = entr.lastseen._ -  entr.start._;
        }
    }
    
    //check lastseen time if more than start_time_to_check consider this row
    function lastseen_filter(entr)
    {
        return (entr.lastseen._ > start_time_to_check);
    }
     
    var query = new azure.TableQuery()
        // .where('(lastseen._ gt ?) and (status == ?)', start_time_to_check, 'high');   
        .where('status == ?', 'high');   
    

    tableService.queryEntities(Events_History_table, query, null, function(error, result, response) 
    {
        if(!error) 
        {   
            var queryentr     =  result.entries; 
            queryentr.reverse();
            
            //Fitler according to timw window/////////////////////////////////////////
            var lastseen_filterd = queryentr;//queryentr.filter(lastseen_filter);for test
            //lastseen_filterd.forEach(update_start);//for test
            /////////////////////////////////////////////////////////////////////////

            //Grouping devIDs, then calc. sum of periods/////////////////////////////
            //Grouping
            var grouped_devIDs = lastseen_filterd.reduce(groupby_devID, {});
            //Calc sum of periods
            var grouped_devIDs_period = [];
            Object.keys(grouped_devIDs).forEach(function (key){
                grouped_devIDs_period[key] = grouped_devIDs[key].reduce(function(sum_period, item){
                      
                      return sum_period + item.period._; 
                },0);
            });
            ////////////////////////////////////////////////////////////////////////

            //Calc Availability/////////////////////////////////////////////////////
            var avail = {};
            Object.keys(grouped_devIDs_period).forEach(function(key){
                var value = grouped_devIDs_period[key] * 100 / avail_period_in_ms;
                avail[key] = value;
            }); 
            ////////////////////////////////////////////////////////////////////////
           
            //Create, Add, update Availability Report///////////////////////////////
            //Create Table if not exist
            tableService.createTableIfNotExists(Availability_table, function(error, result, response) {
                if (error) {
                    context.log("Error Creating ", Events_History_table );
                }
            });

            var findentr = 
            {
                 devid : {'_': 0}
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

            tableService.queryEntities(Availability_table, query, null, function(error, result, response) 
            {
               if(!error) 
               {          
                    var queryentr = result.entries; 
                    queryentr.reverse(); 
                    Object.keys(avail).forEach(function(key)
                    {
                        findentr.devid._  = key;
                        var indx = queryentr.findIndex(ishere);
                        if(indx < 0)
                        {
                           // item.start      = date;
                            //tablestrg_add_msg(item, Availability_table); 
                            ;                               
                        }
                        else
                        {
                            //queryentr[indx].lastseen._  = date; 
                            //tableService.replaceEntity(Events_History_table, queryentr[indx], function(error, result, response)
                            // {
                            //     if(!error) {
                            //         context.log(' Entity updated ' );
                            //     }
                            // });
                            ;
                        }
                    });
                }
            }); 
        }
    }); 



    if(myTimer.isPastDue)
    {
        context.log('JavaScript is running late!');
    }
    context.log('JavaScript timer trigger function ran!', timeStamp);   
    
    context.done();
};