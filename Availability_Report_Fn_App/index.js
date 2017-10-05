module.exports = function (context, myTimer) {
    // var timeStamp = new Date().toISOString();
    
    var azure               = require('azure-storage');

    var timeStamp           = Date.now();
    var avail_period_in_hr  = 24 * 7;
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
            devID         : entGen.String(msg.devID),
            availability  : msg.avail,
        };

        tableService.insertEntity(table, tableentr, function (error, result, response) {
            if(!error){
                context.log('Entity inserted');
                context.done();
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
            var lastseen_filterd = queryentr.filter(lastseen_filter);
            lastseen_filterd.forEach(update_start);
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
            var availability = [];
            Object.keys(grouped_devIDs_period).forEach(function(key){
                var value = grouped_devIDs_period[key] * 100 / avail_period_in_ms;
                availability.push({devID:key, avail:value});
            }); 
            ////////////////////////////////////////////////////////////////////////
           
            //Create, Add, update Availability Report///////////////////////////////
            //Create Table if not exist
            tableService.createTableIfNotExists(Availability_table, function(error, result, response) {
                if (error) {
                    context.log("Error Creating ", Events_History_table );
                }
            });           

            //Check Availability Table
            tableService.queryEntities(Availability_table, null, null, function(error, result, response) 
            {
               if(!error) 
               {          
                    var queryentr = result.entries; 
                    queryentr.reverse(); 
                    availability.forEach(function(item)
                    {
                        var indx = queryentr.findIndex(function (entr)
                        {
                            return entr.devID._ == item.devID;
                        });

                        if(indx < 0)
                        {
                            //Add new entry of devID not exist 
                            tablestrg_add_msg(item, Availability_table); 
                        }
                        else
                        {
                            //Update already existing entry
                            queryentr[indx].availability._  = item.avail; 
                            tableService.replaceEntity(Availability_table, queryentr[indx], function(error, result, response)
                            {
                                if(!error) {
                                    context.log(' Entity updated ' );
                                    context.done();
                                }
                            });
                        }
                    });
                }
            }); 
            ///////////////////////////////////////////////////////////////////////////
        }
    }); 

    if(myTimer.isPastDue)
    {
        context.log('JavaScript is running late!');
    }
    context.log('JavaScript timer trigger function ran!', timeStamp);   
    
    context.done();
};