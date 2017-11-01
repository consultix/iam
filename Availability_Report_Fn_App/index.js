var azure               = require('azure-storage');
var connectionString    = 'DefaultEndpointsProtocol=https;AccountName=butterflystorageaccount;AccountKey=M2fwzoGsZ+nlxeKY8wRDlCjXr/YUPkJHFG9cuX0ve3DVYyvugi0lNNOamWV+E45WXQn4kCyigCT9i1+oFbI1QQ==;EndpointSuffix=core.windows.net';
var tableService        = azure.createTableService(connectionString);


function groupby_devID(groups, item)
{
    var devid = item.devID._;
    groups[devid] = groups[devid] || [];
    groups[devid].push(item);
    return groups;
}


function Calc_Acc_Periods(table, starttime, state, callback)
{
    //var grouped_devIDs_period ={} ;
    var query = new azure.TableQuery()
        .where('status == ?', state);  
        
    var testarr = [];

    tableService.queryEntities(table, query, null, function(error, result, response) 
        {
            if(!error) 
            {   
                var queryentr     =  result.entries; 
                queryentr.reverse();
                
                //Fitler according to time window/////////////////////////////////////////
                //check lastseen time if more than start_time_to_check consider this row           
                var lastseen_filterd = queryentr.filter(function(entr){
                    var lastseen = Date.parse(entr.lastseen._);
                    return (lastseen > starttime);
                }, starttime);


                lastseen_filterd.forEach(function(entr){
                    var start = Date.parse(entr.start._);
                    if(start < starttime)
                    {
                        start = starttime;
                        entr.period._ = Date.parse(entr.lastseen._) -  start;
                    }
                }, starttime);
                /////////////////////////////////////////////////////////////////////////

                //Grouping devIDs, then calc. sum of periods/////////////////////////////
                //Grouping
                var grouped_devIDs = lastseen_filterd.reduce(groupby_devID, {});
                
                var grouped_devIDs_period = {};
                //Calc sum of periods
                Object.keys(grouped_devIDs).forEach(function (key){
                    grouped_devIDs_period[key] = grouped_devIDs[key].reduce(function(sum_period, item){
                        
                        return sum_period + item.period._; 
                    },0);
                });
                ////////////////////////////////////////////////////////////////////////
            }
            return callback(grouped_devIDs_period); 
        });
}


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
            //context.log('Entity inserted');
        }
    });
}


module.exports = function (context, myTimer) {
    
    var timeStamp           = Date.now();
    var avail_period_in_hr  = 24 * 7;
    var avail_period_in_ms  = avail_period_in_hr * 60 * 60 * 1000.0;
    var start_time_to_check = timeStamp - avail_period_in_ms ;//in ms
  
    var Events_History_table    = 'EventsHistoryTable';
    var Availability_table      = 'AvailabilityTable';

    //Create Availability Report///////////////////////////////
    //Create Table if not exist
    tableService.createTableIfNotExists(Availability_table, function(error, result, response) {
        if (error) {
            context.log("Error Creating ", Events_History_table );
        }
    }); 
    ////////////////////////////////////////////////////////////////////////

    Calc_Acc_Periods(Events_History_table, start_time_to_check, 1, function(on_periods)
    {
        Calc_Acc_Periods(Events_History_table, start_time_to_check, 0, function(off_periods)
        {
            //Calc Availability/////////////////////////////////////////////////////
            var availability = [];
            Object.keys(on_periods).forEach(function(key){
                var off_per;
                if(off_periods[key]) off_per =  off_periods[key];
                else off_per = 0;
                var value = on_periods[key] * 100 / (on_periods[key] + off_per);
                availability.push({devID:key, avail:value});
            });
            ////////////////////////////////////////////////////////////////////////

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
                                }
                            });
                        }
                    });
                }
            }); 
            ///////////////////////////////////////////////////////////////////////////
        });
    });    
    
          
    if(myTimer.isPastDue)
    {
        context.log('JavaScript is running late!');
    }
    context.log('JavaScript timer trigger function ran!', timeStamp);   
    
    context.done();
};