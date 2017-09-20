module.exports = function (context, myTimer) {
    // var timeStamp = new Date().toISOString();
    
    var azure               = require('azure-storage');
    var queryString         = require('query-string');

    var timeStamp           = Date();
    var avail_period_in_hr  = 24;
    var avail_period_in_ms  = avail_period_in_hr * 60 * 60 * 1000.0;
    var start_time_to_check = timeStamp - avail_period_in_ms;//in ms

    var Failuire_Table          = 'FailureTable';   
    var connectionString        = 'DefaultEndpointsProtocol=https;AccountName=spectralqualstorage;AccountKey=+aiJXDKs9RrGNu1/XXLglqw8ihm5pNVVHqXCmZ8Om6u47OVWCfy18PuP4D99Ez6zOigh1WpWlHLSKLRrTGRZzw==;EndpointSuffix=core.windows.net';
    var tableService            = azure.createTableService(connectionString);
     
    // var query = new azure.TableQuery();
    // total = query.select(['start']) + query.select(['period']);
    // query.where('total > ?' , 5000);
    
    var query = new azure.TableQuery()
        .where('lastseen gt ?', start_time_to_check);
    


    tableService.queryEntities(Failuire_Table, query, null, function(error, result, response) 
    {
        if(!error) 
        {          
            var queryentr     =  result.entries; 
            queryentr.reverse();
        }
    }); 



    if(myTimer.isPastDue)
    {
        context.log('JavaScript is running late!');
    }
    context.log('JavaScript timer trigger function ran!', timeStamp);   
    
    context.done();
};