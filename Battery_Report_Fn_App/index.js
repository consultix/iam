var connectionString    = 'DefaultEndpointsProtocol=https;AccountName=butterflystorageaccount;AccountKey=M2fwzoGsZ+nlxeKY8wRDlCjXr/YUPkJHFG9cuX0ve3DVYyvugi0lNNOamWV+E45WXQn4kCyigCT9i1+oFbI1QQ==;EndpointSuffix=core.windows.net';
var azure               = require('azure-storage');
var blobSvc             = azure.createBlobService(connectionString);



function add_tableentr(item, table)
{
    var date = Date.now();
    var partitionKey = Math.floor(date / (24 * 60 * 60 * 1000)) + '';

    var entGen = azure.TableUtilities.entityGenerator;

    var entr = {
        PartitionKey  : (partitionKey),
        RowKey        : (item.ID),
        devID         : (item.ID),
        BatteryVolt   : (item.batt_volt),
        BatteryLevel  : (item.batt_level),
    };
    
    table.push(entr);
}



module.exports = function (context, eventHubMessages) 
{
    var date                = new Date();
    var containername       = 'butterflycontainer';
    var blobpath            = 'batterydatasets/';
    
    if(typeof eventHubMessages === 'string')
        var event_msg = JSON.parse("[" + eventHubMessages + "]");
    
    else
        var event_msg = [eventHubMessages];     

        //var event_msg = {"PartitionKey":"17483","RowKey":"d4711aa652b6ddd1","devID":"d4711aa652b6ddd1","BatteryVolt":6095,"BatteryLevel":95};

    //Constract the new table
    var tableentr = [];
    event_msg.forEach(function(item){
        if(item.batt_level )//ON/OFF antenna alarm packet
            add_tableentr(item, tableentr);     
        else
            return;
    });

    
    if(tableentr.length)
    {
        blobSvc.createContainerIfNotExists(containername, function(err, result, response) {
            if (err) {
                console.log("Couldn't create container %s", containername);
                console.error(err);
            } 
            else 
            {
                if (result) {
                    console.log('Container %s created', containername);
                } else {
                    console.log('Container %s already exists', containername);
                }
                
                {
                    var strings = "";
                    tableentr.forEach(function(item){
                        strings = strings + ',' + JSON.stringify(item);
                    });;
                    
                    blobpath = blobpath + `${date.getFullYear()}/${date.getMonth()+1}/${date.getDate()}/${date.getHours()}/${date.getMinutes()}`;
    
                    blobSvc.createBlockBlobFromText(
                        containername,
                        blobpath,
                        strings,
                        function(error, result, response){
                            if(error){
                                console.log("Couldn't upload string");
                                console.error(error);
                            } else {
                                console.log('String uploaded successfully');
                            }
                        });
                }
            }
        });    
    }
    context.done();
};
