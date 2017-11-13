var azure               = require('azure-storage');
var connectionString    = 'DefaultEndpointsProtocol=https;AccountName=butterflystorageaccount;AccountKey=M2fwzoGsZ+nlxeKY8wRDlCjXr/YUPkJHFG9cuX0ve3DVYyvugi0lNNOamWV+E45WXQn4kCyigCT9i1+oFbI1QQ==;EndpointSuffix=core.windows.net';
var tableService        = azure.createTableService(connectionString);
var blobSvc             = azure.createBlobService(connectionString);

function add_tableentr(item, table)
{
    var date = Date.now();
    var partitionKey = Math.floor(date / (24 * 60 * 60 * 1000)) + '';

    var entGen = azure.TableUtilities.entityGenerator;

    var entr = {
        PartitionKey  : /*entGen.String*/(partitionKey),
        RowKey        : /*entGen.String*/(item.ID),
        devID         : /*entGen.String*/(item.ID),
        Status        : /*entGen.Boolean*/(item.Pin1)
    };
    
    table.push(entr);
}


function table_update_currentstatus(table, entries, context)
{
    tableService.createTableIfNotExists(table, function(error, result, response) {
        if (error) 
        {
          context.log("Error Creating ", table );
          return;
        }
        else
        {
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
                        tableService.executeBatch(table, batch, function (error, result, response) 
                        {
                            if(error) 
                            {
                                context.log(table,'**** Error Deleting Entries***', error.code);
                                return;
                            }
                            else
                            {
                                context.log('Entities Deleted');
                                batch.clear();
        
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
                }
            });
        }
     }); 

    
}


module.exports = function (context, eventHubMessages) {

    var tablename           = 'NodesCurrentStatusTable';  
    var date                = new Date();
    var containername       = 'butterflycontainer';
    var blobpath            = 'ncsds/'; 

    if(typeof eventHubMessages === 'string')
        var event_msg = JSON.parse("[" + eventHubMessages + "]");
    
    else
        var event_msg = [eventHubMessages];
 
    //var event_msg = [{"projectname":"Butterfly","ID":"403d9c26e44f4078","Pin0":1,"Pin1":1}];

    //Constract the new table
    var tableentr = [];
    event_msg.forEach(function(item){
        if(item.batt_level )//ON/OFF antenna alarm packet
            return;
        else
            add_tableentr(item, tableentr);     
    });

    // if(tableentr.length)
    //     table_update_currentstatus(tablename, tableentr, context);        

    // var streamify = require('stream-array');
    // var streamarray = streamify(tableentr);
    
    // var fs = require('fs');
    // var wstream = fs.createWriteStream('nodecurrentstatus.json');
    // tableentr.forEach(function(item){
    //     wstream.write(item);
    // });
    // wstream.end();

    // var Stream = require('stream');
    // const readable = new Stream.Readable();
    // tableentr.forEach(function(item){
    //     readable.push(item);
    // });
    

    blobSvc.createContainerIfNotExists(containername, function(err, result, response) {
        if (err) {
            console.log("Couldn't create container %s", containername);
            console.error(err);
        } else {
            if (result) {
                console.log('Container %s created', containername);
            } else {
                console.log('Container %s already exists', containername);
            }
            
            if(tableentr.length)
            {
                var strings = "";
                tableentr.forEach(function(item){
                    strings = strings + JSON.stringify(item) + ',';
                });;
                
                //var strings = JSON.stringify(tableentr[0]); 
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

                // blobSvc.createBlockBlobFromStream(
                //     containername,
                //     'my-awesome-stream-blob',
                //     wstream,
                //     tableentr.length,
                //     function(error, result, response){
                //         if(error){
                //             console.log("Couldn't upload stream");
                //             console.error(error);
                //         } else {
                //             console.log('Stream uploaded successfully');
                //         }
                //     });
            }
        }
    });


    

    context.done();
};