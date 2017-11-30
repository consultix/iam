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


module.exports = function (context, eventHubMessages) {

    var tablename           = 'NodesCurrentStatusTable';  
    var date                = new Date();
    var containername       = 'butterflycontainer';
    var blobpath            = 'ncsds/'; 

    var hrstr = date.getHours().toString();    //To make sure that the hr string 
    if (hrstr.length < 2) hrstr = '0' + hrstr; //length is two to keep format "HH"

    var daystr = date.getDate().toString();  //To make sure that the day string 
    if (daystr.length < 2) daystr = '0' + daystr; //length is two to keep format "DD"

    var monthstr = date.getMonth() + 1;           //To make sure that the day string 
    monthstr = monthstr.toString();                //length is two to keep format "DD"
    if (monthstr.length < 2) monthstr = '0' + monthstr;


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
                var strings = JSON.stringify(tableentr[0]); 
                blobpath = blobpath + `${date.getFullYear()}/${monthstr}/${daystr}/${hrstr}/${date.getMinutes()}`;

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

    context.done();
};