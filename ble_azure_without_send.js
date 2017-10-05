'use strict';

var noble = require('noble');

const fs = require('fs');
const path = require('path');

const wpi = require('wiring-pi');

const Client = require('azure-iot-device').Client;
const ConnectionString = require('azure-iot-device').ConnectionString;
const Message = require('azure-iot-device').Message;
const Protocol = require('azure-iot-device-mqtt').Mqtt;

const bi = require('az-iot-bi');

const MessageProcessor = require('./messageProcessor.js');

var sendingMessage = true;
var messageId = 0;
var client, config, messageProcessor;
var TelemetryPacket = [];
var BatteryPacket = [];
var teststring = '\0';



// Packest from the Estimote family (Telemetry, Connectivity, etc.) are
// broadcast as Service Data (per "§ 1.11. The Service Data - 16 bit UUID" from
// the BLE spec), with the Service UUID 'fe9a'.
var ESTIMOTE_SERVICE_UUID = 'fe9a';  

// Once you obtain the "Estimote" Service Data, here's how to check if it's
// a Telemetry packet, and if so, how to parse it.
function parseEstimoteTelemetryPacket(data) 
{ // data is a 0-indexed byte array/buffer

  // byte 0, lower 4 bits => frame type, for Telemetry it's always 2 (i.e., 0b0010)
  var frameType = data.readUInt8(0) & 0b00001111;
  var ESTIMOTE_FRAME_TYPE_TELEMETRY = 2;
  if (frameType != ESTIMOTE_FRAME_TYPE_TELEMETRY) 
    { return '\0'; }

  // byte 0, upper 4 bits => Telemetry protocol version ("0", "1", "2", etc.)
  var protocolVersion = (data.readUInt8(0) & 0b11110000) >> 4;
  // this parser only understands version up to 2
  // (but at the time of this commit, there's no 3 or higher anyway :wink:)
  if (protocolVersion > 2) 
    { return '\0'; }

  // bytes 1, 2, 3, 4, 5, 6, 7, 8 => first half of the identifier of the beacon
  var shortIdentifier = data.toString('hex', 1, 9);

  // byte 9, lower 2 bits => Telemetry subframe type
  // to fit all the telemetry data, we currently use two packets, "A" (i.e., "0")
  // and "B" (i.e., "1")
  var subFrameType = data.readUInt8(9) & 0b00000011;

  var ESTIMOTE_TELEMETRY_SUBFRAME_A = 0;
  var ESTIMOTE_TELEMETRY_SUBFRAME_B = 1;

  // ****************
  // * SUBFRAME "A" *
  // ****************
  if (subFrameType == ESTIMOTE_TELEMETRY_SUBFRAME_A) 
  {
    // ***** GPIO
    // byte 15, upper 4 bits => state of GPIO pins, one bit per pin
    // 0 = state "low", 1 = state "high"
    //var gpio = {
     var pin0 = (data.readUInt8(15) & 0b00010000) >> 4 ? 'high' : 'low';
     var pin1 = 'high';//(data.readUInt8(15) & 0b00100000) >> 5 ? 'high' : 'low';
      //pin2: (data.readUInt8(15) & 0b01000000) >> 6 ? 'high' : 'low',
      //pin3: (data.readUInt8(15) & 0b10000000) >> 7 ? 'high' : 'low',
    //};

    // ***** ERROR CODES
    var errors;
    if (protocolVersion == 2) {
      // in protocol version "2"
      // byte 15, bits 2 & 3
      // bit 2 => firmware error
      // bit 3 => clock error (likely, in beacons without Real-Time Clock, e.g.,
      //                      Proximity Beacons, the internal clock is out of sync)
      //errors = {
       var hasFirmwareError = ((data.readUInt8(15) & 0b00000100) >> 2) == 1;
       var hasClockError = ((data.readUInt8(15) & 0b00001000) >> 3) == 1;
      //};
    } else if (protocolVersion == 1) {
      // in protocol version "1"
      // byte 16, lower 2 bits
      // bit 0 => firmware error
      // bit 1 => clock error
      //errors = {
        hasFirmwareError = (data.readUInt8(16) & 0b00000001) == 1;
        hasClockError = ((data.readUInt8(16) & 0b00000010) >> 1) == 1;
      //};
    } else if (protocolVersion == 0) {
      // in protocol version "0", error codes are in subframe "B" instead
    }
 
    var tele_message = {projectname:'Butterfly', ID:shortIdentifier, Pin0:pin0, Pin1:pin1};

    return tele_message;
   
  } 
    // **************** 
    // * SUBFRAME "B" *
    // ****************
  else if (subFrameType == ESTIMOTE_TELEMETRY_SUBFRAME_B) 
  {
    var batteryVoltage =
        (data.readUInt8(18)               << 6) |
        ((data.readUInt8(17) & 0b11111100) >> 2);
    if (batteryVoltage == 0b11111111111111) { batteryVoltage = undefined; }

    var batteryLevel;
    if (protocolVersion >= 1) 
    {
      batteryLevel = data.readUInt8(19);
      if (batteryLevel == 0b11111111) { batteryLevel = undefined; }
    }
        
    var tele_message = 
      {
        projectname:'Butterfly', 
        ID:shortIdentifier, 
        batt_volt:batteryVoltage, 
        batt_level:batteryLevel
      };

    return tele_message;
      
  }
 
  else {
    return '\0';

    }
}
// example how to scan & parse Estimote Telemetry packets with noble


function onStart(request, response) {
  console.log('Try to invoke method start(' + request.payload || '' + ')');
  sendingMessage = true;

  response.send(200, 'Successully start sending message to cloud', function (err) {
    if (err) {
      console.error('[IoT hub Client] Failed sending a method response:\n' + err.message);
    }
  });
}


function onStop(request, response) {
  console.log('Try to invoke method stop(' + request.payload || '' + ')')
  sendingMessage = false;

  response.send(200, 'Successully stop sending message to cloud', function (err) {
    if (err) {
      console.error('[IoT hub Client] Failed sending a method response:\n' + err.message);
    }
  });
}



function initClient(connectionStringParam, credentialPath) {
  var connectionString = ConnectionString.parse(connectionStringParam);
  var deviceId = connectionString.DeviceId;

  // fromConnectionString must specify a transport constructor, coming from any transport package.
  client = Client.fromConnectionString(connectionStringParam, Protocol);

  // Configure the client to use X509 authentication if required by the connection string.
  if (connectionString.x509) {
    // Read X.509 certificate and private key.
    // These files should be in the current folder and use the following naming convention:
    // [device name]-cert.pem and [device name]-key.pem, example: myraspberrypi-cert.pem
    var connectionOptions = {
      cert: fs.readFileSync(path.join(credentialPath, deviceId + '-cert.pem')).toString(),
      key: fs.readFileSync(path.join(credentialPath, deviceId + '-key.pem')).toString()
    };

    client.setOptions(connectionOptions);

    console.log('[Device] Using X.509 client certificate authentication');
  }
  return client;
}


(function (connectionString) {
 
    // read in configuration in config.json
    try {
        config = require('./config.json');
    } catch (err) {
        console.error('Failed to load config.json: ' + err.message);
        return;
    }

    messageProcessor = new MessageProcessor(config);

    bi.start()
    bi.trackEvent('success');
    bi.flush();    


    noble.on('stateChange', function(state) {
        console.log('state has changed', state);
        if (state == 'poweredOn') {
            var serviceUUIDs = [ESTIMOTE_SERVICE_UUID]; // Estimote Service
            var allowDuplicates = true;
            noble.startScanning(serviceUUIDs, allowDuplicates, function(error) {
                if (error) {
                    console.log('error starting scanning', error);
                } else {
                    console.log('started scanning');
                }
            });
        }
    });

    var connectionString = "HostName=Spectraqual-free.azure-devices.net;DeviceId=spectralqual_gateway1;SharedAccessKey=d+CD4LnE1b8TELtwrYqBJ0b7UNWes2bv2Uajtoe7DY8=";
    // create a client
    // read out the connectionString from process environment
    connectionString = connectionString || process.env['AzureIoTHubDeviceConnectionString'];
    client = initClient(connectionString, config);

        
    client.open((err) => {
        if (err) {
            console.error('[IoT hub Client] Connect error: ' + err.message);
            return;
        }

        // set C2D and device method callback
        client.onDeviceMethod('start', onStart);
        client.onDeviceMethod('stop', onStop);
        //client.on('message', receiveMessageCallback);
        setInterval(() => {
            client.getTwin((err, twin) => {
                if (err) {
                    console.error("get twin message error");
                    return;
                }
                config.interval = twin.properties.desired.interval || config.interval;
            });
        }, config.interval);      
      
      
        noble.on('discover', function(peripheral) {
          var data = peripheral.advertisement.serviceData.find(function(el) {
              return el.uuid == ESTIMOTE_SERVICE_UUID;
          }).data;

          teststring = parseEstimoteTelemetryPacket(data);
          if( teststring != '\0' )
          {
            if(teststring.batt_volt)
              BatteryPacket.push(teststring); 
            else  
              TelemetryPacket.push(teststring);  
          }
      });

       function Filter_Repetition(buff)
       {
          var grouped_IDs = [];
          buff.forEach(function(item)
          {
            var id = item.ID;
            grouped_IDs[id] = grouped_IDs[id] || [];
            grouped_IDs[id] = item;
          });

          var single_pkt = [];
          Object.keys(grouped_IDs).forEach(function(key)
          {
            single_pkt.push(JSON.stringify(grouped_IDs[key]));
          });

          return single_pkt;
       }

         setInterval(() => {

          if(TelemetryPacket.length != 0) 
          {
            TelemetryPacket = Filter_Repetition(TelemetryPacket);
            console.log("TELEMETRY",TelemetryPacket, TelemetryPacket.length);
            //Azure_Send(TelemetryPacket);
            TelemetryPacket = [] ;
          }

          if(BatteryPacket.lenrth != 0)
          {
            BatteryPacket = Filter_Repetition(BatteryPacket);
            console.log("BATTERY",BatteryPacket, BatteryPacket.length);
            //Azure_Send(BatteryPacket);
            BatteryPacket = [];
          }  
        },1000); //config.interval
    });
         

})(process.argv[2]);
