// Default URL for triggering event grid function in the local environment.
// http://localhost:7071/runtime/webhooks/EventGrid?functionName={functionname}
using System;
using System.Net.Http;
using System.Text;
using Azure.Core.Pipeline;
using Azure.DigitalTwins.Core;
using Azure.DigitalTwins.Core.Serialization;
using Azure.Identity;
using Microsoft.Azure.EventGrid.Models;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.EventGrid;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace EventGridToDigitalTwin
{
    

    public class EGtoDT
    {
        private static readonly string adtInstanceUrl = Environment.GetEnvironmentVariable("ADT_SERVICE_URL");
        private static readonly HttpClient httpClient = new HttpClient();

        [FunctionName("EGtoDT")]
        public async void Run([EventGridTrigger]EventGridEvent eventGridEvent, ILogger log)
        {
            if (adtInstanceUrl == null) log.LogError("Application setting \"ADT_SERVICE_URL\" not set");

            try
            {
                //Authenticate with Digital Twins

                ManagedIdentityCredential cred = new ManagedIdentityCredential("https://digital-twin-demo-jc.api.wcus.digitaltwins.azure.net");
                DigitalTwinsClient client = new DigitalTwinsClient(
                    new Uri(adtInstanceUrl), cred, new DigitalTwinsClientOptions
                    { Transport = new HttpClientTransport(httpClient) });
                log.LogInformation($"ADT service client connection created.");

                if (eventGridEvent != null && eventGridEvent.Data != null)
                {
                    log.LogInformation(eventGridEvent.Data.ToString());

                    // Reading deviceId, temperature, humidity, pressure, magnetometer, accelerometer and gyroscope for IoT Hub JSON

                    JObject deviceMessage = (JObject)JsonConvert.DeserializeObject(eventGridEvent.Data.ToString());
                    string deviceId = (string)deviceMessage["systemProperties"]["iothub-connection-device-id"];
                    byte[] body = System.Convert.FromBase64String(deviceMessage["body"].ToString());
                    var value = System.Text.ASCIIEncoding.ASCII.GetString(body);
                    var bodyProperty = (JObject)JsonConvert.DeserializeObject(value);
                    var temperature = bodyProperty["temperature"];
                    var humidity = bodyProperty["humidity"];
                    var pressure = bodyProperty["pressure"];
                    var magnetometer_x = bodyProperty["magnetometer_x"];
                    var magnetometer_y = bodyProperty["magnetometer_y"];
                    var magnetometer_z = bodyProperty["magnetometer_z"];
                    var accelerometer_x = bodyProperty["accelerometer_x"];
                    var accelerometer_y = bodyProperty["accelerometer_y"];
                    var accelerometer_z = bodyProperty["accelerometer_z"];
                    var gyroscope_x = bodyProperty["gyroscope_x"];
                    var gyroscope_y = bodyProperty["gyroscope_y"];
                    var gyroscope_z = bodyProperty["gyroscope_z"];

                    log.LogInformation($"Device:{deviceId} Temperature is: {temperature}");

                    //note: AppendReplaceOp only works if model properties are instantiated when twin created.

                    var uou = new UpdateOperationsUtility();

                    if (temperature != null) //accounting for null values from I2C bus on MXChip dev kits
                    {
                        uou.AppendAddOp("/temperature", temperature.Value<double>());
                    }
                    else
                    {
                        temperature = -1000;
                        uou.AppendAddOp("/temperature", temperature.Value<double>());
                    }

                    if (humidity != null)
                    {
                        uou.AppendAddOp("/humidity", humidity.Value<double>());
                    }
                    else
                    {
                        humidity = -1000;
                        uou.AppendAddOp("/humidity", humidity.Value<double>());
                    }

                    if (pressure != null)
                    {
                        uou.AppendAddOp("/pressure", pressure.Value<double>());
                    }
                    else
                    {
                        pressure = -1000;
                        uou.AppendAddOp("/pressure", pressure.Value<double>());
                    }

                    if (magnetometer_x != null)
                    {
                        uou.AppendAddOp("/magnetometer_x", magnetometer_x.Value<double>());
                    }
                    else
                    {
                        magnetometer_x = -1000;
                        uou.AppendAddOp("/magnetometer_x", magnetometer_x.Value<double>());
                    }

                    if (magnetometer_y != null)
                    {
                        uou.AppendAddOp("/magnetometer_y", magnetometer_y.Value<double>());
                    }
                    else
                    {
                        magnetometer_y = -1000;
                        uou.AppendAddOp("/magnetometer_y", magnetometer_y.Value<double>());
                    }

                    if (magnetometer_z != null)
                    {
                        uou.AppendAddOp("/magnetometer_z", magnetometer_z.Value<double>());

                    }
                    else
                    {
                        magnetometer_z = -1000;
                        uou.AppendAddOp("/magnetometer_z", magnetometer_z.Value<double>());
                    }

                    if (accelerometer_x != null)
                    {
                        uou.AppendAddOp("/accelerometer_x", accelerometer_x.Value<double>());

                    }
                    else
                    {
                        accelerometer_x = -1000;
                        uou.AppendAddOp("/accelerometer_x", accelerometer_x.Value<double>());
                    }

                    if (accelerometer_y != null)
                    {
                        uou.AppendAddOp("/accelerometer_y", accelerometer_y.Value<double>());

                    }
                    else
                    {
                        accelerometer_y = -1000;
                        uou.AppendAddOp("/accelerometer_y", accelerometer_y.Value<double>());
                    }

                    if (accelerometer_z != null)
                    {
                        uou.AppendAddOp("/accelerometer_z", accelerometer_z.Value<double>());

                    }
                    else
                    {
                        accelerometer_z = -1000;
                        uou.AppendAddOp("/accelerometer_z", accelerometer_z.Value<double>());
                    }

                    if (gyroscope_x != null)
                    {
                        uou.AppendAddOp("/gyroscope_x", gyroscope_x.Value<double>());

                    }
                    else
                    {
                        gyroscope_x = -1000;
                        uou.AppendAddOp("/gyroscope_x", gyroscope_x.Value<double>());
                    }

                    if (gyroscope_y != null)
                    {
                        uou.AppendAddOp("/gyroscope_y", gyroscope_y.Value<double>());

                    }
                    else
                    {
                        gyroscope_y = -1000;
                        uou.AppendAddOp("/gyroscope_y", gyroscope_y.Value<double>());
                    }

                    if (gyroscope_z != null)
                    {
                        uou.AppendAddOp("/gyroscope_z", gyroscope_z.Value<double>());

                    }
                    else
                    {
                        gyroscope_z = -1000;
                        uou.AppendAddOp("/gyroscope_z", gyroscope_z.Value<double>());
                    }

                    await client.UpdateDigitalTwinAsync(deviceId, uou.Serialize());

                    log.LogInformation($"Device:{deviceId} Pressure is: {pressure}");
                }
            }
            catch (Exception e)
            {
                log.LogError($"Error in ingest function: {e.Message}");
            }
        }
    }
}
