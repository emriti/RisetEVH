using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FunctionPublishEvhConsumption
{
    public static class SubscribeMessage
    {

        [FunctionName("SubscribeMessage2")]
        public static async Task SubscribeMessage2([EventHubTrigger("feedpost-con-2", Connection = "evh-func-riset-evh-premium")] EventData[] events,
            [CosmosDB(ConnectionStringSetting = "cosmos-db")] DocumentClient client,
            ILogger log)
        {
            var exceptions = new List<Exception>();

            foreach (EventData eventData in events)
            {
                try
                {
                    string messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);
                    dynamic data = JsonConvert.DeserializeObject(messageBody);

                    // Replace these two lines with your processing logic.
                    log.LogInformation($"{data?.Code}");

                    var dto = new DataDTO()
                    {
                        Id = Guid.NewGuid().ToString(),
                        Batch = data?.Batch,
                        Code = data?.Code,
                        PartitionKey = data?.Batch,
                        EventDateTime = DateTime.Now,
                        EnqueuedTimeUtc = eventData.SystemProperties.EnqueuedTimeUtc,
                        SequenceNumber = eventData.SystemProperties.SequenceNumber,
                        Offset = eventData.SystemProperties.Offset,
                        EvhPK = eventData.SystemProperties.PartitionKey
                    };

                    await client.CreateDocumentAsync(UriFactory.CreateDocumentCollectionUri("DataEvh", "Partition2"), dto);

                    await Task.Yield();
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }

        [FunctionName("SubscribeMessage4")]
        public static async Task SubscribeMessage4(
            [EventHubTrigger("feedpost-con-4", Connection = "evh-func-riset-evh-premium")] EventData[] events,
            [CosmosDB(ConnectionStringSetting = "cosmos-db")] DocumentClient client,
            ILogger log)
        {
            var exceptions = new List<Exception>();

            foreach (EventData eventData in events)
            {
                try
                {
                    string messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);
                    dynamic data = JsonConvert.DeserializeObject(messageBody);

                    // Replace these two lines with your processing logic.
                    log.LogInformation($"{data?.Code}");

                    var dto = new DataDTO()
                    {
                        Id = Guid.NewGuid().ToString(),
                        Batch = data?.Batch,
                        Code = data?.Code,
                        PartitionKey = data?.Batch,
                        EventDateTime = DateTime.Now,
                        EnqueuedTimeUtc = eventData.SystemProperties.EnqueuedTimeUtc,
                        SequenceNumber = eventData.SystemProperties.SequenceNumber,
                        Offset = eventData.SystemProperties.Offset,
                        EvhPK = eventData.SystemProperties.PartitionKey
                    };

                    await client.CreateDocumentAsync(UriFactory.CreateDocumentCollectionUri("DataEvh", "Partition4"), dto);

                    await Task.Yield();
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }

        [FunctionName("SubscribeMessage8")]
        public static async Task SubscribeMessage8([EventHubTrigger("feedpost-con-8", Connection = "evh-func-riset-evh-premium")] EventData[] events,
            [CosmosDB(ConnectionStringSetting = "cosmos-db")] DocumentClient client,
            ILogger log)
        {
            var exceptions = new List<Exception>();

            foreach (EventData eventData in events)
            {
                try
                {
                    string messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);
                    dynamic data = JsonConvert.DeserializeObject(messageBody);

                    // Replace these two lines with your processing logic.
                    log.LogInformation($"{data?.Code}");

                    var dto = new DataDTO()
                    {
                        Id = Guid.NewGuid().ToString(),
                        Batch = data?.Batch,
                        Code = data?.Code,
                        PartitionKey = data?.Batch,
                        EventDateTime = DateTime.Now,
                        EnqueuedTimeUtc = eventData.SystemProperties.EnqueuedTimeUtc,
                        SequenceNumber = eventData.SystemProperties.SequenceNumber,
                        Offset = eventData.SystemProperties.Offset,
                        EvhPK = eventData.SystemProperties.PartitionKey
                    };

                    await client.CreateDocumentAsync(UriFactory.CreateDocumentCollectionUri("DataEvh", "Partition8"), dto);

                    await Task.Yield();
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }

        [FunctionName("SubscribeMessage16")]
        public static async Task SubscribeMessage16([EventHubTrigger("feedpost-con-16", Connection = "evh-func-riset-evh-premium")] EventData[] events,
            [CosmosDB(ConnectionStringSetting = "cosmos-db")] DocumentClient client,
            ILogger log)
        {
            var exceptions = new List<Exception>();

            foreach (EventData eventData in events)
            {
                try
                {
                    string messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);
                    dynamic data = JsonConvert.DeserializeObject(messageBody);

                    // Replace these two lines with your processing logic.
                    log.LogInformation($"{data?.Code}");

                    var dto = new DataDTO()
                    {
                        Id = Guid.NewGuid().ToString(),
                        Batch = data?.Batch,
                        Code = data?.Code,
                        PartitionKey = data?.Batch,
                        EventDateTime = DateTime.Now,
                        EnqueuedTimeUtc = eventData.SystemProperties.EnqueuedTimeUtc,
                        SequenceNumber = eventData.SystemProperties.SequenceNumber,
                        Offset = eventData.SystemProperties.Offset,
                        EvhPK = eventData.SystemProperties.PartitionKey
                    };

                    await client.CreateDocumentAsync(UriFactory.CreateDocumentCollectionUri("DataEvh", "Partition16"), dto);

                    await Task.Yield();
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }

        [FunctionName("SubscribeMessage32")]
        public static async Task SubscribeMessage32([EventHubTrigger("feedpost-con-32", Connection = "evh-func-riset-evh-premium")] EventData[] events, 
            [CosmosDB(ConnectionStringSetting = "cosmos-db")] DocumentClient client,
            ILogger log)
        {
            var exceptions = new List<Exception>();

            foreach (EventData eventData in events)
            {
                try
                {
                    string messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);
                    dynamic data = JsonConvert.DeserializeObject(messageBody);

                    // Replace these two lines with your processing logic.
                    log.LogInformation($"{data?.Code}");



                    var dto = new DataDTO()
                    {
                        Id = Guid.NewGuid().ToString(),
                        Batch = data?.Batch,
                        Code = data?.Code,
                        PartitionKey = data?.Batch,
                        EventDateTime = DateTime.Now,
                        EnqueuedTimeUtc = eventData.SystemProperties.EnqueuedTimeUtc,
                        SequenceNumber = eventData.SystemProperties.SequenceNumber,
                        Offset = eventData.SystemProperties.Offset,
                        EvhPK = eventData.SystemProperties.PartitionKey
                    };

                    await client.CreateDocumentAsync(UriFactory.CreateDocumentCollectionUri("DataEvh", "Partition32"), dto);

                    await Task.Yield();
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }




    }
}
