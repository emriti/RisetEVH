using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using System;
using System.Text;
using System.Threading.Tasks;

namespace PublishEvH
{
    class Program
    {
        // connection string to the Event Hubs namespace
        private const string connectionString = "";
        // name of the event hub
        private const string eventHubName = "";
        // number of events to be sent to the event hub
        private const int numOfEvents = 500;
        // The Event Hubs client types are safe to cache and use as a singleton for the lifetime
        // of the application, which is best practice when events are being published or read regularly.
        static EventHubProducerClient producerClient;

        static async Task Main(string[] args)
        {
            try
            {
                await Send(30000);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.StackTrace);
            }
        }

        public static async Task Send(int totalData)
        {
            var totalSent = 0;

            // Create a producer client that you can use to send events to an event hub
            producerClient = new EventHubProducerClient(connectionString, eventHubName);

            // Create a batch of events 
            EventDataBatch eventBatch = await producerClient.CreateBatchAsync();

            var batch = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            for (int i = 1; i <= totalData; i++)
            {
                if (!eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes($"{{\"Code\":\"{i}\", \"Batch\":\"{batch}\"}}"))))
                {
                    // if it is too large for the batch
                    throw new Exception($"Event {i} is too large for the batch and cannot be sent.");
                }

                if (i % numOfEvents == 0)
                {
                    totalSent += numOfEvents;
                    // Use the producer client to send the batch of events to the event hub
                    await producerClient.SendAsync(eventBatch);
                    Console.WriteLine($"[{DateTime.Now.ToString("yyyyMMdd_HHmmss")}] A batch of {totalSent} events has been published.");
                    eventBatch = await producerClient.CreateBatchAsync();
                }
            }

            if (eventBatch.Count > 0)
            {
                totalSent += eventBatch.Count;
                // Use the producer client to send the batch of events to the event hub
                await producerClient.SendAsync(eventBatch);
                Console.WriteLine($"[{DateTime.Now.ToString("yyyyMMdd_HHmmss")}] A batch of {totalSent} events has been published.");
            }

            await producerClient.DisposeAsync();

            Console.WriteLine(batch);
        }
    }
}
