using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace FunctionPublishEvhConsumption
{
    public class DataDTO
    {
        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("code")]
        public string Code { get; set; }

        [JsonProperty("batch")]
        public string Batch { get; set; }

        [JsonProperty("partitionKey")]
        public string PartitionKey { get; set; }

        [JsonProperty("evhDateTime")]
        public DateTime EventDateTime { get; set; }

        [JsonProperty("enqueuedTimeUtc")]
        public DateTime EnqueuedTimeUtc { get; set; }

        [JsonProperty("sequenceNumber")]
        public Int64 SequenceNumber { get; set; }

        [JsonProperty("offset")]
        public string Offset { get; set; }

        [JsonProperty("evhPK")]
        public string EvhPK { get; set; }

    }
}
