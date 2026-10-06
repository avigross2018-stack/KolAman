using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace OverseasConsumer.Models
{
    public class Alert
    {
        [JsonPropertyName("alert_id")]
        [Required]
        public string AlertId { get; set; }

        [JsonPropertyName("source")]
        [Required]
        public string Source { get; set; }

        [JsonPropertyName("title")]
        [Required]
        public string Tilte { get; set; }

        [JsonPropertyName("content")]
        [Required]
        public string Content { get; set; }

        [JsonPropertyName("priority")]
        [Required]
        public string Priority { get; set; }

        [JsonPropertyName("classification")]
        [Required]
        public string Classification { get; set; }

        [JsonPropertyName("lat")]
        [Required]
        public double Lat { get; set; }

        [JsonPropertyName("lon")]
        [Required]
        public double Lon { get; set; }

        [JsonPropertyName("timestamp")]
        [Required]
        public DateTime Timestamp { get; set; }

        [JsonPropertyName("status")]
        [Required]
        public string Status { get; set; }
    }
}