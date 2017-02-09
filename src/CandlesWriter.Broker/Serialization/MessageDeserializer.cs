using System;
using System.Text;
using Newtonsoft.Json;
using Lykke.RabbitMqBroker.Subscriber;
using Lykke.Domain.Prices.Model;

namespace CandlesWriter.Broker.Serialization
{
    public class MessageDeserializer : IMessageDeserializer<Quote>
    {
        public Quote Deserialize(byte[] data)
        {
            JsonSerializerSettings settings = new JsonSerializerSettings()
            {
                DateTimeZoneHandling = DateTimeZoneHandling.Utc // treat datetime as Utc
            };

            string json = Encoding.UTF8.GetString(data);
            return JsonConvert.DeserializeObject<Quote>(json, settings);
        }
    }
}
