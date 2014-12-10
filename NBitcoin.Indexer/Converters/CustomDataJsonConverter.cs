using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer.Converters
{
    public class CustomDataConverter : JsonConverter
    {
        Dictionary<string, Type> _States = new Dictionary<string,Type>();

        public void AddKnownType<T>() where T : ICustomData
        {
            _States.Add(typeof(T).Name, typeof(T));
        }

        public override bool CanConvert(Type objectType)
        {
            return typeof(ICustomData).IsAssignableFrom(objectType) && (objectType.IsAbstract || objectType.IsInterface);
        }
        public override bool CanWrite
        {
            get
            {
                return false;
            }
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            if (reader.TokenType == JsonToken.Null)
                return null;

            var obj = serializer.Deserialize<JObject>(reader);
            var type = _States[obj["Type"].Value<string>()];
            return serializer.Deserialize(obj.CreateReader(), type);
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {

        }
    }
}
