using System;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Storage.Data
{
	public class SliceJsonConverter : JsonConverter
	{
		public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
		{
			var slice = (Slice) value;
			var buffer = new byte[slice.Count];
			Buffer.BlockCopy(slice.Array, slice.Offset, buffer, 0, slice.Count);
			writer.WriteValue(buffer);
			writer.WriteComment(slice.DebugVal);
		}

		public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
		{
			return new Slice(Convert.FromBase64String((string)reader.Value));
		}

		public override bool CanConvert(Type objectType)
		{
			return typeof (Slice) == objectType;
		}
	}
}