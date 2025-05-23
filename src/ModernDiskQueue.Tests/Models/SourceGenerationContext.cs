// -----------------------------------------------------------------------
// <copyright file="SourceGenerationContext.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

namespace ModernDiskQueue.Tests.Models
{
    using System;
    using System.Text.Json;
    using System.Text.Json.Serialization;

    [JsonSourceGenerationOptions(GenerationMode = JsonSourceGenerationMode.Metadata, Converters = new[] { typeof(IPAddressConverter) })]
    [JsonSerializable(typeof(TestClassSlim))]
    internal partial class SourceGenerationContext : JsonSerializerContext
    {
        public class IPAddressConverter : JsonConverter<System.Net.IPAddress>
        {
            /// <inheritdoc/>
            public override System.Net.IPAddress Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                return System.Net.IPAddress.Parse(reader.GetString() ?? "0.0.0.0");
            }

            /// <inheritdoc/>
            public override void Write(Utf8JsonWriter writer, System.Net.IPAddress value, JsonSerializerOptions options)
            {
                writer.WriteStringValue(value.ToString());
            }
        }
    }
}