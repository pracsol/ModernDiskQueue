// -----------------------------------------------------------------------
// <copyright file="SourceGenerationContext.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

namespace ModernDiskQueue.Tests.Models
{
    using System;
    using System.Text.Json.Serialization;

    [JsonSourceGenerationOptions(GenerationMode = JsonSourceGenerationMode.Metadata)]
    [JsonSerializable(typeof(TestClassSlim))]
    [JsonSerializable(typeof(TimeZoneInfo))]
    internal partial class SourceGenerationContext : JsonSerializerContext;
}