// -----------------------------------------------------------------------
// <copyright file="Extensions.cs" company="ModernDiskQueue Contributors">
// Copyright (c) ModernDiskQueue Contributors. All rights reserved. See LICENSE file in the project root.
// </copyright>
// -----------------------------------------------------------------------

namespace ModernDiskQueue.Tests.Helpers
{
    using System;
    using System.Collections.Generic;

    public static class Extensions
    {
        public static IEnumerable<T> OrEmpty<T>(this IEnumerable<T>? src)
        {
            if (src is null)
            {
                return Array.Empty<T>();
            }

            return src;
        }
    }
}