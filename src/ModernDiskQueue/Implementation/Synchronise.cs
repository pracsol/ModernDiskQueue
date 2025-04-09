// Copyright (c) 2005 - 2008 Ayende Rahien (ayende@ayende.com)
// Extensions (c) 2008-2022 Iain Ballard
// Partial (c) 2025 Stephen Shephard
// All rights reserved.
// 
// Redistribution and use in source and binary forms, with or without modification,
// are permitted provided that the following conditions are met:
// 
//     * Redistributions of source code must retain the above copyright notice,
//     this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright notice,
//     this list of conditions and the following disclaimer in the documentation
//     and/or other materials provided with the distribution.
//     * Neither the name of Ayende Rahien nor the name of the product nor the names
//     of its contributors may be used to endorse or promote products derived from
//     this software without specific prior written permission.
// 
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
// THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace ModernDiskQueue.Implementation
{
    /// <summary>
    /// Helper class to properly wait for async tasks
    /// </summary>
    internal static class Synchronise
	{
		private static readonly TaskFactory _taskFactory = new(CancellationToken.None,
				TaskCreationOptions.None,
				TaskContinuationOptions.None,
				TaskScheduler.Default);

		/// <summary>
		/// Run an async function synchronously and return the result
		/// </summary>
		public static TResult Run<TResult>(Func<Task<TResult>> func)
		{
			if (_taskFactory == null) throw new Exception("Static init failed");
            ArgumentNullException.ThrowIfNull(func);

            var rawTask = _taskFactory.StartNew(func).Unwrap();
			if (rawTask == null) throw new Exception("Invalid task");

			return rawTask.GetAwaiter().GetResult();
		}

        /// <summary>
        /// Run an async action synchronously
        /// </summary>
        public static void Run(Func<Task> func)
        {
            ArgumentNullException.ThrowIfNull(func);

            var rawTask = _taskFactory.StartNew(func).Unwrap();
            if (rawTask == null) throw new Exception("Invalid task");

            rawTask.GetAwaiter().GetResult();
        }
    }
}