
namespace ModernDiskQueue.Tests.Models
{
    using Microsoft.Extensions.Logging;
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;

    public class InMemoryLoggerProvider : ILoggerProvider
    {
        private readonly ConcurrentDictionary<string, InMemoryLogger> _loggers = new();

        public List<LogEntry> LogEntries { get; } = new();

        public ILogger CreateLogger(string categoryName)
        {
            return _loggers.GetOrAdd(categoryName, name => new InMemoryLogger(name, LogEntries));
        }

        public void Dispose() { }

        public IEnumerable<string> GetMessages(string category) => LogEntries.Where(e => e.Category == category).Select(e => e.Message);

        public class LogEntry
        {
            public string Category { get; set; } = "";
            public LogLevel Level { get; set; }
            public string Message { get; set; } = "";
            public Exception? Exception { get; set; }
        }

        private class InMemoryLogger : ILogger
        {
            private readonly string _category;
            private readonly List<LogEntry> _logEntries;

            public InMemoryLogger(string category, List<LogEntry> entries)
            {
                _category = category;
                _logEntries = entries;
            }

            public IDisposable BeginScope<TState>(TState state) => new NullScope();

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state,
                Exception exception, Func<TState, Exception, string> formatter)
            {
                _logEntries.Add(new LogEntry
                {
                    Category = _category,
                    Level = logLevel,
                    Message = formatter(state, exception),
                    Exception = exception
                });
            }

            private class NullScope : IDisposable { public void Dispose() { } }
        }
    }

}
