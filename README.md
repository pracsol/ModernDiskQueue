# ModernDiskQueue

**NuGet Package:** https://nuget.org/packages/ModernDiskQueue

**Source Repository:** https://github.com/pracsol/ModernDiskQueue

**Original DiskQueue project is at:** https://github.com/i-e-b/DiskQueue

<!--TOC-->
  - [Description](#description)
  - [Requirements and Environment](#requirements-and-environment)
  - [Thanks to](#thanks-to)
  - [Basic Usage](#basic-usage)
    - [Asynchronous Operation (first supported in v2)](#asynchronous-operation-first-supported-in-v2)
    - [Dependency Injection Containers (first supported in v2)](#dependency-injection-containers-first-supported-in-v2)
    - [Original Synchronous Operation](#original-synchronous-operation)
  - [Basic Usage Examples](#basic-usage-examples)
    - [Register MDQ Factory in DI Container](#register-mdq-factory-in-di-container)
    - [Create a queue and add some data using async API and injected factory.](#create-a-queue-and-add-some-data-using-async-api-and-injected-factory.)
    - [Create a queue and add some data with legacy synchronous API](#create-a-queue-and-add-some-data-with-legacy-synchronous-api)
  - [Transactions](#transactions)
  - [Data Loss and Transaction Truncation](#data-loss-and-transaction-truncation)
  - [Global default settings](#global-default-settings)
    - [Async API](#async-api)
    - [Sync API](#sync-api)
  - [Logging](#logging)
    - [Async API](#async-api)
    - [Sync API](#sync-api)
  - [Removing or Resetting Queues](#removing-or-resetting-queues)
    - [Async API](#async-api)
    - [Sync API](#sync-api)
  - [Inter-Thread and Cross-Process Locking](#inter-thread-and-cross-process-locking)
  - [Performance of the Async vs Sync APIs](#performance-of-the-async-vs-sync-apis)
  - [More Detailed Examples](#more-detailed-examples)
    - [Dependency Injection](#dependency-injection)
      - [Async Initialization in Hosted Startup Pattern](#async-initialization-in-hosted-startup-pattern)
      - [Discrete Queue Creation and Disposal](#discrete-queue-creation-and-disposal)
  - [Multi-Process and Multi-Thread Work](#multi-process-and-multi-thread-work)
    - [Multi-Process Work](#multi-process-work)
    - [Multi-Thread Work](#multi-thread-work)
  - [Trim Self-Contained Deployments and Executables](#trim-self-contained-deployments-and-executables)
    - [Dependency Errors in Trimmed Applications](#dependency-errors-in-trimmed-applications)
    - [Serialization Issues in Trimmed Applications](#serialization-issues-in-trimmed-applications)
<!--/TOC-->
## Description
ModernDiskQueue (MDQ) is a fork of DiskQueue, a robust, thread-safe, and multi-process persistent queue. MDQ adds first-class async support, dependency injection semantics, and is built on the latest .NET LTS runtime.

**MDQ v2.*** - Added first-class async support and support for consumer DI containers. This version retains all the existing synchronous functionality, but the two APIs should not be invoked interchangeably.

**MDQ v1.\*** - Upgraded the runtime to .NET 8, but otherwise functionally equivalent to the original DiskQueue synchronous library.

The original DiskQueue project was created by Iain Ballard
and based very heavily on Ayende Rahien's work documented at http://ayende.com/blog/3479/rhino-queues-storage-disk


## Requirements and Environment

- Requires access to filesystem storage
- Only partial support for "Trim Self-Contained" deployments and executables (see below)

The file system is used to hold cross-process locks, so any bug in your file system may cause
issues with DiskQueue -- although it tries to work around them.

## Thanks to

- Tom Halter https://github.com/thalter
- Niklas Rydén https://github.com/nikryden
- Stefan Dascalu https://github.com/stefandascalu64
- Iain Ballard https://github.com/i-e-b

## Basic Usage

### Asynchronous Operation (first supported in v2)
* `PersistentQueueFactory.WaitForAsync(..)` is the main entry point. This will attempt to gain an exclusive lock on the given storage location by calling `.CreateAsync(..)` until the specified timeout expires. `CreateAsync` gracefully fails if the queue is locked and contention is detected. On first use, a directory will be created with the required files inside of it.
* This returned queue object can be shared amongst threads. Each thread should call `OpenSessionAsync()` to get its own session object.
* Generally speaking, ALWAYS wrap your `IPersistentQueue` and `IPersistentQueueSession` in an `await using()` block, or otherwise call `DisposeAsync()` manually. Failure to do this will result in runtime errors or lock contention -- you will get errors that the queue is still in use. Be careful about using the simplified `await using` declarations introduced in C# 8.0 (no `{}`) when conducting multiple operations. Generally, you have more control over the scope of the object using the classic `await using [obj] {..}` statements.
* DO NOT implement queues or sessions using the synchronous patterns when using the asynchronous API. The two implementations are not interchangeable and will cause deadlocks or other issues if you try to mix them. This is largely because the lock awareness mechanisms under the hood are necessarily different for the sync and async APIs. Specifically, use the `PersistentQueueFactory` and its `.CreateAsync()` or `.WaitForAsync()` methods instead of the `PersistentQueue` constructors or the `IPersistentQueue.Create()` or `.WaitFor()` static methods. Always use methods suffixed by *Async* instead of the synchronous equivalents.
* Generic-typed queues are supported with the `.CreateAsync<T>` and `WaitForAsync<T>` methods of the `PersistentQueueFactory` class.

### Dependency Injection Containers (first supported in v2)
* The `PersistentQueueFactory` can be registered with your DI container. This will allow you to inject the factory into your classes and use it to create queues. Use `services.AddModernDiskQueue()` to register the factory with the default settings.
* You can also use `services.AddModernDiskQueue(options => { ... })` to configure the factory with custom default settings. This overrides the GlobalDefaults static values implemented in the original synchronous API.
* Your logging context will be passed to the factory, so you can use your DI container's logging framework to log messages from the queue.

### Original Synchronous Operation
 - The original sync operations of DiskQueue have been preserved in v2. All the implementation guidance for the original DiskQueue is still supported.
 - `PersistentQueue.WaitFor(...)` is the main entry point. This will attempt to gain an exclusive lock
   on the given storage location. On first use, a directory will be created with the required files
   inside it.
 - This queue object can be shared among threads. Each thread should call `OpenSession()` to get its 
   own session object.
 - Both `IPersistentQueue` and `IPersistentQueueSession` objects should be wrapped in `using()` clauses, or otherwise
   disposed of properly. Failure to do this will result in lock contention -- you will get errors that the queue
   is still in use.
 - There is also a generic-typed `PersistentQueue<T>(...);` which will handle the serialization and deserialization of elements in the queue. Use `new PersistentQueue<T>(...)` in place of `new PersistentQueue(...)` or `PersistentQueue.WaitFor<T>(...)` in place of `PersistentQueue.WaitFor(...)` in any of the examples below.
 - You can also assign your own `ISerializationStrategy<T>` 
to your `PersistentQueueSession<T>` if you wish to have more granular control over Serialization/Deserialization, or if you wish to 
use your own serializer (e.g System.Text.Json). This is done by assigning your implementation of `ISerializationStrategy<T>` to `IPersistentQueueSession<T>.SerializationStrategy`.
 - NOTE: `BinaryFormatter` was removed from the default serializer. See https://learn.microsoft.com/en-us/dotnet/standard/serialization/binaryformatter-security-guide.
 - NOTE: Synchronous methods called on a queue instantiated through the `ModernDiskQueueFactory` will throw a runtime exception. A queue instance created by the `ModernDiskQueueFactory` should only use methods whose names are suffixed with *Async*.

## Basic Usage Examples

### Register MDQ Factory in DI Container
```csharp
// Add logging - your implementation may be different, 
// but your logging context will be injected into the 
// ModernDiskQueue factory automatically.
services.AddLogging(builder =>
{
    builder.SetMinimumLevel(LogLevel.Trace);
    builder.AddConsole();
});

services.AddModernDiskQueue();
```

### Create a queue and add some data using async API and injected factory.
Note: this is a functional example; please see the more detailed examples lower down for more practical implementation patterns.
```csharp
public class MyClass
{
	private readonly IPersistentQueueFactory _factory;

	public MyClass(IPersistentQueueFactory factory)
	{
		_factory = factory;
	}

	public async Task EnqueueStuff()
	{
		await using (var queue = await _factory.WaitForAsync(QueuePath, TimeSpan.FromSeconds(5)))
		{
			await using (var session = await queue.OpenSessionAsync())
			{
				await session.EnqueueAsync(new byte[] { 1, 2, 3, 4 });
				// always commit the new enqueue with flushasync
				await session.FlushAsync();
			}
		}
	}

	public async Task DequeueStuff()
	{
		await using (var queue = await _factory.WaitForAsync(QueuePath, TimeSpan.FromSeconds(5)))
		{
			await using (var session = await queue.OpenSessionAsync())
			{
				var obj = await session.DequeueAsync();
				// always commit the dequeue transaction with flushasync
				await session.FlushAsync();
			}
		}
	}
}
```

### Create a queue and add some data with legacy synchronous API
```csharp
using (var queue = PersistentQueue.WaitFor(QueuePath, TimeSpan.FromSeconds(5)))
{
	using (var session = queue.OpenSession())
	{
		session.Enqueue(new byte[] { 1, 2, 3, 4 });
		session.Flush();
	}
}
```

## Transactions
Each session is a transaction. Any Enqueues or Dequeues will be rolled back when the session is disposed unless
you call `session.FlushAsync()`. Data will only be visible between threads once it has been flushed (similar to a database commit).
Each flush incurs a performance penalty due to disk I/O and fsync. By default, each flush is persisted to disk before continuing. You 
can get more speed at a safety cost by setting `queue.ParanoidFlushing = false;`. Additionally, the transaction log will get cleaned up of unnecessary operations (e.g. dequeued items) each time the queue itself is disposed if `TrimTransactionLogOnDispose` is set to true. This is the default behaviour. Because the trimming of the transaction log involves disk IO, there is a performance penalty each time this operation happens.

Each instance of a `PersistentQueue` has its own settings for flush levels and corruption behaviour. You can set these individually after creating an instance.

For example, if performance is more important than crash safety:
```csharp
IPersistentQueue.ParanoidFlushing = false;
IPersistentQueue.TrimTransactionLogOnDispose = false;
```

Or if up-time is more important than detecting corruption early (often the case for embedded systems):
```csharp
IPersistentQueue.ParanoidFlushing = true;
IPersistentQueue.TrimTransactionLogOnDispose = true;
```

## Data Loss and Transaction Truncation
By default, ModernDiskQueue will silently discard transaction blocks that have been truncated; it will throw an `InvalidOperationException`
when transaction block markers are overwritten (this happens if more than one process is using the queue by mistake. It can also happen with some kinds of disk corruption).
If you construct your queue with `throwOnConflict: false`, all recoverable transaction errors will be silently truncated. 

```csharp
await using (var queue = await _factory.WaitForAsync(path, throwOnConflict: false)) {
    . . .
}
```

Similarly, the value `IPersistentQueue.AllowTruncatedEntries` affects how the library behaves when it encounters incomplete data entries. If set to false, an exception will be thrown when a truncated entry is found. This is the default behavior. If set to true, the library will skip over truncated entries and continue processing.

By default, the queues are set up to prioritize data integrity over performance.

## Global default settings

### Async API
In the async api, the global defaults are specified with the ModernDiskQueueOptions class, provided when configuring services.
```csharp
// Add options configuration. These are the default
// settings, and listed here to illustrate how
// they can be set initially using ModernDiskQueueOptions.
services.AddModernDiskQueue(options =>
{
    options.ParanoidFlushing = true;
    options.TrimTransactionLogOnDispose = true;
    options.AllowTruncatedEntries = false;
    options.SetFilePermissions = false;
    options.FileTimeoutMilliseconds = 10000;
});
```

Each instance of a queue created using the `IPersistentQueueFactory.CreateAsync()` or `IPersistentQueueFactory.WaitForAsync()` methods can have these values set directly on a per-instance basis, which will override the global defaults if you need to affect a specific queue's behavior at runtime. This behaves differently than the sync API, in that the async API does not envision the *global defaults* being changed at runtime. Rather, change the settings on a queue instance at runtime if needed.

### Sync API

Global default settings can be set using the `PersistentQueue.DefaultSettings' static class.

Default settings are applied to all queue instances *in the same process* created *after* the setting is changed. The SetFilePermissions value behaves differently in that changes *will* affect behavior for existing queue instances.

This behavior has not changed from the legacy library.
```csharp
PersistentQueue.DefaultSettings.ParanoidFlushing = true;
```

## Logging

### Async API
All logging is performed against the logging context you configure in your DI container. Logging can be an expensive operation so at present the logging has been kept to a minimum. The async API does log more data than the sync API, but any points in the legacy sync API that were logged have been retained in the async API.

| Log Level    | Events |
| -------- | ------- |
| Error | Factory queue creation errors.<br/>Queue session dequeue errors.<br/>Critical errors when locking or unlocking queue.<br/>Errors during hard delete.<br/>Path or permission errors.<br/>Errors accessing the transaction log.
| Warning | Errors generated when accessing meta state. |
| Information  | Queue creation.    |
| Trace/Verbose |  Queue hard delete start and finish.<br/>Session enqueue and dequeue events.<br/>Cross-process lock file creation attempt, failure and success.<br/>Cross-process lock file release.<br/>Queue disposal.<br/>Factory queue access retries (when locked).<br/>Non-critical errors when locking or unlocking queue.|


### Sync API

Some internal warnings and non-critical errors are logged through `PersistentQueue.Log`.
This defaults to stdout, i.e. `System.Console.WriteLine`, but can be replace with any `Action<string>`. This behavior has not changed from the legacy library.

## Removing or Resetting Queues

Queues create a directory and set of files for storage. You can remove all files for a queue with the `HardDeleteAsync` method.
If you give `true` as the reset parameter, the directory will be written again.

WARNING: This WILL delete ANY AND ALL files inside the queue directory. You should not call this method in normal use.
If you start a queue with the same path as an existing directory, this method will delete the entire directory, not just
the queue files.

### Async API
```csharp
await using (var queue = await _factory.CreateAsync(QueuePath))
{
    await queue.HardDeleteAsync(false);
}
```

### Sync API
```csharp
var subject = new PersistentQueue("queue_a");
subject.HardDelete(true); // wipe any existing data and start again
```
NOTE: there is a bug in the legacy sync API that happens when `PersistentQueue.DefaultSettings.TrimTransactionLogOnDispose` is set to `true`. The `HardDelete` method successfully removes files and folder, but when the queue object is disposed, a new transaction.log file is written, recreating the folder. This may be corrected in a future release, but care has been taken to preserve original sync API in all respects for now.

## Inter-Thread and Cross-Process Locking

Internally, thread safety is accomplished with standard lock awareness patterns, but these don't guard against contention between different processes trying to access the same storage location.

To solve this, the queue uses a lock file to indicate that the queue is in use. This file is created with the name 'lock' in the queue directory. It is only written on queue creation, not during session creation. It contains the process ID, the thread ID, and a timestamp. The lock file is deleted when the queue is properly disposed.

These implementation details are why activities performed across multiple *threads* should not be trying to recreate the queue each time they have work to do; instead they should create and dispose of sessions on a long-lived queue object created by the process. Doing otherwise will cause your threads to be using the slower locking mechanism meant for cross-process access when it is not necessary.

However, if it's anticipated that multiple processes will be using the same storage location, you'll want the lock file to be very short lived, and this will mean disposing of the queue itself as soon as you can.

The factory method `WaitForAsync` will accommodate multi-process access by attempting to obtain a lock on the queue until the specified timeout has been reached. If each process uses the lock for a short time and waits long enough, they can share a storage location.

This approach works relatively well, but can show its limits when extreme contention is anticipated (many processes conducting many operations each, concurrently). The cross-platform compatibility goal of this library design precludes use of OS-specific mutexes which would likely offer more performance and reliability. That said, even with cross-process use of the queue, sub-second enqueuing and dequeuing is certainly achievable. There are performance tests in the benchmarks project that you can use to determine whether your needs can be met.

If you need the transaction semantics of sessions across multiple processes, try a more robust solution like https://github.com/i-e-b/SevenDigital.Messaging

## Performance of the Async vs Sync APIs
The async API does bring some extra overhead inherent to the asynchronous context switching, but for all intents and purposes the APIs are equivalent in performance. Mean task completion times between the two seem to be within the margins of error. In tight loops, the sync API seems consistently faster (often by ms), but in highly contentious scenarios the async API seems to have an edge. 

Test design for the async benchmarks has been found to play an incredibly important role, with small adjustments in consumer loop design having a profound impact on performance. You may need to tune your loops to avoid "thundering herd" or starvation issues by introducing random or fixed jitter into each iteration. As well, increasing timeouts waiting for queues can make concurrent threads more tolerant of lock contention.

So generally speaking, the async API should perform just as well as the sync API while adding the semantics of asynchronous programming and other benefits as described earlier.

## More Detailed Examples

### Dependency Injection
For the async support, a simple factory pattern was implemented as the vehicle to create queue objects. This was done to perform internal async initialization tasks which couldn't be done inside constructors; it will also facilitate feature development down the road.

A registration helper was built to allow the factory to be registered with your DI container. This is done by calling `services.AddModernDiskQueue()` in your `ConfigureServices` method. By doing this, your logging context will be used by the library.

There is presently no support for registering individual queues in the DI container. Instead, it is contemplated that queues will be created via async initialization in a hosted startup pattern (like a `BackgroundService` or any `IHostedService`). Alternatively, queues may be created and disposed for discrete operations if multiprocess usage of the same storage queue is a design requirement.

#### Async Initialization in Hosted Startup Pattern
This is a conceptual example of how to use the factory in a hosted service. It assumes that the storage queue will be created on service startup and disposed when the service is shut down - that is to say, the service will be using the queue exclusively and will not be worried about other processes needing access.

This simple example performs eager initialization in the service class using a nullable field, but you can implement with a lazy task or other approaches.

Service configuration:
```csharp
services.AddLogging(builder =>
{
    builder.SetMinimumLevel(LogLevel.Trace);
    builder.AddConsole();
});

services.AddModernDiskQueue();
```

Application infrastructure service implementing MDQ:
```csharp
public class LocalStorageService 
{
	private readonly IPersistentQueueFactory _factory;
	private IPersistentQueue? myQueue;
	
	public LocalStorageService(IPersistentQueueFactory factory)
	{
		_factory = factory;
	}

	public async InitializeService()
	{
		myQueue = await _factory.WaitForAsync("myStoragePath", TimeSpan.FromSeconds(5));
	}
	// Methods for getting and putting objects into queue.
	[...]
}
```

Application hosted service:
```csharp
protected override async Task ExecuteAsync(CancellationToken stoppingToken)
{
	_localStorageService.InitializeService();
	// now do work
	[...]
}
```

#### Discrete Queue Creation and Disposal
This is an example of a service that creates and disposes of a queue for each operation. This is useful if you are using the same storage location across multiple processes, or if you want to ensure that the lock on the queue is dropped as soon as possible. It is far less performant but is more friendly to cross-process access to the same storage queue. Note the use of `await using` blocks to ensure that the queue is disposed of properly, and as soon as we're done with it.

```csharp
public class MyService
{
	private readonly IPersistentQueueFactory _factory;
	public MyService(IPersistentQueueFactory factory)
	{
		_factory = factory;
	}
	public async Task DoWork()
	{
		await using (var queue = await _factory.WaitForAsync("myStoragePath", TimeSpan.FromSeconds(5)))
		{
			await using (var session = await queue.OpenSessionAsync())
			{
				var data = await session.DequeueAsync();
				if (data != null)
				{
					await session.FlushAsync();
					// do work with data
				}
			}
		}
	}
}
```

## Multi-Process and Multi-Thread Work
Some of the examples in the original library showing how to create a thread to enqueue and a thread to dequeue have been left out of this readme. The implementation of such a pattern is a bit outside the scope of this library, particularly as it concerns the async API. But some thoughts follow.

### Multi-Process Work
This scenario is likely going to arise from two discrete applications attempting to access the same storage queue. As described above, the guidance for this scenario is to instantiate and dispose of the queue as quickly as possible so the multi-process file lock is not held for any longer than needed.

### Multi-Thread Work
This scenario is likely to arise from a single application with multiple threads trying to access the same storage queue. The guidance for this scenario is to create a long-lived queue object and then create and dispose of sessions as needed. This will allow the threads to share the queue object and avoid contention and performance penalties associated with the cross-process file locking mechanism.

How this is implemented - your design decisions - can greatly affect performance, and consumers are advised to evaluate the suitability of simply using Task-based asynchronous programming patterns to achieve the same goal. If a dedicated thread is created to handle, for example, all the dequeue operations while the main call stack works on enqueuing objects, be sure to follow best practices for executing asynchronous operations inside a thread. It's easy to do this incorrectly, creating deadlocks, executing blocking synchronous units of work, or simply creating a thread that does nothing more than throwing the body of work back on the shared thread pool, with no thread affinity, which may not yield the results you're after.

## Trim Self-Contained Deployments and Executables

When publishing your consuming application with trimming enabled, you'll encounter two problems: (de)serialization issues and dependency errors. 

### Dependency Errors in Trimmed Applications
Expect to see errors like this when publishing with trimming enabled:
```shell
Unhandled exception. 
System.IO.FileNotFoundException: Could not load file or assembly 'Microsoft.Extensions.Logging
```

Even though the MDQ library has implemented some steps to prevent dependencies from being trimmed, such as using attributes like `DynamicDependency`, these do not propagate preservation requirements to consuming applications.

Therefore, tree-shaking does require some cooperation between libraries and their consumers. Specifically, your application should include explicit references to the libraries MDQ needs if not already doing so for your own needs. Adding the following lines to your project file should avoid these such problems:

```xml
    <PackageReference Include="Microsoft.Extensions.Options" Version="8.0.2" />
    <PackageReference Include="Microsoft.Extensions.DependencyInjection" Version="8.0.1" />
    <PackageReference Include="Microsoft.Extensions.Logging.Abstractions" Version="8.0.2" />
    <PackageReference Include="Microsoft.Extensions.Logging" Version="8.0.1" />
```

### Serialization Issues in Trimmed Applications

In .NET 8, reflection based serialization is disabled by default when a project is
compiled with the `<PublishTrimmed>` attribute set to `true`. 

The default serialization strategy in this library, as in the legacy DiskQueue, is `System.Runtime.Serialization.DataContractSerializer`. .NET 8 uses type adapters that get lost in the trimming process. Getting an error like `No set method for property 'OffsetMinutes' in type 'System.Runtime.Serialization.DateTimeOffsetAdapter'`, even though there most certainly is an internal setter for this property, is a symptom of the tree-shaking.

As a consequence of these challenges, you are probably better off using source generated serialization for JSON
rather than reflection-based serialization, particularly when your consuming application is being published with trimming enabled. The good news is, it's more performant than reflection anyway!

To do this, you can create and use a custom `ISerializationStrategy<T>` for your types.

Using `ISerializationStrategy<T>`, create a partial class as in the following example. There are of course many options
you can leverage with source generation, so this serves as only a very basic conceptual idea of class definition, source generation, and how to implement the ISerializationStrategy when performing enqueue and dequeue operations.


```csharp
    internal class MyCustomClass
    {
        public Guid Id { get; set; }
        public DateTimeOffset LastUpdated { get; set; }
    }

    [JsonSourceGenerationOptions(GenerationMode = JsonSourceGenerationMode.Metadata)]
    [JsonSerializable(typeof(MyCustomClass))]
    internal partial class MyAppJsonContext : JsonSerializerContext
    {
    }

    internal class JsonSerializationStrategyForQueueItem : ISerializationStrategy<MyCustomClass>
    {
        public byte[] Serialize(MyCustomClass? data)
        {
            ArgumentNullException.ThrowIfNull(data);

            string json = JsonSerializer.Serialize(data, MyAppJsonContext.Default.MyCustomClass);
            return Encoding.UTF8.GetBytes(json);
        }

        public MyCustomClass? Deserialize(byte[]? data)
        {
            if (data == null || data.Length == 0) throw new ArgumentNullException(nameof(data));
            string json = Encoding.UTF8.GetString(data);
            return JsonSerializer.Deserialize(json, MyAppJsonContext.Default.MyCustomClass)
                ?? throw new InvalidOperationException("Deserialization returned null");
        }
    }

    using (var session = _queue.OpenSession())
    {
        session.SerializationStrategy = new JsonSerializationStrategyForQueueItem();
        session.Enqueue(testObject);
        session.Flush();
    }
    using (var session = _queue.OpenSession())
    {
        session.SerializationStrategy = new JsonSerializationStrategyForQueueItem();
        var data = session.Dequeue();
        session.Flush();
    }
```

For more info, please see
- https://learn.microsoft.com/en-us/dotnet/core/deploying/trimming/incompatibilities#reflection-based-serializers
- https://learn.microsoft.com/en-us/dotnet/core/compatibility/serialization/8.0/publishtrimmed
- https://learn.microsoft.com/en-us/dotnet/standard/serialization/system-text-json/source-generation
- https://github.com/i-e-b/DiskQueue/issues/35
