# ModernDiskQueue

**NuGet Package:** https://nuget.org/packages/ModernDiskQueue

**Source Repository:** https://github.com/pracsol/ModernDiskQueue

**Original DiskQueue project is at:** https://github.com/i-e-b/DiskQueue

## Description
ModernDiskQueue is a fork of DiskQueue, a robust, thread-safe, and multi-process persistent queue.

**MDQ v2.*** - Added first-class async support and support for consumer DI containers. This version retains all the existing synchronous functionality, but the two APIs should not be invoked interchangeably or within the same context.

**MDQ v1.\*** - Upgraded the runtime to .NET 8, but otherwise functionally equivalent to the original DiskQueue synchronous library.

The original DiskQueue project was created by Iain Ballard
and based very heavily on Ayende Rahien's work documented at http://ayende.com/blog/3479/rhino-queues-storage-disk


## Requirements and Environment

- Requires access to filesystem storage
- Only partial support for "Trim Self-Contained" deployments and executables (see below)

The file system is used to hold locks, so any bug in your file system may cause
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
* Generally speaking, ALWAYS wrap your `IPersistentQueue` and `IPersistentQueueSession` in an `await using()` block, or otherwise call `DisposeAsync()` manually. Failure to do this will result in lock contention -- you will get errors that the queue is still in use. Be careful about using the simplified `await using` declarations introduced in C# 8.0 (no `{}`) when conducting multiple operations. Generally, you have more control over the scope of the object using the classic `await using [obj] {..}` statements.
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
 - There is also a generic-typed `PersistentQueue<T>(...);` which will handle the serialisation and deserialization of elements in the queue. Use `new PersistentQueue<T>(...)` in place of `new PersistentQueue(...)` or `PersistentQueue.WaitFor<T>(...)` in place of `PersistentQueue.WaitFor(...)` in any of the examples below.
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
				var obj = await session.DequeueAsync());
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
you call `session.FlushAsync()`. Data will only be visible between threads once it has been flushed.
Each flush incurs a performance penalty. By default, each flush is persisted to disk before continuing. You 
can get more speed at a safety cost by setting `queue.ParanoidFlushing = false;`. Additionally, the transaction log will get cleaned up of unnecessary operations (e.g. dequeued items) each time the queue itself is disposed if `TrimTransactionLogOnDispose` is set to true. This is the default behaviour. Because the trimming of the transaction log involves disk IO, there is a performance penalty each time this operation happens.

Each instance of a `PersistentQueue` has it's own settings for flush levels and corruption behaviour. You can set these individually after creating an instance.

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
using (var queue = _factory.WaitForAsync(path, throwOnConflict: false)) {
    . . .
}
```

Similarly, the value `IPersistentQueue.AllowTruncatedEntries` affects how the library behaves when it encounters incomplete data entries. If set to false, an exception will be thrown when a truncated entry is found. This is the default behavior. If set to true, the library will skip over truncated entries and continue processing.

By default, the queues are set up to prioritize data integrity over performance.

## Global default settings

### Async API
In the async api, the global defaults are specified with the ModernDiskQueueOptions class, and provided when configuring services.
```csharp
// Add options configuration. These are the default
// settings, but just listed here to illustrate how
// they can be set initially.
services.AddModernDiskQueue(options =>
{
    options.ParanoidFlushing = true;
    options.TrimTransactionLogOnDispose = true;
    options.AllowTruncatedEntries = false;
    options.SetFilePermissions = false;
    options.FileTimeoutMilliseconds = 10000;
});
```

Each instance of a queue created using the `IPersistentQueueFactory.CreateAsync()` or `IPersistentQueueFactory.WaitForAsync()` methods can have these values set directly, which will override the global defaults if you need to affect a specific queue's behavior at runtime. This behaves differently than the sync API, in that the async API does not envision the *global defaults* being changed at runtime.

### Sync API

Global default settings can be set using the `PersistentQueue.DefaultSettings' static class.

Default settings are applied to all queue instances *in the same process* created *after* the setting is changed. However, SetFilePermissions will affect all queues, including existing instances, when the default is changed.
```csharp
PersistentQueue.DefaultSettings.ParanoidFlushing = true;
```

## Logging

### Async API
All logging is performed against the logging context you configure in your DI container. Logging can be a relatively expensive operation, so at present the logging has been kept to a minimum. All points in the legacy sync API that were logged have been converted to output to the injected ILogger.

### Sync API

Some internal warnings and non-critical errors are logged through `PersistentQueue.Log`.
This defaults to stdout, i.e. `System.Console.WriteLine`, but can be replace with any `Action<string>`

## Removing or Resetting Queues

Queues create a directory and set of files for storage. You can remove all files for a queue with the `HardDeleteAsync` method.
If you give true as the reset parameter, the directory will be written again.

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

These implementation details are why activities performed across multiple threads should not be trying to recreate the queue each time they have work to do; instead they should create and dispose of sessions for a long-lived queue object created by the process. Doing otherwise will cause your threads to be using the slower locking mechanism meant for cross-process access when its not necessary.

However, if it's anticipated that multiple processes will be using the same storage location, you'll want the lock file to be very short lived, and this will mean disposing of the queue itself as soon as you can.

The factory method `WatiForAsync` will accomodates multi-process access by attempting to obtain a lock on the queue until the specified timeout has been reached. If each process uses the lock for a short time and waits long enough, they can share a storage location.

This approach works relatively well, but can show its limits when extreme contention is anticipated (many processes conducting many operations each, concurrently). The cross-platform compaitiblity goal of this library design precludes use of OS-specific mutexes which would offer more performance and reliability for now.

If you need the transaction semantics of sessions across multiple processes, try a more robust solution like https://github.com/i-e-b/SevenDigital.Messaging


## More Detailed Examples

### Dependency Injection
For the async support, a simple factory pattern was implemented as the vehicle to create queue objects. This was done to perform internal async initialization tasks which couldn't be done inside constructors, and it will facilitate additional feature development.

A registration helper was built to allow the factory to be registered with your DI container. This is done by calling `services.AddModernDiskQueue()` in your `ConfigureServices` method. By doing this, your logging context will be used by the library.

There is presently no support for registering individual queues in the DI container. Instead, it is contemplated that queues will be created via async initialization in a hosted startup pattern (like a `BackgroundService` or any `IHostedService`). Alternatively, queues may be created and disposed for discrete operations if multiprocess usage of the same storage queue is a design requirement.

#### Async Initialization in Hosted Startup Pattern
This is a conceptual example of how to use the factory in a hosted service. It assumes that the storage queue will be created on service startup and disposed when the service is shut down - that is to say, that the service will be using the queue exclusively and will not be worried about other processes needing access.

This simple example performs eager initialization using a nullable field, but you can implement with a lazy task or other approaches.

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
This is an example of a service that creates and disposes of a queue for each operation. This is useful if you are using the same storage location across multiple processes, or if you want to ensure that the lock on the queue is dropped as soon as possible. It is less performant but is more friendly to cross-process access to the same storage queue.
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

### Queue on one thread, consume on another; retry some exceptions.

**Note** this is one queue being shared between two sessions. You should not open two queue instances for one storage location at once.

```csharp
IPersistentQueue queue = new PersistentQueue("queue_a");
var t1 = new Thread(() =>
{
	while (HaveWork())
	{
		using (var session = queue.OpenSession())
		{
			session.Enqueue(NextWorkItem());
			session.Flush();
		}
	}
});
var t2 = new Thread(()=> {
	while (true) {
		using (var session = queue.OpenSession()) {
			var data = session.Dequeue();
			if (data == null) {Thread.Sleep(100); continue;}
			
			try {
				MaybeDoWork(data)
				session.Flush();
			} catch (RetryException) {
				continue;
			} catch {
				session.Flush();
			}
		}
	}
});

t1.Start();
t2.Start();
```

### Batch up a load of work and have another thread work through it.
```csharp
IPersistentQueue queue = new PersistentQueue("batchQueue");
var worker = new Thread(()=> {
	using (var session = queue.OpenSession()) {
		byte[] data;
		while ((data = session.Dequeue()) != null) {
			MaybeDoWork(data)
			session.Flush();
		}
	}
});

using (var session = queue.OpenSession()) {
	foreach (var item in LoadsOfStuff()) {
		session.Enqueue(item);
	}
	session.Flush();
}

worker.IsBackground = true; // anything not complete when we close will be left on the queue for next time.
worker.Start();
```










## Multi-Process Usage


E.g.
```csharp
...
void AddToQueue(byte[] data) {
	Thread.Sleep(150);
	using (var queue = PersistentQueue.WaitFor(SharedStorage, TimeSpan.FromSeconds(30)))
	using (var session = queue.OpenSession()) {
		session.Enqueue(data);
		session.Flush();
	}
}

byte[] ReadQueue() {
	Thread.Sleep(150);
	using (var queue = PersistentQueue.WaitFor(SharedStorage, TimeSpan.FromSeconds(30)))
	using (var session = queue.OpenSession()) {
		var data = session.Dequeue();
		session.Flush();
		return data;
	}
}
...

```



## Trim Self-Contained Deployments and Executables

In .NET 8, reflection based serialization is disabled by default when a project is
compiled with the `<PublishTrimmed>` attribute set to **true**. As a consequence of this and other
changes to serialization in .NET 8, you are probably better off using source generated serialization for JSON
rather than reflection-based serialization, particularly when publishing trimmed executables.

To do this, you can either use the raw byte-based interfaces and do your own serialisation, 
or create and use a custom `ISerializationStrategy<T>` for your types.

Using `ISerializationStrategy<T>`, you can forego marking your classes with the `[Serializable]` attribute
and instead create a partial class as in the following example. There are of course many options
you can leverage with source generation, so this serves as only a very basic conceptual idea.


```csharp
    internal class ObjectToEnqueue
    {
        public Guid Id { get; set; }
        public DateTimeOffset LastUpdated { get; set; }
    }

    [JsonSourceGenerationOptions(GenerationMode = JsonSourceGenerationMode.Metadata)]
    [JsonSerializable(typeof(ObjectToEnqueue))]
    internal partial class MyAppJsonContext : JsonSerializerContext
    {
    }

    internal class JsonSerializationStrategyForQueueItem : ISerializationStrategy<ObjectToEnqueue>
    {
        public byte[] Serialize(ObjectToEnqueue? data)
        {
            ArgumentNullException.ThrowIfNull(data);

            string json = JsonSerializer.Serialize(data, MyAppJsonContext.Default.ObjectToEnqueue);
            return Encoding.UTF8.GetBytes(json);
        }

        public Report? Deserialize(byte[]? data)
        {
            if (data == null || data.Length == 0) throw new ArgumentNullException(nameof(data));
            string json = Encoding.UTF8.GetString(data);
            return JsonSerializer.Deserialize(json, MyAppJsonContext.Default.ObjectToEnqueue)
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

See
- https://learn.microsoft.com/en-us/dotnet/core/deploying/trimming/incompatibilities#reflection-based-serializers
- https://learn.microsoft.com/en-us/dotnet/core/compatibility/serialization/8.0/publishtrimmed
- https://learn.microsoft.com/en-us/dotnet/standard/serialization/system-text-json/source-generation
- https://github.com/i-e-b/DiskQueue/issues/35
