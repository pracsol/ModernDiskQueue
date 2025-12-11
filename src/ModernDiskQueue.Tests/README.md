# ModernDiskQueue.Tests

This project contains the tests for ModernDiskQueue built using nUnit and nSubstitute.

## Performance Tests
Both `read_heavy_multi_thread_workload` and `write_heavy_multi_thread_workload` are performance tests that simulate a high load on the disk queue. They measure the time taken to read from and write to the queue under heavy multi-threaded conditions. These are pass/fail tests, and they will fail unless you adjust the performance envelope by changing the thread.sleep duration between the Enqueueing thread starting and the dequeueing threads starting, and also the timeout value in the WaitFor method for the dequeue operations. Your system's IO performance greatly affects these tolerances, so understanding your anticipated load and deployment target performance characteristics are important to leveraging this test successfully.

A test named `PerformanceProfiler_ReadHeavyMultiThread_StatsCollection` functions as a more comprehensive performance profiler if you don't know what your system can sustain. It should be given with generous timeouts to ensure completion, but it will output a report illustrating how many write and read operations can be performed per second on your test machine, helping you understand limitations.

## Testing Trimmed Executables
If you build a self-contained and trimmed executable (e.g. console application or background service.)
that makes use of DiskQueue or ModernDiskQueue, you will likely encounter errors using the
default serialization strategy. Not only does the default serializer rely on reflection,
but certain changes in .NET 8 have broken the ability for the default serializer to deserialize
some types, like `DateTimeOffset`.

### The Problem
In .NET 8, the `System.Runtime.Serialization` code paths now use internal types like `DateTimeOffsetAdapter`
when serializing/deserializing values. However, some of these internal types, specifically the one
handling `DateTimeOffset` values, are missing some setters for values like `OffsetMinutes` — 
leading to this deserialization error.

### A Workaround
To work around this issue, it's recommended that you implement your own `ISerializationStrategy` using
`System.Text.Json`, as discussed in the ModernDiskQueue README.

### Incorporating Behavior into Testing
Reproducing (rather than simulating) this behavior in a test project is somewhat complex, and I'm
sure there are simpler ways to do it than what's presently in the project. If you would like to help
with this, by all means submit a PR or open an issue with a proposed solution.

Part of the complexity is that even if a project has the self-contained and trimmed flags set in 
the `csproj` file, running the program in BUILD or RELEASE mode does not reproduce the errors.
Another issue is that the program must be using a non-trimmed version of the ModerDiskQueue library,
so a project reference to ModernDiskQueue cannot be used (a project reference will result in MDQ being
trimmed as well). Finally, the program needs to be reliably accessible to the test project's test methods.

The current approach uses a project named `TestTrimmedExecutable` to reproduce the errors in deserializing
DateTimeOffset values. This project gets published by the `ModernDiskQueue.Tests` project build process
with the following action defined in its csproj file:
```xml
  <Target Name="PublishTrimmedExecutable" AfterTargets="Build">
    <Exec Command="dotnet publish $(SolutionDir)TestTrimmedExecutable\TestTrimmedExecutable.csproj -c Release -p:PublishTrimmed=true -o $(ProjectDir)bin\$(Configuration)\net10.0\TrimmedHost" />
  </Target>
```

This publishing task happens after the build cycle of the test project so that the 
`ModernDiskQueue.dll` file is guaranteed to exist when the `TestTrimmedExecutable` project is published.
The output path is set to a subdirectory of the test project's bin directory, ensuring that the trimmed
executable is available for the test runner.

Currently, two tests exist using this approach - one to test the deserialization of simple types
like `int`, which will pass if the output (dequeued) value matches the input (enqueued) value. The
second test will enqueue a more complex type, a property of which is of type `DateTimeOffset` and
then attempt to dequeue the object. This test will currently pass if the dequeue operation throws
an exception.

In time, more tests should be produced to cover the use of a custom `ISerializationStrategy` to avoid
the issue, and to support any future efforts to migrate away from default XML serialization strategy.

