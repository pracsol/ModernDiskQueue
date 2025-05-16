# TestTrimmedExecutable
This project is used by ModernDiskQueue.Tests to recreate behavior when the ModernDiskQueue library
is used by an executable published with trimming enabled.

The following settings in the `csproj` file dictate this publish profile.
```xml
    <PublishSingleFile>true</PublishSingleFile>
    <PublishTrimmed>true</PublishTrimmed>
    <SelfContained>true</SelfContained>
    <TrimMode>full</TrimMode>
```

# How To Use
This executable is used by supplying command-line arguments to control its behavior.

```cmd
NAME
    TestTrimmedExecutable - A utility program for use by ModernDiskQueue.Tests.

SYNTAX
    TestTrimmedExecutable test=<1|2|3|4> value=<int|DateTimeOffset> [writeoutput=true|false]

DESCRIPTION
    TestTrimmedExecutable tests the behavior of the ModernDiskQueue library by 
    a commandline program published with trimming enabled. It enqueues and dequeues 
    objects to test the behavior of serialization. The expectation is that standard
    output will be the same as the value you provided.

    Options:
        --test <1|2|3|4>
            Specifies the test scenario to run. Must be either 1 or 2.
            1: Using sync API, enqueues an integer value and dequeues it, returning the value you provided.
            2: Using sync API, enqueues a more complex object, including a DateTimeOffset value, and
            dequeues it, returning the value you provided.
            3: Using async API, enqueues an integer value and dequeues it, returning the value you provided.
            4: Using async API, enqueues a more complex object, including a DateTimeOffset value, and
            dequeues it, returning the value you provided.

        -- value <int|DateTimeOffset>
            Provides the value to use for the test. These are string representations of
            either an integer or a DateTimeOffset value.
            - int: a value that can be parsed as a 32-bit integer (e.g. 42).
            - DateTimeOffset: a date-time with an offset (e.g. 2025-04-01T12:00:00+00:00).

        -- writeoutput <true|false>
            Console logging is disabled by default since it will pollute the test output, 
            but for debugging purposes it can be overridden with this argument. Setting the 
            value to false is essentially a no-op.

EXAMPLES
    ./TestTrimmedExecutable.exe test=1 value=42
        Runs test scenario 1 with the integer value of 42.

    ./TestTrimmedExecutable.exe test=2 value=2025-04-01T12:00:00+00:00

REMARKS
    The return value (stdout) will be the value you submitted if everything is working correctly. 
    The program will throw an exception and return an exit code > 0 if an error is encountered.
    Exception text is written to stderr.
```

