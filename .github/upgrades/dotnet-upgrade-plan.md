# .NET 10 Upgrade Plan

## Execution Steps

Execute steps below sequentially one by one in the order they are listed.

1. Validate that a .NET 10 SDK required for this upgrade is installed on the machine and if not, help to get it installed.
2. Ensure that the SDK version specified in global.json files is compatible with the .NET 10 upgrade.
3. Upgrade src\ModernDiskQueue\ModernDiskQueue.csproj
4. Upgrade TestDummyProcess\TestDummyProcess.csproj
5. Upgrade src\ModernDiskQueue.Benchmarks\ModernDiskQueue.Benchmarks.csproj
6. Upgrade TestTrimmedExecutable\TestTrimmedExecutable.csproj
7. Upgrade src\ModernDiskQueue.Tests\ModernDiskQueue.Tests.csproj
8. Run unit tests to validate upgrade in the project: src\ModernDiskQueue.Tests\ModernDiskQueue.Tests.csproj

## Settings

This section contains settings and data used by execution steps.

### Aggregate NuGet packages modifications across all projects

NuGet packages used across all selected projects or their dependencies that need version update in projects that reference them.

| Package Name                                    | Current Version | New Version | Description                                           |
|:------------------------------------------------|:---------------:|:-----------:|:------------------------------------------------------|
| Microsoft.Extensions.DependencyInjection        | 8.0.1           | 10.0.1      | Recommended for .NET 10                               |
| Microsoft.Extensions.Logging                    | 8.0.1; 9.0.4    | 10.0.1      | Recommended for .NET 10                               |
| Microsoft.Extensions.Logging.Abstractions       | 8.0.2           | 10.0.1      | Recommended for .NET 10                               |
| Microsoft.Extensions.Logging.Console            | 8.0.1; 9.0.4    | 10.0.1      | Recommended for .NET 10                               |
| Microsoft.Extensions.Logging.Debug              | 9.0.4           | 10.0.1      | Recommended for .NET 10                               |
| Microsoft.Extensions.Options                    | 8.0.2           | 10.0.1      | Recommended for .NET 10                               |
| System.Text.RegularExpressions                  | 4.3.1           |             | Package functionality included with framework         |

### Project upgrade details

This section contains details about each project upgrade and modifications that need to be done in the project.

#### src\ModernDiskQueue\ModernDiskQueue.csproj modifications

Project properties changes:
  - Target framework should be changed from `net8.0` to `net10.0`

NuGet packages changes:
  - Microsoft.Extensions.Logging should be updated from `8.0.1` to `10.0.1` (*recommended for .NET 10*)

#### TestDummyProcess\TestDummyProcess.csproj modifications

Project properties changes:
  - Target framework should be changed from `net8.0` to `net10.0`

#### src\ModernDiskQueue.Benchmarks\ModernDiskQueue.Benchmarks.csproj modifications

Project properties changes:
  - Target framework should be changed from `net8.0` to `net10.0`

NuGet packages changes:
  - Microsoft.Extensions.Logging should be updated from `8.0.1` to `10.0.1` (*recommended for .NET 10*)
  - Microsoft.Extensions.Logging.Console should be updated from `8.0.1` to `10.0.1` (*recommended for .NET 10*)

#### TestTrimmedExecutable\TestTrimmedExecutable.csproj modifications

Project properties changes:
  - Target framework should be changed from `net8.0` to `net10.0`

NuGet packages changes:
  - Microsoft.Extensions.Options should be updated from `8.0.2` to `10.0.1` (*recommended for .NET 10*)
  - Microsoft.Extensions.DependencyInjection should be updated from `8.0.1` to `10.0.1` (*recommended for .NET 10*)
  - Microsoft.Extensions.Logging.Abstractions should be updated from `8.0.2` to `10.0.1` (*recommended for .NET 10*)
  - Microsoft.Extensions.Logging should be updated from `8.0.1` to `10.0.1` (*recommended for .NET 10*)

#### src\ModernDiskQueue.Tests\ModernDiskQueue.Tests.csproj modifications

Project properties changes:
  - Target framework should be changed from `net8.0` to `net10.0`

NuGet packages changes:
  - Microsoft.Extensions.Logging should be updated from `9.0.4` to `10.0.1` (*recommended for .NET 10*)
  - Microsoft.Extensions.Logging.Console should be updated from `9.0.4` to `10.0.1` (*recommended for .NET 10*)
  - Microsoft.Extensions.Logging.Debug should be updated from `9.0.4` to `10.0.1` (*recommended for .NET 10*)
  - System.Text.RegularExpressions should be removed from `4.3.1` (*package functionality included with framework*)