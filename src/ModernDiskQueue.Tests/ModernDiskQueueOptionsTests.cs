namespace ModernDiskQueue.Tests;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System.Collections.Generic;
using NUnit.Framework;

[TestFixture]
public class ModernDiskQueueOptionsTests
{
    [Test]
    public void Options_BindFromConfiguration_AllPropertiesDeserializeCorrectly()
    {
        // Arrange - Create in-memory configuration simulating appsettings.json
        var configData = new Dictionary<string, string?>
        {
            ["ModernDiskQueue:ParanoidFlushing"] = "false",
            ["ModernDiskQueue:TrimTransactionLogOnDispose"] = "false",
            ["ModernDiskQueue:AllowTruncatedEntries"] = "true",
            ["ModernDiskQueue:SetFilePermissions"] = "true",
            ["ModernDiskQueue:FileTimeoutMilliseconds"] = "5000",
            ["ModernDiskQueue:ThrowOnConflict"] = "false",
        };

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(configData)
            .Build();

        // Act - Bind the configuration to the options class
        var options = new ModernDiskQueueOptions();
        configuration.GetSection("ModernDiskQueue").Bind(options);

        // Assert - Verify all properties deserialized correctly
        Assert.That(options.ParanoidFlushing, Is.False);
        Assert.That(options.TrimTransactionLogOnDispose, Is.False);
        Assert.That(options.AllowTruncatedEntries, Is.True);
        Assert.That(options.SetFilePermissions, Is.True);
        Assert.That(options.FileTimeoutMilliseconds, Is.EqualTo(5000));
        Assert.That(options.ThrowOnConflict, Is.False);
    }

    [Test]
    public void Options_BindFromConfiguration_DefaultValuesUsedWhenNotSpecified()
    {
        // Arrange - Create configuration with only some values specified
        var configData = new Dictionary<string, string?>
        {
            ["ModernDiskQueue:ParanoidFlushing"] = "false"
        };

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(configData)
            .Build();

        // Act
        var options = new ModernDiskQueueOptions();
        configuration.GetSection("ModernDiskQueue").Bind(options);

        // Assert - Specified value is bound, others retain defaults
        Assert.That(options.ParanoidFlushing, Is.False);
        Assert.That(options.TrimTransactionLogOnDispose, Is.True); // default
        Assert.That(options.AllowTruncatedEntries, Is.False); // default
        Assert.That(options.SetFilePermissions, Is.False); // default
        Assert.That(options.FileTimeoutMilliseconds, Is.EqualTo(10_000)); // default
        Assert.That(options.ThrowOnConflict, Is.True); // default
    }

    [Test]
    public void AddModernDiskQueue_WithConfiguration_OptionsResolvedFromDI()
    {
        // Arrange - Simulate loading from appsettings.json
        var configData = new Dictionary<string, string?>
        {
            ["ModernDiskQueue:ParanoidFlushing"] = "false",
            ["ModernDiskQueue:TrimTransactionLogOnDispose"] = "false",
            ["ModernDiskQueue:FileTimeoutMilliseconds"] = "20000"
        };

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(configData)
            .Build();

        var services = new ServiceCollection();

        // Act - Register options from configuration section, then add ModernDiskQueue
        services.Configure<ModernDiskQueueOptions>(
            configuration.GetSection("ModernDiskQueue"));
        services.AddModernDiskQueue();

        var serviceProvider = services.BuildServiceProvider();

        // Assert - Options should be resolvable and have correct values
        var options = serviceProvider.GetRequiredService<IOptions<ModernDiskQueueOptions>>();
        Assert.That(options.Value, Is.Not.Null);
        Assert.That(options.Value.ParanoidFlushing, Is.False);
        Assert.That(options.Value.TrimTransactionLogOnDispose, Is.False);
        Assert.That(options.Value.FileTimeoutMilliseconds, Is.EqualTo(20000));
    }

    [Test]
    public void AddModernDiskQueue_WithConfigureAction_OverridesDefaults()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act - Use the configure action overload
        services.AddModernDiskQueue(options =>
        {
            options.ParanoidFlushing = false;
            options.FileTimeoutMilliseconds = 15000;
            options.ThrowOnConflict = false;
        });

        var serviceProvider = services.BuildServiceProvider();

        // Assert
        var options = serviceProvider.GetRequiredService<IOptions<ModernDiskQueueOptions>>();
        Assert.That(options.Value.ParanoidFlushing, Is.False);
        Assert.That(options.Value.FileTimeoutMilliseconds, Is.EqualTo(15000));
        Assert.That(options.Value.ThrowOnConflict, Is.False);
    }

    [Test]
    public void AddModernDiskQueue_FactoryReceivesConfiguredOptions()
    {
        // Arrange
        var configData = new Dictionary<string, string?>
        {
            ["ModernDiskQueue:ParanoidFlushing"] = "false",
            ["ModernDiskQueue:AllowTruncatedEntries"] = "true"
        };

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(configData)
            .Build();

        var services = new ServiceCollection();
        services.Configure<ModernDiskQueueOptions>(
            configuration.GetSection("ModernDiskQueue"));
        services.AddModernDiskQueue();

        var serviceProvider = services.BuildServiceProvider();

        // Act - Resolve the factory
        var factory = serviceProvider.GetRequiredService<IPersistentQueueFactory>();

        // Assert - Factory should be created successfully
        Assert.That(factory, Is.Not.Null);
        Assert.That(factory, Is.InstanceOf<PersistentQueueFactory>());
    }

    [Test]
    public void Options_ConfigureActionTakesPrecedence_WhenBothConfigured()
    {
        // Arrange - Configuration has one value
        var configData = new Dictionary<string, string?>
        {
            ["ModernDiskQueue:FileTimeoutMilliseconds"] = "5000"
        };

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(configData)
            .Build();

        var services = new ServiceCollection();

        // Register from configuration first
        services.Configure<ModernDiskQueueOptions>(
            configuration.GetSection("ModernDiskQueue"));

        // Then override with configure action (PostConfigure pattern)
        services.PostConfigure<ModernDiskQueueOptions>(options =>
        {
            options.FileTimeoutMilliseconds = 30000;
        });

        services.AddModernDiskQueue();

        var serviceProvider = services.BuildServiceProvider();

        // Act
        var options = serviceProvider.GetRequiredService<IOptions<ModernDiskQueueOptions>>();

        // Assert - PostConfigure should take precedence
        Assert.That(options.Value.FileTimeoutMilliseconds, Is.EqualTo(30000));
    }

    [Test]
    public void Options_ToString_ReturnsExpectedFormat()
    {
        // Arrange
        var options = new ModernDiskQueueOptions
        {
            ParanoidFlushing = false,
            AllowTruncatedEntries = true,
            FileTimeoutMilliseconds = 5000
        };

        // Act
        var result = options.ToString();

        // Assert
        Assert.That(result, Does.Contain("ParanoidFlushing: False"));
        Assert.That(result, Does.Contain("AllowTruncatedEntries: True"));
        Assert.That(result, Does.Contain("FileTimeoutInMilliseconds: 5000"));
    }
}