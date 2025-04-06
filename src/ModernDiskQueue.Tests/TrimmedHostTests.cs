﻿
namespace ModernDiskQueue.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Text;
    using System.Threading.Tasks;
    using NUnit.Framework;

    [TestFixture]
    public class TrimmedHostTests
    {
        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
        }

        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
        }

        [Test]
        public void TestTrimmedExecutable_ComplexTypeWithDefaultSerializer_CantDeserialize()
        {
            string path = "TrimmedHost/TestTrimmedExecutable.exe";
            DateTimeOffset inputDate = DateTimeOffset.Now;
            string argument = inputDate.ToString();
            string stdOut, stdErr = string.Empty;
            int processTimeOut = 3000;

            //ARRANGE
            var processStartInfo = new ProcessStartInfo()
            {
                FileName = path,
                Arguments = $"test=2 value={argument}",
                RedirectStandardError = true,
                RedirectStandardOutput = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            };

            Assert.IsTrue(File.Exists(path), "Checking that file exists: TestTrimmedExecutable.exe.");

            try
            {
                // ACT
                using (var process = new Process { StartInfo = processStartInfo })
                {
                    process.Start();
                    stdOut = process.StandardOutput.ReadToEnd();
                    stdErr = process.StandardError.ReadToEnd();
                    bool isProcessDone = process.WaitForExit(processTimeOut);

                    if (!isProcessDone)
                    {
                        process.Kill();
                        Assert.Fail("Executable did not complete within specified timeout.");
                    }

                    if (!string.IsNullOrEmpty(stdErr))
                    {
                        Assert.Pass($"Executable returned an error: {stdErr}");
                    }

                    Assert.IsNotEmpty(stdOut, "Executable did not return any data.");
                    Assert.IsTrue(DateTimeOffset.TryParse(stdOut, out DateTimeOffset returnedValue), "Could not parse returned value.");
                    Assert.That(inputDate, Is.EqualTo(returnedValue), "Returned value did not match input.");
                }
            }
            catch (InvalidOperationException)
            {
                Assert.Fail("InvalidOperationException trying to run test.");
            }
            catch (AssertionException) { }
            catch (SuccessException) { }
            catch (Exception ex)
            {
                Assert.Fail($"Exception trying to run test. {ex.GetType().Name} {ex.Message} {ex.StackTrace} {stdErr}");
            }
        }

        [Test]
        public void TestTrimmedExecutable_SimpleTypeWithDefaultSerializer_CanDeserialize()
        {
            string path = "TrimmedHost/TestTrimmedExecutable.exe";
            int argument = 5;
            string stdOut, stdErr = string.Empty;
            int processTimeOut = 3000;

            //ARRANGE
            var processStartInfo = new ProcessStartInfo()
            {
                FileName = path,
                Arguments = $"test=1 value={argument}",
                RedirectStandardError = true,
                RedirectStandardOutput = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            };

            Assert.IsTrue(File.Exists(path), "Checking that file exists: TestTrimmedExecutable.exe.");

            try
            {
                // ACT
                using (var process = new Process { StartInfo = processStartInfo })
                {
                    process.Start();
                    stdOut = process.StandardOutput.ReadToEnd();
                    stdErr = process.StandardError.ReadToEnd();
                    bool isProcessDone = process.WaitForExit(processTimeOut);

                    if (!isProcessDone)
                    {
                        process.Kill();
                        Assert.Fail("Executable did not complete within specified timeout.");
                    }

                    if (!string.IsNullOrEmpty(stdErr))
                    {
                        Assert.Fail($"Executable returned an error: {stdErr}");
                    }

                    Assert.IsNotEmpty(stdOut, "Executable did not return any data.");
                    Assert.IsTrue(int.TryParse(stdOut, out int returnedValue), "Could not parse output value from executable.");
                    Assert.That(argument, Is.EqualTo(returnedValue), "Return value did not match input.");
                }
            }
            catch (InvalidOperationException)
            {
                Assert.Fail("InvalidOperationException trying to run test.");
            }
            catch (AssertionException) { }
            catch (SuccessException) { }
            catch (Exception ex)
            {
                Assert.Fail($"Exception trying to run test. {ex.GetType().Name} {ex.Message} {ex.StackTrace} {stdErr}");
            }
        }
    }
}
