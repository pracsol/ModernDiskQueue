using ModernDiskQueue.Implementation;
using NUnit.Framework;

namespace ModernDiskQueue.Tests
{
    [TestFixture]
    internal class LockFileDataSerializationTest
    {
        [Test]
        public void ConvertToBytes_ExpectedOrderIsObtained()
        {
            var expected = new byte[]
            {
                8, 1, 0, 0,
                16, 2, 0, 0,
                32, 4, 0, 0, 0, 0, 0, 0
            };

            var data = new LockFileData
            {
                ProcessId = 8 + 256 * 1,
                ThreadId = 16 + 256 * 2,
                ProcessStart = 32 + 256 * 4
            };

            var actual = MarshallHelper.Serialize(data);
            for (var i = 0; i < expected.Length && i < actual.Length; i++)
            {
                Assert.That(expected[i], Is.EqualTo(actual[i]));
            }
        }

        [Test]
        public void ConvertFromBytes_MissingProcessStart_DeserializesCorrectly()
        {
            var input = new byte[]
            {
                8, 1, 0, 0,
                16, 2, 0, 0,
                //32, 4, 0, 0, 0, 0, 0, 0 // no ProcessStart bytes
            };

            var actual = MarshallHelper.Deserialize<LockFileData>(input);
            Assert.That(8 + 256 * 1, Is.EqualTo(actual.ProcessId));
            Assert.That(16 + 256 * 2, Is.EqualTo(actual.ThreadId));
            Assert.That(0, Is.EqualTo(actual.ProcessStart));
        }

        [Test]
        public void ConvertFromBytes_CompleteData_DeserializesCorrectly()
        {
            var input = new byte[]
            {
                8, 1, 0, 0,
                16, 2, 0, 0,
                32, 4, 0, 0, 0, 0, 0, 0
            };

            var actual = MarshallHelper.Deserialize<LockFileData>(input);
            Assert.That(8 + 256 * 1, Is.EqualTo(actual.ProcessId));
            Assert.That(16 + 256 * 2, Is.EqualTo(actual.ThreadId));
            Assert.That(32 + 256 * 4, Is.EqualTo(actual.ProcessStart));
        }
    }
}
