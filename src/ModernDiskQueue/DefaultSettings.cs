namespace ModernDiskQueue
{
    public partial class PersistentQueue
    {
        /// <summary>
        /// Static settings that affect all queue instances created in this process
        /// </summary>
        public static class DefaultSettings
        {
            /// <summary>
            /// Initial setting: false
            /// <p>This setting allows sharing of the queue file across multiple processes and users. You
            /// will probably want to set this to <c>true</c> if you are synchronising across containers
            /// or are using network storage.</p>
            /// <p>If true, files that are created will be given read/write access for all users</p>
            /// <p>If false, files that are created will be left at default permissions of the running process</p>
            /// </summary>
            public static bool SetFilePermissions { get; set; } = false;

            /// <summary>
            /// Initial setting: false
            /// <para>Setting this to true will prevent some file-system level errors from stopping the queue.</para>
            /// <para>Only use this if uptime is more important than correctness of data</para>
            /// </summary>
            public static bool AllowTruncatedEntries { get; set; }

            /// <summary>
            /// Initial setting: true
            /// <para>Safe, available for tests and performance.</para>
            /// <para>If true, trim and flush waiting transactions on dispose</para>
            /// </summary>
            public static bool TrimTransactionLogOnDispose { get; set; } = true;

            /// <summary>
            /// Initial setting: true
            /// <para>Setting this to false may cause unexpected data loss in some failure conditions.</para>
            /// <para>If true, each transaction commit will flush the transaction log.</para>
            /// <para>This is slow, but ensures the log is correct per transaction in the event of a hard termination (i.e. power failure)</para>
            /// </summary>
            public static bool ParanoidFlushing { get; set; } = true;

            /// <summary>
            /// Initial setting: 10000 (10 sec).
            /// Maximum time for IO operations (including read &amp; write) to complete.
            /// If any individual operation takes longer than this, an exception will occur.
            /// </summary>
            public static int FileTimeoutMilliseconds { get; set; } = 10_000;
        }
    }
}