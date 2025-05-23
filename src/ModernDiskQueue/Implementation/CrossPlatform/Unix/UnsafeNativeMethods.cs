﻿namespace ModernDiskQueue.Implementation.CrossPlatform.Unix
{
    using System.Runtime.InteropServices;
    /// <summary>
    /// Unix calls
    /// </summary>
    public static class UnsafeNativeMethods
    {
        [DllImport("libc", EntryPoint = "chmod", SetLastError = true,
            CharSet = CharSet.Ansi, BestFitMapping = false, ThrowOnUnmappableChar = true)]
        private static extern int sys_chmod(string path, uint mode);

        /// <summary>
        /// "Change Mode" -- sets file permissions in Linux / Unix.
        /// </summary>
        /// <param name="path">Path to set</param>
        /// <param name="mode">Permissions mode flags</param>
        /// <returns>System result status</returns>
        public static int Chmod(string path, UnixFilePermissions mode)
        {
            return sys_chmod(path, (uint)mode);
        }
    }
}