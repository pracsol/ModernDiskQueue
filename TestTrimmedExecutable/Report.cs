namespace TestTrimmedExecutable
{
    [Serializable]
    internal class Report
    {
        public Guid Id { get; set; } = new();
        public int ReportType { get; set; }
        public string DataField { get; set; } = string.Empty;
        public DateTimeOffset LocalTime { get; set; }
    }
}