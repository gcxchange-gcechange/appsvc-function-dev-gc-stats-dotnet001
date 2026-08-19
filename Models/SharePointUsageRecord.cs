using CsvHelper.Configuration.Attributes;

namespace GCStats.Models
{
    public class SharePointUsageRecord
    {
        [Name("Report Refresh Date")]
        public string ReportRefreshDate { get; set; } = "";

        [Name("Site Id")]
        public string SiteId { get; set; } = "";

        [Name("Site URL")]
        public string SiteURL { get; set; } = "";

        [Name("Owner Display Name")]
        public string OwnerDisplayName { get; set; } = "";

        [Name("Is Deleted")]
        public string IsDeleted { get; set; } = "";

        [Name("Last Activity Date")]
        public DateTime? LastActivityDate { get; set; }

        [Name("File Count")]
        public string FileCount { get; set; } = "";

        [Name("Active File Count")]
        public string ActiveFileCount { get; set; } = "";

        [Name("Page View Count")]
        public string PageViewCount { get; set; } = "";

        [Name("Visited Page Count")]
        public string VisitedPageCount { get; set; } = "";

        [Name("Storage Used (Byte)")]
        public string StorageUsedByte { get; set; } = "";

        [Name("Storage Allocated (Byte)")]
        public string StorageAllocatedByte { get; set; } = "";

        [Name("Root Web Template")]
        public string RootWebTemplate { get; set; } = "";

        [Name("Owner Principal Name")]
        public string OwnerPrincipalName { get; set; } = "";

        [Name("Report Period")]
        public string ReportPeriod { get; set; } = "";
    }
}
