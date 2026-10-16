namespace GCStats.Models
{
    internal class GoogleAnalyticsEvent
    {
        required public string JobTitleEn { get; set; }
        required public string JobTitleFr { get; set; }
        required public string EventName { get; set; }
        required public DateTime EventDate { get; set; }
        required public string JobOpportunityId { get; set; }
        required public int EventCount { get; set; }
    }
}
