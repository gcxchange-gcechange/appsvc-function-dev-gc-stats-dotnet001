using CsvHelper.Configuration.Attributes;

public class TeamActivityRecord
{
    [Name("Report Refresh Date")]
    public string ReportRefreshDate { get; set; } = "";

    [Name("Team Name")]
    public string TeamName { get; set; } = "";

    [Name("Team Id")]
    public string TeamId { get; set; } = "";

    [Name("Team Type")]
    public string TeamType { get; set; } = "";

    [Name("Last Activity Date")]
    public DateTime? LastActivityDate { get; set; }

    [Name("Active Users")]
    public string ActiveUsers { get; set; } = "";

    [Name("Active Channels")]
    public string ActiveChannels { get; set; } = "";

    [Name("Guests")]
    public string Guests { get; set; } = "";

    [Name("Reactions")]
    public string Reactions { get; set; } = "";

    [Name("Meetings Organized")]
    public string MeetingsOrganized { get; set; } = "";

    [Name("Post Messages")]
    public string PostMessages { get; set; } = "";

    [Name("Reply Messages")]
    public string ReplyMessages { get; set; } = "";

    [Name("Channel Messages")]
    public string ChannelMessages { get; set; } = "";

    [Name("Urgent Messages")]
    public string UrgentMessages { get; set; } = "";

    [Name("Mentions")]
    public string Mentions { get; set; } = "";

    [Name("Active Shared Channels")]
    public string ActiveSharedChannels { get; set; } = "";

    [Name("Active External Users")]
    public string ActiveExternalUsers { get; set; } = "";
}