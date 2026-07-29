namespace GCStats.Models
{
    class User(Microsoft.Graph.Models.User user)
    {
        string Id { get; set; } = user.Id;
        string Mail { get; set; } = user.Mail;
    }
}