using Microsoft.Extensions.Logging;
using Microsoft.Graph;
using Microsoft.Graph.Models;

namespace GCStats
{
    static class Users
    {

        public static async Task<IEnumerable<Models.User>> GetUsers(ILogger _logger)
        {
            var allUsers = new List<Models.User>();
            var graph = new Auth().GraphAuth(_logger);

            var usersPage = await graph.Users.GetAsync((requestConfiguration) =>
            {
                requestConfiguration.Headers.Add("ConsistencyLevel", "eventual");
                requestConfiguration.QueryParameters.Top = 999;
                requestConfiguration.QueryParameters.Select = ["id", "mail"];
            });

            var pageIterator = PageIterator<User, UserCollectionResponse>
            .CreatePageIterator(
                graph,
                usersPage!,
                user =>
                {
                    allUsers.Add(new Models.User(user));
                    return true; 
                },
                requestConfiguration =>
                {
                    requestConfiguration.Headers.Add("ConsistencyLevel", "eventual");
                    return requestConfiguration;
                });

            await pageIterator.IterateAsync();

            return allUsers;
        }
    }
}
