using Hangfire.Dashboard;

namespace Dispatcher.Filters
{
    public class LocalRequestsOnlyAuthorizationFilter : IDashboardAuthorizationFilter
    {
        public bool Authorize(DashboardContext context)
        {
            return !string.IsNullOrEmpty(context.Request.RemoteIpAddress) && (context.Request.RemoteIpAddress == "127.0.0.1" || context.Request.RemoteIpAddress == "::1" || context.Request.RemoteIpAddress == context.Request.LocalIpAddress);
        }
    }
}
