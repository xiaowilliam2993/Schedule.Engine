using Dispatcher.Services;

namespace Dispatcher.Models
{
    public class Crawler
    {
        public FinanceArea Area { get; set; }
        public string ApiHost { get; set; }
        public string UserToken { get; set; }
    }
}
