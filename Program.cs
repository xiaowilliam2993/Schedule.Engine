using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Serilog;
using System;

namespace Dispatcher
{
    public class Program
    {
        public static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        public static IWebHostBuilder CreateHostBuilder(string[] args)
        {
            var configuration = new ConfigurationBuilder().SetBasePath(AppContext.BaseDirectory).AddJsonFile("appsettings.json").Build();
            return WebHost.CreateDefaultBuilder(args)
                .ConfigureLogging((hostingContext, logging) =>
                {
                    logging.AddEventLog();
                })
                .ConfigureAppConfiguration((c, h) =>
                {
                    h.AddJsonFile("tenants.json", true, true);
                })
                .UseUrls(configuration["urls"])
                .UseStartup<Startup>()
                .UseSerilog((ctx, config) => config.ReadFrom.Configuration(ctx.Configuration));
        }
    }
}
