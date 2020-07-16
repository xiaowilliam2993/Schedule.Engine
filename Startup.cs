using System;
using System.Collections.Generic;
using System.IO;
using Dispatcher.Filters;
using Dispatcher.Models;
using Dispatcher.Services;
using Dispatcher.Services.Impl;
using Hangfire;
using Hangfire.Storage.SQLite;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.OpenApi.Models;
using Newtonsoft.Json.Converters;

namespace Dispatcher
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        [Obsolete]
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddControllersWithViews().AddNewtonsoftJson(options =>
            {
                options.SerializerSettings.Converters.Add(new StringEnumConverter { CamelCaseText = true });
            });
            services.AddSingleton(provider => provider.GetService<IConfiguration>().GetSection("Tenants").Get<IEnumerable<Tenant>>());
            services.AddSwaggerGen(c => c.SwaggerDoc("v1", new OpenApiInfo() { Title = "Dispatcher api", Version = "v1" }));
            services.AddHealthChecks();

            services.AddHangfire(config => 
            {
                config.UseSerilogLogProvider();
                config.UseSQLiteStorage("Repository/Hangfire.db"); 
            });
            services.AddScoped<IDispatchService, DispatchServiceImpl>();
            services.AddScoped<IEngineService, EngineServiceImpl>();
            services.AddScoped<IMySqlService, MySqlService>();
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }
            //app.UseHttpsRedirection();
            app.UseRouting();
            app.UseAuthorization();
            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();
            });
            app.UseEndpoints(endpoints => endpoints.MapHealthChecks("/health"));
            app.UseSwagger();
            app.UseSwaggerUI(c => { c.SwaggerEndpoint("/swagger/v1/swagger.json", "Dispatcher api v1"); });

            app.UseHangfireDashboard("/hangfire", new DashboardOptions 
            {
                Authorization = new[]
                {
                    new LocalRequestsOnlyAuthorizationFilter()
                }
            });
            app.UseHangfireServer(new BackgroundJobServerOptions
            {
                WorkerCount = int.Parse(Configuration.GetSection("Hangfire:WorkerCount").Value)
            });

            string initializeFilePath = Path.Combine(AppContext.BaseDirectory, ".initialized");
            if (!File.Exists(initializeFilePath))
            {
                RecurringJob.AddOrUpdate<IEngineService>(services => services.AutoUpdate(), Configuration.GetValue("Hangfire:AutoUpdate_CronExpression", "0 0/5 * * * ? "), TimeZoneInfo.Local);
                RecurringJob.AddOrUpdate<IDispatchService>(services => services.DeleteInvalidPhysicalTables(), Configuration.GetValue("Hangfire:DeleteInvalidPyhsicalTable_CronExpression", "0 0 2 * * ? "), TimeZoneInfo.Local);

                File.Create(initializeFilePath);
            }
        }
    }
}
