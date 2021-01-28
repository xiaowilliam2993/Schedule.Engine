using System;
using System.Collections.Generic;
using System.Linq;
using Dispatcher.Models;
using Dispatcher.Services;
using Hangfire;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace Dispatcher.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class DispatchController : ControllerBase
    {
        private readonly ILogger<DispatchController> _logger;
        private readonly IEnumerable<Tenant> _tenants;

        public DispatchController(ILogger<DispatchController> logger, IEnumerable<Tenant> tenants)
        {
            _logger = logger;
            _tenants = tenants;
        }
        [HttpGet]
        public string Get()
        {
            return JsonConvert.SerializeObject(_tenants, Formatting.Indented);
        }
        /// <summary>
        /// 用于创建关联型复合数据源保存时生成排成任务产生工作表数据
        /// </summary>
        /// <param name="dataSourceId">工作表ID</param>
        /// <returns></returns>
        [HttpPut]
        [Route("task/{dataSourceId}")]
        public ActionResult Put(Guid dataSourceId, [FromQuery] string masterData, [FromQuery] string url)
        {
            _logger.LogInformation($"put dispatch task, params: dataSourceId={dataSourceId}, masterData={masterData}, applicationUrl={url}");

            try
            {
                var tenant = _tenants.FirstOrDefault(_ => string.Equals(_.MasterData, masterData, StringComparison.OrdinalIgnoreCase) || string.Equals(_.ApplicationUrl, url, StringComparison.OrdinalIgnoreCase));
                if (tenant == null)
                {
                    throw new KeyNotFoundException(masterData);
                }

                BackgroundJob.Enqueue<IEngineService>(services => services.UpdateTask(tenant, dataSourceId, UpdateMode.FromApi));

                _logger.LogInformation("调度任务生成成功");

                return Ok();
            } 
            catch (Exception ex)
            {
                _logger.LogError(ex, "生成调度任务发生错误");
                return BadRequest(ex.Message);
            }
        }
    }
}