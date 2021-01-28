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
    public class CrawlerController : ControllerBase
    {
        private readonly ILogger<CrawlerController> _logger;
        private readonly IEnumerable<Tenant> _tenants;
        private readonly IEnumerable<Crawler> _crawlers;
        private readonly IDwthService _dwthService;
        private readonly ICrawlerService _crawlerService;

        public CrawlerController(ILogger<CrawlerController> logger, IEnumerable<Tenant> tenants, IEnumerable<Crawler> crawlers, IDwthService dwthService, ICrawlerService crawlerService)
        {
            _logger = logger;
            _tenants = tenants;
            _crawlers = crawlers;
            _dwthService = dwthService;
            _crawlerService = crawlerService;
        }

        [HttpGet]
        [Route("industryindval/{area}/{industry_id}/{ind_id}/{yyyymm_s}/yyyymm_e")]
        public object GetIndustryIndVal(string area, string industry_id, string ind_id, string yyyymm_s, string yyyymm_e)
        {
            var crawler = _crawlers.Single(_ => _.Area == (FinanceArea)Enum.Parse(typeof(FinanceArea), area));
            return JsonConvert.SerializeObject(_dwthService.GetIndustryIndVal(crawler, area, industry_id, new[] { ind_id }, "1", yyyymm_s, yyyymm_e), Formatting.Indented);
        }

        [HttpGet]
        [Route("fin5p/{area}")]
        public object GetFin5p(string area)
        {
            var crawler = _crawlers.Single(_ => _.Area == (FinanceArea)Enum.Parse(typeof(FinanceArea), area));
            return JsonConvert.SerializeObject(_dwthService.GetFin5p(crawler, area, Enumerable.Empty<string>()), Formatting.Indented);
        }

        [HttpPut]
        [Route("pullindustryindval/{dataModelingId}")]
        public ActionResult PullInustryIndVal(Guid dataModelingId, [FromQuery] string masterData)
        {
            _logger.LogInformation($"put crawler task, params: dataModelingId={dataModelingId}, masterdata={masterData}");

            try
            {
                var tenant = _tenants.FirstOrDefault(_ => string.Equals(_.MasterData, masterData, StringComparison.OrdinalIgnoreCase));
                if (tenant == null)
                {
                    throw new KeyNotFoundException($"調度引擎未找到匹配的租戶信息，tenantName={masterData}，請聯繫管理員處理。");
                }
                return Ok(_crawlerService.ImportFinanceialReport(dataModelingId, tenant, CrawlerAction.UpdateForModeling));
            }
            catch (Exception ex)
            {
                string errorMessage = "拉取行業財務比率發生錯誤";
                _logger.LogError(ex, errorMessage);
                return BadRequest($"{errorMessage}：{ex.Message}");
            }
        }

        [HttpPut]
        [Route("pullindustryindval-sysparamschanged")]
        public ActionResult PullIndustryIndVal([FromQuery] string masterData)
        {
            _logger.LogInformation($"Sys parameters has changed, put crawler task, params: masterdata={masterData}");

            try
            {
                var tenant = _tenants.FirstOrDefault(_ => string.Equals(_.MasterData, masterData, StringComparison.OrdinalIgnoreCase));
                if (tenant == null)
                {
                    throw new KeyNotFoundException($"調度引擎未找到匹配的租戶信息，tenantName={masterData}，請聯繫管理員處理。");
                }

                BackgroundJob.Enqueue<ICrawlerService>(services => services.ImportFinanceialReport(tenant));

                return Ok();
            }
            catch (Exception ex)
            {
                string errorMessage = "拉取行業財務比率發生錯誤（觸發時機：系統設定發生變更）";
                _logger.LogError(ex, errorMessage);
                return BadRequest($"{errorMessage}：{ex.Message}");
            }
        }
    }
}
