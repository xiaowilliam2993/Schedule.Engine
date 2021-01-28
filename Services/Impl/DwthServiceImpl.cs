using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using Dispatcher.Models;
using System.Diagnostics;

namespace Dispatcher.Services.Impl
{
    public class DwthServiceImpl : IDwthService
    {
        private readonly ILogger<DwthServiceImpl> _logger;

        public DwthServiceImpl(ILogger<DwthServiceImpl> logger)
        {
            _logger = logger;
        }

        /// <summary>
        /// 获取爬虫系统数据
        /// </summary>
        /// <param name="crawler">爬蟲系統配置</param>
        /// <param name="userToken">用户秘钥</param>
        /// <param name="service">
        /// 服务名称
        ///     1、IndustryInfo，获取行业清单
        ///     2、PeerInfo，获得行业公司清单
        ///     3、IndustryIndVal，获得行业财务比率
        ///     4、PeerIndVal，获得公司财务比率
        ///     5、Fin5p，获得财务比率资料
        ///     6、Fin5pSetting，取得财务比率设定资讯
        ///     7、Fin5pVar，取得财务比率变数资讯
        ///     8、FinCategory，取得财务类别资讯
        ///     9、News，取得新闻资讯
        ///     10、CurrencyInfo，取得币别清单
        ///     11、ExchangeRate，获取汇率资讯
        ///     12、MaterialInfo，取得原物料清单
        ///     13、MaterialPrice，取得原物料行情
        /// </param>
        /// <param name="info">
        /// url带参，json格式字符串
        ///     1、IndustryInfo：{"area":"2","industry_id":""}
        ///         area：区域
        ///         industry_id：行业ID，空白表全部
        ///     2、PeerInfo：{"area":"2","industry_id":"","comp_id":""}
        ///     3、IndustryIndVal：{"area":"2","industry_id":"M74","indicator_id":[],"type":"2","yyyymm_s":"201901","yyyymm_e":"201903"}
        ///     4、PeerIndVal：{"area":"2","comp_id":"002469","indicator_id":[],"type":"2","yyyymm_s":"201901","yyyymm_e":"201903"}
        ///     5、Fin5p：{"area":"2","indicator_id":[]}
        ///     6、Fin5pSetting：{"area":"2"}
        ///     7、Fin5pVar：{"area":"2"}
        ///     8、FinCategory：{"area":"2"}
        ///     9、News：{"id":"002469","start_date":"20200401","end_date":"20200430"}
        ///     10、CurrencyInfo：{"area":"2"}
        ///     11、ExchangeRate：{"currencyIdList":["EUR","USD"],"base_currency_id":"CNY","start_date":"20200401","end_date":"20200407"}
        ///     12、MaterialInfo：空字符串
        ///     13、MaterialPrice：{"materialIdList":["CA"],"start_date":"20200401","end_date":"20200407"}
        /// </param>
        /// <returns></returns>
        private DwthResponseModel GetDwthInfo(Crawler crawler, string service, string info, string userToken)
        {
            if (string.IsNullOrWhiteSpace(service))
            {
                throw new ArgumentException("service不可为空白");
            }

            if (string.IsNullOrWhiteSpace(userToken))
            {
                userToken = crawler.UserToken;
            }

            Stopwatch stopwatch = new Stopwatch();
            string url = $"{crawler.ApiHost}/restful/service/DWth/{service}";
            try
            {
                stopwatch.Start();
                if (!string.IsNullOrWhiteSpace(info))
                {
                    url += $"?info={info}";
                }
                using HttpClient httpClient = new HttpClient
                {
                    BaseAddress = new Uri(url)
                };
                httpClient.DefaultRequestHeaders.Add("token", userToken);
                var result = httpClient.GetAsync(string.Empty).Result.Content.ReadAsStringAsync().Result;
                return JsonConvert.DeserializeObject<DwthResponseModel>(result);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"获取行业对标数据发生错误，url={url}");
                throw ex;
            }
            finally
            {
                stopwatch.Stop();
                _logger.LogInformation($"url={url}, timewatchUsed={stopwatch.Elapsed.TotalMilliseconds} sec.");
            }
        }
        /// <summary>
        /// 获取行业财务比率
        /// </summary>
        /// <param name="crawler"></param>
        /// <param name="area"></param>
        /// <param name="industry_id"></param>
        /// <param name="indicator_id"></param>
        /// <param name="type"></param>
        /// <param name="yyyymm_s"></param>
        /// <param name="yyyymm_e"></param>
        /// <param name="userToken"></param>
        /// <returns></returns>
        public DwthResponseModel GetIndustryIndVal(Crawler crawler, string area, string industry_id, IEnumerable<string> indicator_id, string type, string yyyymm_s, string yyyymm_e, string userToken = "")
        {
            var dwthResponse = GetDwthInfo(crawler, "IndustryIndVal", JsonConvert.SerializeObject(new
            {
                area,
                industry_id,
                indicator_id,
                type,
                yyyymm_s,
                yyyymm_e
            }), userToken);
            dwthResponse.Response.Data = JsonConvert.DeserializeObject<IEnumerable<IndustryIndVal>>(dwthResponse.Response.Data.ToString())
                .Select(_ => JsonConvert.SerializeObject(_)).Distinct()
                .Select(_ => JsonConvert.DeserializeObject<IndustryIndVal>(_));
            return dwthResponse;
        }

        /// <summary>
        /// 获取公司财务比率
        /// </summary>
        /// <param name="crawler"></param>
        /// <param name="area"></param>
        /// <param name="comp_id"></param>
        /// <param name="indicator_id"></param>
        /// <param name="type"></param>
        /// <param name="yyyymm_s"></param>
        /// <param name="yyyymm_e"></param>
        /// <param name="userToken"></param>
        /// <returns></returns>
        public DwthResponseModel GetPeerIndVal(Crawler crawler, string area, string comp_id, IEnumerable<string> indicator_id, string type, string yyyymm_s, string yyyymm_e, string userToken = "")
        {
            var dwthResponse = GetDwthInfo(crawler, "PeerIndVal", JsonConvert.SerializeObject(new
            {
                area,
                comp_id,
                indicator_id,
                type,
                yyyymm_s,
                yyyymm_e
            }), userToken);
            dwthResponse.Response.Data = JsonConvert.DeserializeObject<IEnumerable<PeerIndVal>>(dwthResponse.Response.Data.ToString());
            return dwthResponse;
        }

        /// <summary>
        /// 获取财务比率资料
        /// </summary>
        /// <param name="crawler"></param>
        /// <param name="area"></param>
        /// <param name="indicator_id"></param>
        /// <param name="userToken"></param>
        /// <returns></returns>
        public DwthResponseModel GetFin5p(Crawler crawler, string area, IEnumerable<string> indicator_id, string userToken = "")
        {
            return GetDwthInfo(crawler, "Fin5p", JsonConvert.SerializeObject(new
            {
                area,
                indicator_id
            }), userToken);
        }
    }
}
