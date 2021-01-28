using Dispatcher.Models;
using System.Collections.Generic;

namespace Dispatcher.Services
{
    public interface IDwthService
    {
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
        DwthResponseModel GetIndustryIndVal(Crawler crawler, string area, string industry_id, IEnumerable<string> indicator_id, string type, string yyyymm_s, string yyyymm_e, string userToken = "");
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
        DwthResponseModel GetPeerIndVal(Crawler crawler, string area, string comp_id, IEnumerable<string> indicator_id, string type, string yyyymm_s, string yyyymm_e, string userToken = "");
        /// <summary>
        /// 获取财务比率资料
        /// </summary>
        /// <param name="crawler"></param>
        /// <param name="area"></param>
        /// <param name="indicator_id"></param>
        /// <param name="userToken"></param>
        /// <returns></returns>
        DwthResponseModel GetFin5p(Crawler crawler, string area, IEnumerable<string> indicator_id, string userToken = "");
    }
}
