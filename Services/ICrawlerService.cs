using Dispatcher.Models;
using System;

namespace Dispatcher.Services
{
    /// <summary>
    /// 更新行业财务比率动作类型
    /// </summary>
    public enum CrawlerAction
    {
        None = 0,
        /// <summary>
        /// 更新单个数据模型
        /// </summary>
        UpdateForModeling = 1,
        /// <summary>
        /// 更新租户下所有的数据模型
        /// </summary>
        UpdateForTenant = 2,
    }
    /// <summary>
    /// 财务区域
    /// </summary>
    public enum FinanceArea
    {
        /// <summary>
        /// 台湾
        /// </summary>
        TW = 1,
        /// <summary>
        /// 大陆
        /// </summary>
        CN = 2,
    }
    public enum IndicatorCategory
    {
        /// <summary>
        /// 收入利润类
        /// </summary>
        RevenueAndProfiit = 1,
        /// <summary>
        /// 资金周转类
        /// </summary>
        CapitalTurnover
    }
    public interface ICrawlerService
    {
        /// <summary>
        /// 导入行业财务指标数据（周期任务专属入口）
        /// </summary>
        /// <param name="area"></param>
        void ImportFinanceialReport(FinanceArea area);
        /// <summary>
        /// 导入行业财务指标数据（用于当系统设定发生变更时）
        /// </summary>
        /// <param name="tenant"></param>
        void ImportFinanceialReport(Tenant tenant);
        /// <summary>
        /// 导入行业财务指标数据
        /// </summary>
        /// <param name="dataModelingId"></param>
        /// <param name="tenant"></param>
        /// <param name="action"></param>
        /// <param name="guidSerial"></param>
        /// <returns></returns>
        string ImportFinanceialReport(Guid dataModelingId, Tenant tenan, CrawlerAction action, string guidSerial = "");
    }
}
