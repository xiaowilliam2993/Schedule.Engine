using Dispatcher.Models;
using Hangfire;
using Margin.Core.Data;
using Margin.Core.Data.Entities;
using Margin.Core.Utils;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Dispatcher.Services.Impl
{
    public class CrawlerServiceImpl : ICrawlerService
    {
        private readonly ILogger<CrawlerServiceImpl> _logger;
        private readonly IConfiguration _configuration;
        private readonly IEnumerable<Tenant> _tenants;
        private readonly IEnumerable<Crawler> _crawlers;
        private readonly IMySqlService _mySqlService;
        private readonly IDwthService _dwthService;
        private static readonly object _importLock = new object();
        private bool _isBusy;
        private readonly IList<dynamic> _demandProperties = new List<dynamic>//bindIndValPropertyName1：对应收入利润类指标；bindIndValPropertyName2：对应资金周转类指标；
        {
            new { name = "能力", dataType = "string", dbType = "varchar", size = 50, precision = 0, scale = 0, bindIndValPropertyName1 = "", bindIndValPropertyName2 = "" },
            new { name = "頂標", dataType = "number", dbType = "decimal", size = 0, precision = 24, scale = 6, bindIndValPropertyName1 = "MaxVal", bindIndValPropertyName2 = "MinVal" },
            new { name = "前標", dataType = "number", dbType = "decimal", size = 0, precision = 24, scale = 6, bindIndValPropertyName1 = "Quartile3", bindIndValPropertyName2 = "Quartile1" },
            new { name = "均標", dataType = "number", dbType = "decimal", size = 0, precision = 24, scale = 6, bindIndValPropertyName1 = "AvgVal", bindIndValPropertyName2 = "AvgVal" },
            new { name = "後標", dataType = "number", dbType = "decimal", size = 0, precision = 24, scale = 6, bindIndValPropertyName1 = "Quartile1", bindIndValPropertyName2 = "Quartile3" },
            new { name = "底標", dataType = "number", dbType = "decimal", size = 0, precision = 24, scale = 6, bindIndValPropertyName1 = "MinVal", bindIndValPropertyName2 = "MaxVal" },
            new { name = "對比公司一", dataType = "number", dbType = "decimal", size = 0, precision = 24, scale = 6, bindIndValPropertyName1 = "", bindIndValPropertyName2 = "" },
            new { name = "對比公司二", dataType = "number", dbType = "decimal", size = 0, precision = 24, scale = 6, bindIndValPropertyName1 = "", bindIndValPropertyName2 = "" }
        };

        public CrawlerServiceImpl(ILogger<CrawlerServiceImpl> logger, IConfiguration configuration, IEnumerable<Tenant> tenants, IEnumerable<Crawler> crawlers, IMySqlService mySqlService, IDwthService dwthService)
        {
            _logger = logger;
            _configuration = configuration;
            _tenants = tenants;
            _crawlers = crawlers;
            _mySqlService = mySqlService;
            _dwthService = dwthService;
        }

        public void ImportFinanceialReport(FinanceArea area)
        {
            lock (_importLock)
            {
                if (_isBusy) return;
                _isBusy = true;
            }

            _logger.LogInformation("执行导入财报数据周期任务");

            try
            {
                var crawler = _crawlers.Single(_ => _.Area == area);
                _logger.LogInformation(JsonConvert.SerializeObject(crawler));
                if (IsInvalidOfFinanceialPeriods(crawler))
                {
                    foreach (var group in _tenants.GroupBy(_ => _.ConnectionStrings.Master))
                    {
                        Tenant tenant = group.First();
                        try
                        {
                            using DataContext context = new DataContext(group.Key);
                            foreach (var modeling in context.DataModeling)
                            {
                                BackgroundJob.Enqueue(() => ImportFinanceialReport(modeling.Id, tenant, CrawlerAction.UpdateForTenant, null));
                            }
                        }
                        catch (Exception ex)
                        {
                            throw new Exception($"{ex.Message}，租户信息：{JsonConvert.SerializeObject(tenant)}");
                        }
                    }

                    _logger.LogInformation("导入财报数据已结束");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "执行导入财报数据周期任务发生错误");
            }

            _isBusy = false;
        }

        public void ImportFinanceialReport(Tenant tenant)
        {
            Guid taskId = Guid.NewGuid();
            using DataContext context = new DataContext(tenant.ConnectionStrings.Master);
            foreach (var modeling in context.DataModeling)
            {
                BackgroundJob.Enqueue(() => ImportFinanceialReport(modeling.Id, tenant, CrawlerAction.UpdateForTenant, taskId.ToString()));
            }
        }

        public string ImportFinanceialReport(Guid dataModelingId, Tenant tenant, CrawlerAction action, string guidSerial = "")
        {
            if (dataModelingId == null || dataModelingId == Guid.Empty) throw new ArgumentException("dataModelingId");
            if (tenant == null) throw new ArgumentNullException("tenant");
            var taskId = !string.IsNullOrEmpty(guidSerial) ? new Guid(guidSerial) : Guid.NewGuid();
            using DataContext context = new DataContext(tenant.ConnectionStrings.Master);
            var dataModeling = context.DataModeling.Find(dataModelingId);
            var dataSource = context.DataSource.Find(dataModeling.DsId);

            string message;
            if (dataSource != null)
            {
                RepairSchema(taskId, dataSource, tenant);
                ClearIndustryData(taskId, dataSource, tenant);
                message = ImportFinanceialReportToDataTable(taskId, context, dataModeling, dataSource, tenant);

                if (action == CrawlerAction.UpdateForTenant)
                {
                    dataSource.Hashcode = HashUtil.CreateHashcode();
                    dataSource.UpdateDate = DateTime.Now;
                    dataSource.EndDate = dataSource.UpdateDate.ToString("yyyy-MM-dd HH:mm:ss");
                }

                context.SaveChanges();
            }
            else
            {
                message = $"DataModeling.DsId ({dataModeling.DsId}) 未找到工作表記錄";
                _logger.LogWarning($"taskId={taskId}, {message}");
            }
            return message;
        }

        /// <summary>
        /// 清空已有的行业财务比率
        /// </summary>
        /// <param name="taskId"></param>
        /// <param name="dataSource"></param>
        /// <param name="tenant"></param>
        private void ClearIndustryData(Guid taskId, DataSource dataSource, Tenant tenant)
        {
            using MySqlConnection connection = new MySqlConnection(tenant.ConnectionStrings.Data);
            try
            {
                connection.Open();
                using MySqlCommand command = new MySqlCommand { Connection = connection };

                _mySqlService.ExecuteNonQuery(taskId, command, $"update `{dataSource.TableName}` set {string.Join(",", _demandProperties.Select(_ => $"`{_.name}` = null"))}");
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                if (connection != null && connection.State == ConnectionState.Open) connection.Close();
            }
        }

        /// <summary>
        /// 修复实体表结构和DataSource.Schema信息
        /// </summary>
        /// <param name="taskId"></param>
        /// <param name="dataSource"></param>
        /// <param name="tenant"></param>
        private void RepairSchema(Guid taskId, DataSource dataSource, Tenant tenant)
        {
            using MySqlConnection connection = new MySqlConnection(tenant.ConnectionStrings.Data);
            try
            {
                connection.Open();
                using MySqlCommand command = new MySqlCommand { Connection = connection };

                var jarray = JArray.Parse(dataSource.Schema);
                foreach (var property in _demandProperties)
                {
                    if (!jarray.Any(_ => _["name"].ToString() == property.name))
                    {
                        jarray.Add(new JObject(new JProperty("name", property.name), new JProperty("dataType", property.dataType), new JProperty("title", property.name), new JProperty("expression", null), new JProperty("isAmount", false)));
                    }

                    if (!_mySqlService.IsFieldExists(taskId, dataSource.TableName, property.name, command))
                    {
                        switch (property.dbType)
                        {
                            case "varchar":
                                _mySqlService.ExecuteNonQuery(taskId, command, $"alter table `{dataSource.TableName}` add `{property.name}` {property.dbType}({property.size}) null");
                                break;
                            case "decimal":
                                _mySqlService.ExecuteNonQuery(taskId, command, $"alter table `{dataSource.TableName}` add `{property.name}` {property.dbType}({property.precision},{property.scale}) null");
                                break;
                        }
                    }
                }
                dataSource.Schema = jarray.ToString(Formatting.None);
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                if (connection != null && connection.State == ConnectionState.Open) connection.Close();
            }
        }

        /// <summary>
        /// 导入行业财务指标到实体表
        /// </summary>
        /// <param name="taskId"></param>
        /// <param name="context"></param>
        /// <param name="dataModeling"></param>
        /// <param name="dataSource"></param>
        /// <param name="tenant"></param>
        private string ImportFinanceialReportToDataTable(Guid taskId, DataContext context, DataModeling dataModeling, DataSource dataSource, Tenant tenant)
        {
            string message = "";
            Action<string> SetWarningMessage = new Action<string>((_) => {
                _logger.LogWarning($"taskId={taskId}, {_}");
                message += _;
            });

            JObject jObject = JObject.Parse(dataModeling.Config);
            JArray cols = jObject["cols"] as JArray;
            var companyPropertyName = cols.FirstOrDefault(_ => _["type"].ToString() == "binding" && _["bind"].ToString().ToLower() == "company")?["name"]?.ToString();
            if (companyPropertyName == null)
            {
                message = $"模板（{dataModeling.Name}）未設置綁定公司的字段";
                _logger.LogError($"taskId={taskId}, {message}");
                return message;
            }
            var periodPropertyName = cols.FirstOrDefault(_ => _["type"].ToString() == "binding" && _["bind"].ToString().ToLower() == "period")?["name"]?.ToString();
            if (periodPropertyName == null)
            {
                message = $"模板（{dataModeling.Name}）未設置綁定期別的字段";
                _logger.LogError($"taskId={taskId}, {message}");
                return message;
            }
            if (!cols.Any(_ => _["name"].ToString() == "爬蟲編號") || !cols.Any(_ => _["name"].ToString() == "指標公式") || !cols.Any(_ => _["name"].ToString() == "指標類型"))
            {
                message = $"模板（{dataModeling.Name}）未設置名稱為 '爬蟲編號'、'公式'、'指標類型' 的字段";
                _logger.LogError($"taskId={taskId}, {message}");
                return message;
            }

            using MySqlConnection connection = new MySqlConnection(tenant.ConnectionStrings.Data);
            try
            {
                connection.Open();
                using MySqlCommand command = new MySqlCommand { Connection = connection };

                var data = _mySqlService.ExecuteWithAdapter(taskId, connection, dataSource.Content);

                IList<dynamic> industryCategories = new List<dynamic>();//財報數據按公司所屬行業進行歸類
                foreach (var group in data.Rows.Cast<DataRow>().GroupBy(_ => _[companyPropertyName].ToString()))
                {
                    var companyInfo = context.CompanyInfo.SingleOrDefault(_ => _.CompanyNo == group.Key);//目前只有台湾地区部署了正式的爬虫系统，现阶段只需抓财务区域为台湾地区公司的行业财务比率
                    if (companyInfo == null)
                    {
                        SetWarningMessage($"未找到匹配的系統參數設定>所屬行業及對比公司設定，公司編號：{group.Key}\n");
                        continue;
                    }
                    
                    var crawler = _crawlers.FirstOrDefault(_ => _.Area == (FinanceArea)Enum.ToObject(typeof(FinanceArea), Convert.ToInt32(companyInfo.Area)));
                    if (crawler == null)
                    {
                        SetWarningMessage($"未找到匹配的系統參數設定>所屬行業及對比公司設定，公司編號：{group.Key}，目標財務區域：{companyInfo.AreaName}({companyInfo.Area})\n");
                        continue;
                    }
                    else if (string.IsNullOrEmpty(crawler.ApiHost))
                    {
                        SetWarningMessage($"目標財務區域：{companyInfo.AreaName}({companyInfo.Area})未設置爬蟲API地址\n");
                        continue;
                    }

                    if (string.IsNullOrWhiteSpace(companyInfo.Industry))
                    {
                        SetWarningMessage($"初始化行業財務比率發生錯誤，系統參數公司（{companyInfo.CompanyName}）未設置對比行業\n");
                        continue;
                    }
                    var groupItems = group.AsEnumerable().Where(_ => string.IsNullOrWhiteSpace(_["能力"].ToString()) && !string.IsNullOrWhiteSpace(_["爬蟲編號"].ToString()));//过滤已经写入行业财务指标数据的行
                    industryCategories.Add(new { crawler, industryNo = companyInfo.Industry, industryName = companyInfo.IndustryName, companyNo = group.Key, companyInfo, dataRows = groupItems });
                }
                foreach (var industryGroup in industryCategories.GroupBy(_ => _.industryNo))
                {
                    var industry = industryGroup.First();
                    IEnumerable<DataRow> groupDataRows = Enumerable.Empty<DataRow>();
                    foreach (var companyGroup in industryGroup)
                    {
                        groupDataRows = groupDataRows.Union((IEnumerable<DataRow>)companyGroup.dataRows);
                    }
                    var financeialReportPeriods = GetFinanceialReportPeriods(groupDataRows.Min(_ => _[periodPropertyName].ToString()), groupDataRows.Max(_ => _[periodPropertyName].ToString()));
                    var crawlerCodes = groupDataRows.Select(_ => _["爬蟲編號"].ToString()).Distinct().OrderBy(_ => _);//此处 爬蟲編號 名称采用定值方式，是经过与张晓彬讨论得出的最终结果，因此data-modeling一定是存在名称为 爬蟲編號 的列（注意是繁体中文）
                    var dwthResponse = _dwthService.GetIndustryIndVal(industry.crawler, industry.companyInfo.Area, industryGroup.Key, crawlerCodes, "1", financeialReportPeriods[0].ToString("yyyyMM"), financeialReportPeriods[1].ToString("yyyyMM"));//type传定值1，表示取期（季）的数据，传2表示取年，但是目前没有取年的需求
                    var industryIndVals = (IEnumerable<IndustryIndVal>)dwthResponse.Response.Data;
                    foreach (var companyGroup in industryGroup)
                    {
                        var dataRows = (IEnumerable<DataRow>)companyGroup.dataRows;
                        var companyInfo = companyGroup.companyInfo;
                        crawlerCodes = dataRows.Select(_ => _["爬蟲編號"].ToString()).Distinct().OrderBy(_ => _);
                        financeialReportPeriods = GetFinanceialReportPeriods(dataRows.Min(_ => _[periodPropertyName].ToString()), dataRows.Max(_ => _[periodPropertyName].ToString()));
                        dwthResponse = _dwthService.GetPeerIndVal(industry.crawler, companyInfo.Area, companyInfo.ContrastOne, crawlerCodes, "1", financeialReportPeriods[0].ToString("yyyyMM"), financeialReportPeriods[1].ToString("yyyyMM"));
                        var peerOneIndVals = (IEnumerable<PeerIndVal>)dwthResponse.Response.Data;
                        dwthResponse = _dwthService.GetPeerIndVal(industry.crawler, companyInfo.Area, companyInfo.ContrastTwo, crawlerCodes, "1", financeialReportPeriods[0].ToString("yyyyMM"), financeialReportPeriods[1].ToString("yyyyMM"));
                        var peerTwoIndVals = (IEnumerable<PeerIndVal>)dwthResponse.Response.Data;
                        foreach (var dataRow in dataRows)
                        {
                            var industryIndVal = industryIndVals.SingleOrDefault(_ => _.IndId == dataRow["爬蟲編號"].ToString() && _.Period == GetQuarterMarks(dataRow[periodPropertyName].ToString()));
                            if (industryIndVal == null)
                            {
                                //参数年期可能会造成爬虫系统找不到指定周期的行业财务比率，出现这种情况则不需要更新行业财务比率
                                SetWarningMessage($"未找到匹配的行業財務比率，爬蟲編號={dataRow["爬蟲編號"]}，對比行業=[{companyGroup.industryNo}]{companyGroup.industryName}，日期={dataRow[periodPropertyName]}\n");
                                continue;
                            }
                            var category = dataRow["指標類型"].ToString();
                            IList<string> setItems = new List<string>();
                            foreach (var property in _demandProperties)
                            {
                                if (property.name == "能力")
                                {
                                    setItems.Add($"`{property.name}` = '{GetAbilityMarks(decimal.Parse(dataRow["指標公式"].ToString()), category, industryIndVal)}'");//指標值對應的列名為定值：指標公式
                                }
                                else if (!string.IsNullOrWhiteSpace(property.bindIndValPropertyName1) && !string.IsNullOrWhiteSpace(property.bindIndValPropertyName2))
                                {
                                    setItems.Add($"`{property.name}` = {GetClassPropertyValue<decimal>(industryIndVal, GetBindIndValPropertyName(category, property))}");
                                }
                                else if (property.name == "對比公司一")
                                {
                                    var peerIndVal = peerOneIndVals.SingleOrDefault(_ => _.IndId == dataRow["爬蟲編號"].ToString() && _.Period == GetQuarterMarks(dataRow[periodPropertyName].ToString()));
                                    if (peerIndVal != null)
                                    {
                                        setItems.Add($"`{property.name}` = {peerIndVal.IndVal}");
                                    }
                                }
                                else if (property.name == "對比公司二")
                                {
                                    var peerIndVal = peerTwoIndVals.SingleOrDefault(_ => _.IndId == dataRow["爬蟲編號"].ToString() && _.Period == GetQuarterMarks(dataRow[periodPropertyName].ToString()));
                                    if (peerIndVal != null)
                                    {
                                        setItems.Add($"`{property.name}` = {peerIndVal.IndVal}");
                                    }
                                }
                            }
                            
                            var commandText = $@"update `{dataSource.TableName}` set {string.Join(",", setItems)} where `爬蟲編號` = '{dataRow["爬蟲編號"]}' and `{companyPropertyName}` = '{companyGroup.companyNo}' and `{periodPropertyName}` = '{dataRow[periodPropertyName]}'";
                            _mySqlService.ExecuteNonQuery(taskId, command, commandText);
                        }
                    }
                }
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                if (connection != null && connection.State == ConnectionState.Open) connection.Close();
            }
            return message;
        }

        private string GetAbilityMarks(decimal val, string indType, IndustryIndVal indVal)
        {
            if (indType == "收入利润类" || indType == "收入利潤類")
            {
                if (val >= indVal.MaxVal)
                    return "優良";
                else if (val >= indVal.Quartile1 && val < indVal.MaxVal)
                    return "良好";
                else if (val >= indVal.AvgVal && val < indVal.Quartile1)
                    return "尚可";
                else if (val >= indVal.MinVal && val < indVal.AvgVal)
                    return "待加強";
                else if (val < indVal.MinVal)
                    return "嚴重落後";
                else
                    return "";
            }
            else if (indType == "费用周转类" || indType == "費用周轉類")
            {
                if (val < indVal.MinVal)
                    return "優良";
                else if (val < indVal.Quartile3 && val >= indVal.MinVal)
                    return "良好";
                else if (val < indVal.AvgVal && val >= indVal.Quartile3)
                    return "尚可";
                else if (val < indVal.MaxVal && val >= indVal.AvgVal)
                    return "待加強";
                else if (val >= indVal.MaxVal)
                    return "嚴重落後";
                else
                    return "";
            }
            else
                return "";
        }

        private string GetQuarterMarks(string period)
        {
            DateTime dateTime = DateTime.Parse(period.Insert(4, "/")).AddYears(-1);
            string quarterMarks;
            if (dateTime.Month == 1 || dateTime.Month == 2 || dateTime.Month == 3)
                quarterMarks = $"{dateTime.Year}Q1";
            else if (dateTime.Month == 4 || dateTime.Month == 5 || dateTime.Month == 6)
                quarterMarks = $"{dateTime.Year}Q2";
            else if (dateTime.Month == 7 || dateTime.Month == 8 || dateTime.Month == 9)
                quarterMarks = $"{dateTime.Year}Q3";
            else
                quarterMarks = $"{dateTime.Year}Q4";
            return quarterMarks;
        }

        private string GetBindIndValPropertyName(string indType, dynamic demandProperty)
        {
            if (indType == "收入利润类" || indType == "收入利潤類")
                return demandProperty.bindIndValPropertyName1;
            else if (indType == "费用周转类" || indType == "費用周轉類")
                return demandProperty.bindIndValPropertyName2;
            else
                return "";
        }

        private T GetClassPropertyValue<T>(IndustryIndVal indVal, string propertyName)
        {
            if (string.IsNullOrEmpty(propertyName))
            {
                return Activator.CreateInstance<T>();
            }
            return (T)indVal.GetType().GetProperties().Single(_ => _.Name == propertyName).GetValue(indVal);
        }

        /// <summary>
        /// 检测财报周期参数有效性
        /// </summary>
        /// <param name="crawler"></param>
        /// <returns></returns>
        private bool IsInvalidOfFinanceialPeriods(Crawler crawler)
        {
            DateTime sysDateTime = DateTime.Now;
            switch (crawler.Area)
            {
                case FinanceArea.TW:
                    if (sysDateTime.Month != 4 && sysDateTime.Month != 5 && sysDateTime.Month != 8 && sysDateTime.Month != 11)
                    {
                        throw new ArgumentException($"系统参数 Hangfire:CronExpression:ImportFinanceialReport_{crawler.Area} 设置错误，导致在不合理的时间触发导入财报数据的任务");
                    }
                    break;
                case FinanceArea.CN:
                    if (sysDateTime.Month != 5 && sysDateTime.Month != 6 && sysDateTime.Month != 9 && sysDateTime.Month != 12)
                    {
                        throw new ArgumentException($"系统参数 Hangfire:CronExpression:ImportFinanceialReport_{crawler.Area} 设置错误，导致在不合理的时间触发导入财报数据的任务");
                    }
                    break;
                default:
                    throw new NotSupportedException($"不支持财务区域 {crawler.Area}");
            }
            return true;
        }

        /// <summary>
        /// 获取财报周期
        /// </summary>
        /// <param name="minPeriodValue"></param>
        /// <param name="maxPeriodValue"></param>
        /// <returns></returns>
        private DateTime[] GetFinanceialReportPeriods(string minPeriodValue, string maxPeriodValue)
        {
            Func<DateTime, DateTime> GetBeginPeriod = (_) => {
                if (_.Month == 1 || _.Month == 2 || _.Month == 3)
                    return new DateTime(_.Year, 1, 1);
                else if (_.Month == 4 || _.Month == 5 || _.Month == 6)
                    return new DateTime(_.Year, 4, 1);
                else if (_.Month == 7 || _.Month == 8 || _.Month == 9)
                    return new DateTime(_.Year, 7, 1);
                else
                    return new DateTime(_.Year, 10, 1);
            };
            Func<DateTime, DateTime> GetEndPeriod = (_) => {
                if (_.Month == 1 || _.Month == 2 || _.Month == 3)
                    return new DateTime(_.Year, 4, 1);
                else if (_.Month == 4 || _.Month == 5 || _.Month == 6)
                    return new DateTime(_.Year, 7, 1);
                else if (_.Month == 7 || _.Month == 8 || _.Month == 9)
                    return new DateTime(_.Year, 10, 1);
                else
                    return new DateTime((_.Year + 1), 1, 1);
            };
            return new[] { GetBeginPeriod(DateTime.Parse(minPeriodValue.Insert(4, "/")).AddYears(-1)), GetEndPeriod(DateTime.Parse(maxPeriodValue.Insert(4, "/")).AddYears(-1)) };//取去年的数据
        }
    }
}
