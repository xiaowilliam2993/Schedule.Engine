using Dispatcher.Models;
using System;
using System.Linq;
using System.Collections.Generic;
using Margin.Core.Data;
using Hangfire;
using Newtonsoft.Json;
using Microsoft.Extensions.Logging;
using Margin.Core.Utils;

namespace Dispatcher.Services.Impl
{
    public class EngineServiceImpl : IEngineService
    {
        private readonly ILogger<EngineServiceImpl> _logger;
        private readonly IEnumerable<Tenant> _tenants;
        private readonly IDispatchService _dispatchService;
        private static readonly object _updateObject = new object();
        private bool _autoUpdateIsBusy = false;
        public EngineServiceImpl(ILogger<EngineServiceImpl> logger, IEnumerable<Tenant> tenants, IDispatchService dispatchService)
        {
            _logger = logger;
            _tenants = tenants;
            _dispatchService = dispatchService;
        }
        public void AutoUpdate()
        {
            lock (_updateObject)
            {
                if (_autoUpdateIsBusy)
                {
                    return;
                }
                _autoUpdateIsBusy = true;
            }
            string updateCode = $"{Guid.NewGuid():N}";
            try
            {
                _logger.LogInformation($"updateCode={updateCode}，自动更新开始");
                foreach (var group in _tenants.GroupBy(_ => _.ConnectionStrings.Master))
                {
                    Tenant tenant = group.First();
                    _logger.LogInformation($"updateCode={updateCode}，Tenant.Name={tenant.Name}");
                    try
                    {
                        using DataContext context = new DataContext(group.Key);

                        //1、找到关系网最下层节点（基表，Sync、Excel）
                        var basedslist = context.DataSource.Where(_ =>
                            _.ProjectId != null//排除文件夹
                            && (_.Reference == "Sync" || _.Reference == "Excel" || _.Reference == "TRANSPOSE")//底层表的类型限定为：同步客户端上传、Excel导入、二维转一维
                            && _.TableName != "indicatorwarehouse"//排除指标数据源
                            && !string.IsNullOrEmpty(_.Hashcode)//Hashcode是新增字段，增加此条件是为了兼容旧数据，如果工作表更新过，Hashcode会重现生成
                        ).ToArray();
                        IList<Guid> composedsidlist = new List<Guid>();
                        foreach (var baseds in basedslist)
                        {
                            foreach (var downstreamrelation in context.TableRelation.Where(_ => _.Id == baseds.DataSourceId).ToArray())
                            {
                                //2、找到合表第一层关系节点
                                if (!composedsidlist.Contains(downstreamrelation.ParentId)) composedsidlist.Add(downstreamrelation.ParentId);
                            }
                        }

                        foreach (var id in composedsidlist)
                        {
                            var dataSource = context.DataSource.Find(id);
                            if (dataSource != null) 
                            {
                                if (dataSource.Reference == "jointable" || dataSource.Reference == "uniontable" || dataSource.Reference == "grouptable")
                                {
                                    if (string.IsNullOrWhiteSpace(dataSource.Hashcode) || HashUtil.CreateHashcode(id, context) != dataSource.Hashcode)
                                    {
                                        BackgroundJob.Enqueue<IEngineService>(services => services.UpdateTask(tenant, id, UpdateMode.AutoUpdate));
                                    }
                                }
                            }
                            else
                            {
                                _logger.LogError($"updateCode={updateCode}, Tenent.Name={tenant.Name}, 工作表（DataSourceId={id}）未找到");
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        //单个租户发生异常，不影响其他租户自动更新
                        _logger.LogError(ex, $"updateCode={updateCode}，自动更新发生错误，租户信息：{JsonConvert.SerializeObject(tenant)}");
                    }
                }
                _autoUpdateIsBusy = false;
            }
            catch (Exception ex)
            {
                _autoUpdateIsBusy = false;
                _logger.LogError(ex, $"updateCode={updateCode}，检索租户配置信息发生错误");
                throw ex;
            }
            _logger.LogInformation($"updateCode={updateCode}，自动更新结束");
        }
        public void UpdateTask(Tenant tenant, Guid dataSourceId, UpdateMode updateMode)
        {
            var taskId = Guid.NewGuid();
            _logger.LogInformation($"taskId={taskId}，开始执行调度任务");
            try
            {
                DateTime beginDateTime = DateTime.Now;//记录更新开始时间
                using var ctx = new DataContext(tenant.ConnectionStrings.Master);
                var dataSource = ctx.DataSource.Find(dataSourceId);
                if (dataSource == null)
                {
                    throw new KeyNotFoundException(dataSourceId.ToString());
                }

                Tuple<bool, bool> result = _dispatchService.Update(taskId, tenant, ctx, dataSource, HashUtil.CreateHashcode(dataSourceId, ctx), updateMode, beginDateTime);
                ctx.SaveChanges();
                _logger.LogInformation($"taskId={taskId}，调度任务已完成");

                if (result.Item1)//更新成功，触发上游表更新
                {
                    var relations = ctx.TableRelation.Where(_ => _.Id == dataSource.DataSourceId).ToArray();
                    if (relations.Length > 0)
                    {
                        foreach (var item in relations)
                        {
                            var relationds = ctx.DataSource.Find(item.ParentId);
                            if (relationds != null)
                            {
                                _logger.LogInformation($"taskId={taskId}，更新下游表，tablerelation.DataSource(ParentId).Name={relationds.Name}");
                                BackgroundJob.Enqueue<IEngineService>(services => services.UpdateTask(tenant, relationds.DataSourceId, updateMode));
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"taskId={taskId}，调度任务执行失败: params tenant={JsonConvert.SerializeObject(tenant)}, params dataSourceId={dataSourceId}");
                throw ex;
            }
        }
    }
}
