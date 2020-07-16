using Dispatcher.Models;
using Margin.Core.Data;
using Margin.Core.Data.Entities;
using System;

namespace Dispatcher.Services
{
    /// <summary>
    /// 描述更新任务来源
    /// </summary>
    public enum UpdateMode
    {
        /// <summary>
        /// API发起的任务
        /// </summary>
        FromApi,
        /// <summary>
        /// 自动更新发起的任务
        /// </summary>
        AutoUpdate
    }
    public interface IDispatchService
    {
        /// <summary>
        /// 更新实体表
        /// </summary>
        /// <param name="taskId"></param>
        /// <param name="tenant"></param>
        /// <param name="dataContext"></param>
        /// <param name="dataSource"></param>
        /// <param name="hashcode"></param>
        /// <param name="updateMode"></param>
        /// <param name="beginDateTime"></param>
        /// <returns></returns>
        Tuple<bool, bool> Update(Guid taskId, Tenant tenant, DataContext dataContext, DataSource dataSource, string hashcode, UpdateMode updateMode, DateTime beginDateTime);
        /// <summary>
        /// 删除失效的物理表
        /// </summary>
        void DeleteInvalidPhysicalTables();
    }
}
