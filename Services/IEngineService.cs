using Dispatcher.Models;
using System;

namespace Dispatcher.Services
{
    public interface IEngineService
    {
        /// <summary>
        /// 自动更新
        /// </summary>
        void AutoUpdate();
        /// <summary>
        /// 生成一个更新任务
        /// </summary>
        /// <param name="tenant"></param>
        /// <param name="dataSourceId"></param>
        /// <param name="updateMode"></param>
        void UpdateTask(Tenant tenant, Guid dataSourceId, UpdateMode updateMode);
    }
}
