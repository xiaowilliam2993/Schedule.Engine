using Dispatcher.Models;
using Margin.Core.Data;
using Margin.Core.Data.Entities;
using Margin.Core.Utils;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Entity;
using System.Linq;
using System.Net.Http;

namespace Dispatcher.Services.Impl
{
    public class DispatchServiceImpl : IDispatchService
    {
        private readonly ILogger<DispatchServiceImpl> _logger;
        private readonly IEnumerable<Tenant> _tenants;
        private readonly IMySqlService _mySqlService;

        public DispatchServiceImpl(ILogger<DispatchServiceImpl> logger, IEnumerable<Tenant> tenants, IMySqlService mySqlService)
        {
            _logger = logger;
            _tenants = tenants;
            _mySqlService = mySqlService;
        }
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
        public Tuple<bool, bool> Update(Guid taskId, Tenant tenant, DataContext dataContext, DataSource dataSource, string hashcode, UpdateMode updateMode, DateTime beginDateTime)
        {
            _logger.LogInformation($"taskId={taskId}, update DataSource, DataSourceId={dataSource.DataSourceId}, Name={dataSource.Name}, UpdateStatue={dataSource.UpdateStatus}，params beginDateTime={beginDateTime: yyyy-MM-dd HH:mm:ss.fffffff}");

            if (string.IsNullOrWhiteSpace(dataSource.UpdateSql))
            {
                dataSource.UpdateStatus = UpdateStatusType.Normal;
                _logger.LogInformation($"taskId={taskId}, datasource.UpdateSql is null, skip the update procressing, DataSourceId={dataSource.DataSourceId}, Name={dataSource.Name}");
                return new Tuple<bool, bool>(false, false);
            }

            ////获取上游表
            //Func<IEnumerable<DataSource>> GetUpstreamDs = () => {
            //    IEnumerable<Guid> upstreamDids = dataContext.TableRelation.Where(_ => _.ParentId == dataSource.DataSourceId).Select(_ => _.Id);
            //    return dataContext.DataSource.Where(_ => upstreamDids.Contains(_.DataSourceId)).ToArray();
            //};

            UtilsTools.Timewatch.Restart();
            DateTime startDate = DateTime.Now;
            Tuple<int, int, bool> updateContext = null;

            try
            {
                string dataconnectionstring = dataSource.Connection;
#if DEBUG
                dataconnectionstring = tenant.ConnectionStrings.Data;
#endif
                //更新物理表数据（全量）
                using MySqlConnection connection = new MySqlConnection(dataconnectionstring);
                try
                {
                    connection.Open();
                    updateContext = CreatePhysicalTable(taskId, tenant, dataSource, connection);
                }
                catch (Exception)
                {
                    throw;
                }
                finally
                {
                    if (connection != null && connection.State == ConnectionState.Open)
                    {
                        connection.Close();
                    }
                }

                //在合表更新逻辑开始之前，记录那个时间点的hash1值，在更新完毕后回写（
                //更新逻辑结束后从数据库再取hash2值，判断hash1是否等于hash2，
                //  如果一致，就回写hash值；
                //  如果不一致，说明上游表在更新这段时间内又发生了变更，这个时候直接退出，不修改当前hash值，下一轮重新生成合表；）
                if (hashcode != HashUtil.CreateHashcode(dataSource.DataSourceId, dataContext))
                {
                    return new Tuple<bool, bool>(false, updateContext.Item3);
                }

                var dateTime = DateTime.Now;
                dataSource.UpdateDate = dateTime;
                dataSource.EndDate = dateTime.ToString("yyyy-MM-dd HH:mm:ss");
                dataSource.UpdateStatus = UpdateStatusType.Finish;
                dataSource.Hashcode = hashcode;

                dataContext.Entry(dataSource).State = EntityState.Modified;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"taskId={taskId}，更新实体表发生错误，DataSourceId={dataSource.DataSourceId}，Name={dataSource.Name}");
                dataSource.UpdateStatus = UpdateStatusType.Fail;
                throw ex;
            }

            _logger.LogInformation($"taskId={taskId}, UpdateStatus={dataSource.UpdateStatus}");
            int timewatchUsed = UtilsTools.TimewatchUsed;
            _logger.LogInformation($"taskId={taskId}, TimewatchUsed={timewatchUsed}millisecond({Math.Round((double)timewatchUsed / 60000, 2)}min)");
            UtilsTools.Timewatch.Stop();

            //清理数据源缓存
            try
            {
                RemoveTbbCache(taskId, tenant, dataSource);
            } 
            catch (Exception ex)
            {
                _logger.LogError(ex, $"taskId={taskId}，清理数据源缓存发生错误");
            }

            //保存更新日志
            try
            {
                AddDataSourceUpdateLog(taskId, tenant, dataSource, new Tuple<int, int, DateTime>(updateContext.Item1, updateContext.Item2, startDate));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"taskId={taskId}，保存更新日志发生错误");
            }

            return new Tuple<bool, bool>(true, updateContext.Item3);
        }
        /// <summary>
        /// 更新物理表
        /// </summary>
        /// <param name="taskId"></param>
        /// <param name="dataSource"></param>
        /// <param name="connection"></param>
        private Tuple<int, int, bool> CreatePhysicalTable(Guid taskId, Tenant tenant, DataSource dataSource, MySqlConnection connection)
        {
            bool isNewDataSource = false;
            string newTableName = $"link_{Guid.NewGuid()}";
            using (MySqlCommand command = new MySqlCommand() { Connection = connection, CommandTimeout = 1000 })//数据库执行超时时间设置为16分钟
            {
                int beforeUpdateRows = 0;
                if (_mySqlService.IsTableExists(taskId, dataSource.TableName, command))
                {
                    beforeUpdateRows = _mySqlService.GetRowCount(taskId, command, dataSource.TableName);//记录更新前的数据总行数
                }
                else
                {
                    isNewDataSource = true;
                }
                DataTable prepareData = _mySqlService.ExecuteWithAdapter(taskId, connection, $"select * from ({dataSource.UpdateSql}) tab1 limit 0,1");//预执行UpdateSql，检验源SQL的有效性
                _mySqlService.ExecuteNonQuery(taskId, command, $"create table `{newTableName}` {dataSource.UpdateSql}");//创建新的物理表
                if (dataSource.Reference != "jointable" || (dataSource.Reference == "jointable" && !_mySqlService.IsFieldExists(taskId, newTableName, "key_id", command)))
                {
                    _mySqlService.ExecuteNonQuery(taskId, command, $"alter table `{newTableName}` add `key_id` int AUTO_INCREMENT primary key");//自动创建自增列，jointable不需要做这一步操作，UpdateSql会自动创建自增列
                }
                _mySqlService.ExecuteNonQuery(taskId, command, $"alter table `{newTableName}` convert to character set utf8mb4 collate utf8mb4_unicode_ci");//修正排序规则
                CreateIndexForNewTable(taskId, command, dataSource.TableName, newTableName);//自动创建索引
                if (!isNewDataSource)
                {
                    string invalidTableName = $"invalid_{Guid.NewGuid():N}";
                    _mySqlService.ExecuteNonQuery(taskId, command, $"alter table `{dataSource.TableName}` rename as `{invalidTableName}`");//注意：表名最长不可以超过64个字符
                    try
                    {
                        AddTableInvalidHistory(taskId, tenant, dataSource.TableName, invalidTableName);//保存物理表失效记录
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError($"taskId={taskId}，写入物理表失效历史记录发生错误", ex);
                    }
                }
                _mySqlService.ExecuteNonQuery(taskId, command, $"alter table `{newTableName}` rename as `{dataSource.TableName}`");//替换新的物理表
                int afterUpdateRows = _mySqlService.GetRowCount(taskId, command, dataSource.TableName);//记录更新后的数据总行数
                return new Tuple<int, int, bool>(beforeUpdateRows, afterUpdateRows, isNewDataSource);
            };
        }

        /// <summary>
        /// 自动创建索引
        /// </summary>
        /// <param name="taskId"></param>
        /// <param name="command"></param>
        /// <param name="tableName"></param>
        /// <param name="newTableName"></param>
        private void CreateIndexForNewTable(Guid taskId, MySqlCommand command, string tableName, string newTableName)
        {
            try
            {
                string indexsAlterScheme = _mySqlService.GetIndexsAlterScheme(taskId, command.Connection, tableName, newTableName);
                _logger.LogInformation($"taskId={taskId}, indexsAlterScheme: {indexsAlterScheme}");
                if (!string.IsNullOrWhiteSpace(indexsAlterScheme))
                {
                    _mySqlService.ExecuteNonQuery(taskId, command, indexsAlterScheme);
                }
            } 
            catch (Exception)
            {
                _logger.LogError($"taskId={taskId}，自动创建索引发生错误");
                throw;
            }
        }

        /// <summary>
        /// 写入物理表失效历史记录
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="invalidTableName"></param>
        private void AddTableInvalidHistory(Guid taskId, Tenant tenant, string tableName, string invalidTableName)
        {
            _logger.LogInformation($"taskId={taskId}, 写入物理表失效历史记录");
            bool logSwitch = true;
            using MySqlConnection connection = new MySqlConnection(tenant.ConnectionStrings.Master);
            try
            {
                connection.Open();

                using MySqlCommand command = new MySqlCommand { Connection = connection, CommandTimeout = 45 };
                if (!_mySqlService.IsTableExists(taskId, "tableinvalidhistory", command))
                {
                    _mySqlService.ExecuteNonQuery(taskId, command, $@"CREATE TABLE `tableinvalidhistory` (
    `Id` CHAR(36) NOT NULL,
	`TableName` CHAR(60) NOT NULL,
	`InvaildTableName` CHAR(60) NOT NULL,
    `CreateDate` VARCHAR(60) NOT NULL,
	PRIMARY KEY (`Id`)
)", logSwitch);
                }
                _mySqlService.ExecuteNonQuery(taskId, command, @$"INSERT INTO `tableinvalidhistory`(`Id`, `TableName`, `InvaildTableName`, `CreateDate`) 
VALUES ('{Guid.NewGuid()}', '{tableName}', '{invalidTableName}', '{DateTime.Now:yyyy-MM-dd HH:mm:ss.fffffff}')", logSwitch);
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                if (connection != null && connection.State == ConnectionState.Open)
                {
                    connection.Close();
                }
            }
        }
        /// <summary>
        /// 清理数据源缓存
        /// </summary>
        /// <param name="taskId"></param>
        /// <param name="tenant"></param>
        /// <param name="dataSource"></param>
        private void RemoveTbbCache(Guid taskId, Tenant tenant, DataSource dataSource)
        {
            using HttpClient httpClient = new HttpClient
            {
                BaseAddress = new Uri($"{tenant.ApplicationUrl}/api/sync/refreshCache/{dataSource.DataSourceId}")
            };
            using HttpRequestMessage httpRequest = new HttpRequestMessage(HttpMethod.Post, string.Empty);
            HttpResponseMessage httpResponse = httpClient.SendAsync(httpRequest).GetAwaiter().GetResult();
            string result = httpResponse.Content.ReadAsStringAsync().GetAwaiter().GetResult();
            _logger.LogInformation($"taskId={taskId}，数据源缓存清理已完成，dataSourceId={dataSource.DataSourceId}，request uri={httpClient.BaseAddress}，api result:{result}");
        }
        /// <summary>
        /// 保存更新日志
        /// </summary>
        /// <param name="taskId"></param>
        /// <param name="tenant"></param>
        /// <param name="dataSource"></param>
        /// <param name="updateContext"></param>
        private void AddDataSourceUpdateLog(Guid taskId, Tenant tenant, DataSource dataSource, Tuple<int, int, DateTime> updateContext)
        {
            bool logSwitch = true;
            using MySqlConnection connection = new MySqlConnection(tenant.ConnectionStrings.Master);
            try
            {
                connection.Open();
                using (MySqlCommand command = new MySqlCommand { Connection = connection, CommandTimeout = 45 })
                {
                    if (!_mySqlService.IsTableExists(taskId, "datasourceupdatelog", command))
                    {
                        _mySqlService.ExecuteNonQuery(taskId, command, $@"CREATE TABLE `datasourceupdatelog` (
	`Id` CHAR(36) NOT NULL,
	`DataSourceId` CHAR(36) NOT NULL,
	`StartDate` VARCHAR(60) NOT NULL,
	`UpdateDate` VARCHAR(60) NOT NULL,
	`UpdateStatus` INT(11) NOT NULL,
	`BeforeUpdateRows` INT(11) NOT NULL,
	`AfterUpdateRows` INT(11) NOT NULL,
	PRIMARY KEY (`Id`)
)", logSwitch);
                    }
                    else
                    {
                        ////自动修复Schema
                        //ExecuteNonQuery(taskId, command, "ALTER TABLE `datasourceupdatelog` MODIFY COLUMN `UpdateDate` VARCHAR(60)", logSwitch);
                        //ExecuteNonQuery(taskId, command, "DELETE FROM `datasourceupdatelog` WHERE `UpdateDate` = '0000-00-00 00:00:00.0000000'", logSwitch);
                        //if (_mySqlService.IsFieldExists(taskId, "datasourceupdatelog", "StartDate", command))
                        //{
                        //    _mySqlService.ExecuteNonQuery(taskId, command, "ALTER TABLE `datasourceupdatelog` ADD COLUMN `StartDate` VARCHAR(60) NOT NULL AFTER `DataSourceId`");
                        //}
                    }

                    //删除多余的历史记录
                    int recordCountLimit = 9;
                    using MySqlDataReader dataReader1 = _mySqlService.ExecuteReader(taskId, command, $"SELECT * FROM `datasourceupdatelog` WHERE `DataSourceId` = '{dataSource.DataSourceId}' ORDER BY `UpdateDate` DESC LIMIT 0,{recordCountLimit + 1}", logSwitch);
                    if (dataReader1.HasRows)
                    {
                        int rows = 0;
                        DateTime theFirstRecordUpdateDate = DateTime.Now.AddMonths(-1);
                        while (dataReader1.Read())
                        {
                            if (rows == recordCountLimit)
                            {
                                theFirstRecordUpdateDate = Convert.ToDateTime(dataReader1["UpdateDate"]);
                            }
                            rows++;
                        }
                        dataReader1.Close();

                        if (rows > recordCountLimit)
                        {
                            _mySqlService.ExecuteNonQuery(taskId, command, $"DELETE FROM `datasourceupdatelog` WHERE `DataSourceId` = '{dataSource.DataSourceId}' AND `UpdateDate` <= '{theFirstRecordUpdateDate:yyyy-MM-dd HH:mm:ss.fffffff}'", logSwitch);
                        }
                    }
                    else
                    {
                        dataReader1.Close();
                    }

                    //写入新的log
                    _mySqlService.ExecuteNonQuery(taskId, command, @$"INSERT INTO datasourceupdatelog(`Id`,`DataSourceId`,`StartDate`,`UpdateDate`,`UpdateStatus`,`BeforeUpdateRows`,`AfterUpdateRows`) 
VALUES ('{Guid.NewGuid()}','{dataSource.DataSourceId}','{updateContext.Item3:yyyy-MM-dd HH:mm:ss.fffffff}','{dataSource.UpdateDate:yyyy-MM-dd HH:mm:ss.fffffff}',{(int)dataSource.UpdateStatus},{updateContext.Item1},{updateContext.Item2})", logSwitch);

                    connection.Close();
                }
                _logger.LogInformation($"taskId={taskId}，dataSourceId={dataSource.DataSourceId}，写入更新日志");
            }
            catch (Exception) 
            { 
                throw; 
            }
            finally
            {
                if (connection != null && connection.State == ConnectionState.Open)
                {
                    connection.Close();
                }
            }
        }
        /// <summary>
        /// 删除失效的物理表
        /// </summary>
        public void DeleteInvalidPhysicalTables()
        {
            Guid taskId = Guid.NewGuid();
            try
            {
                foreach (var group in _tenants.GroupBy(_ => _.ConnectionStrings.Master))
                {
                    Tenant tenant = group.First();

                    IList<string> invalidTables = new List<string>();
                    //删除已失效的物理表
                    using MySqlConnection dataConnection = new MySqlConnection(tenant.ConnectionStrings.Data);
                    try
                    {
                        dataConnection.Open();
                        DataTable tableData = _mySqlService.ExecuteWithAdapter(taskId, dataConnection, $"select distinct `table_name` from `information_schema`.`columns` where `table_name` like 'invalid_%' and `table_schema` = '{dataConnection.Database}'");
                        if (tableData.Rows.Count > 0)
                        {
                            using MySqlCommand command = new MySqlCommand { Connection = dataConnection };
                            foreach (DataRow item in tableData.Rows)
                            {
                                string invalidTableName = item["table_name"].ToString();
                                invalidTables.Add(invalidTableName);
                                _mySqlService.ExecuteNonQuery(taskId, command, $"drop table if exists `{invalidTableName}`");
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, $"taskId={taskId}，删除物理表发生错误，租户信息：{JsonConvert.SerializeObject(tenant)}");
                    }
                    finally
                    {
                        if (dataConnection != null && dataConnection.State == ConnectionState.Open)
                        {
                            dataConnection.Close();
                        }
                    }

                    //删除物理表失效历史记录
                    using MySqlConnection masterConnection = new MySqlConnection(tenant.ConnectionStrings.Master);
                    try
                    {
                        masterConnection.Open();
                        using MySqlCommand command = new MySqlCommand { Connection = masterConnection };
                        foreach (var invalidTableName in invalidTables)
                        {
                            _mySqlService.ExecuteNonQuery(taskId, command, $"delete from `tableinvalidhistory` where `InvalidTableName` = `{invalidTableName}`");
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, $"taskId={taskId}，删除物理表失效历史记录发生错误，租户信息：{JsonConvert.SerializeObject(tenant)}");
                    }
                    finally
                    {
                        if (masterConnection != null && masterConnection.State == ConnectionState.Open)
                        {
                            masterConnection.Close();
                        }
                    }
                }
            } 
            catch (Exception ex)
            {
                _logger.LogError(ex, "taskId={taskId}，删除标记为待删除的物理表发生错误");
            }
        }
    }
}
