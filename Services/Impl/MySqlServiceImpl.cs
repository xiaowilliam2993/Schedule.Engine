using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;

namespace Dispatcher.Services.Impl
{
    public class MySqlServiceImpl : IMySqlService
    {
        private readonly ILogger<MySqlServiceImpl> _logger;

        public MySqlServiceImpl(ILogger<MySqlServiceImpl> logger)
        {
            _logger = logger;
        }

        public int ExecuteNonQuery(Guid taskId, MySqlCommand command, string sql, bool logSwitch = true)
        {
            int litHashCode = DateTime.Now.ToString("yyyyMMddHHmmssfff").GetHashCode();
            if (logSwitch) _logger.LogInformation($"taskId={taskId}, litHashCode={litHashCode}, execute sql: {sql}");
            command.CommandText = sql;
            int effectRows = command.ExecuteNonQuery();
            if (logSwitch) _logger.LogInformation($"taskId={taskId}, litHashCode={litHashCode}, effectRows={effectRows}");
            return effectRows;
        }

        public MySqlDataReader ExecuteReader(Guid taskId, MySqlCommand command, string sql, bool logSwitch = true)
        {
            if (logSwitch) _logger.LogInformation($"taskId={taskId}, execute sql: {sql}");
            command.CommandText = sql;
            return command.ExecuteReader();
        }

        public DataTable ExecuteWithAdapter(Guid taskId, MySqlConnection connection, string sql, bool logSwitch = true)
        {
            if (logSwitch) _logger.LogInformation($"taskId={taskId}, execute sql: {sql}");
            return ExecuteWithAdapter(connection, sql);
        }

        public DataTable ExecuteWithAdapter(MySqlConnection connection, string sql)
        {
            DataTable dataTable = new DataTable();
            using MySqlDataAdapter dataAdapter = new MySqlDataAdapter(sql, connection);
            dataAdapter.SelectCommand.CommandTimeout = 1000;
            dataAdapter.Fill(dataTable);
            return dataTable;
        }

        public int BulkCopy(Guid taskId, string tableFullName, MySqlConnection connection, DataTable dataTable, IEnumerable<string> columnFields)
        {
            _logger.LogInformation($"taskId={taskId}, bulk copy start.");

            int effectRows = 0;

            if (dataTable.Rows.Count == 0) return effectRows;

            Func<DataTable, string> DataTableToCsv = (data) => {
                StringBuilder builder = new StringBuilder();
                DataColumn colum;
                foreach (DataRow row in data.Rows)
                {
                    for (int i = 0; i < data.Columns.Count; i++)
                    {
                        colum = data.Columns[i];
                        if (i != 0)
                        {
                            builder.Append(",");
                        }
                        string cellValue = row[colum].ToString();
                        if (colum.DataType == typeof(string) && cellValue.Contains(","))
                        {
                            builder.Append("\"" + cellValue.Replace("\"", "\"\"") + "\"");
                        }
                        else if (colum.DataType == typeof(DateTime) && row[colum] != null && !string.IsNullOrEmpty(cellValue))
                        {
                            try
                            {
                                builder.Append($"{(DateTime)row[colum]:yyyy-MM-dd HH:mm:ss}");
                            }
                            catch
                            {
                                builder.Append("0001-01-01");
                            }
                        }
                        else
                        {
                            builder.Append(cellValue);
                        }
                    }
                    builder.AppendLine();
                }
                return builder.ToString();
            };

            string tempPath = Path.GetTempFileName();
            try
            {
                File.WriteAllText(tempPath, DataTableToCsv(dataTable), Encoding.UTF8);

                MySqlBulkLoader bulkLoader = new MySqlBulkLoader(connection)
                {
                    FieldTerminator = ",",
                    FieldQuotationCharacter = '"',
                    EscapeCharacter = '"',
                    LineTerminator = Environment.NewLine,
                    FileName = tempPath,
                    Local = true,
                    NumberOfLinesToSkip = 0,
                    TableName = tableFullName,
                };
                bulkLoader.Columns.AddRange(columnFields);
                effectRows = bulkLoader.Load();

                _logger.LogInformation($"taskId={taskId}, bulk copy end, effectRows={effectRows}.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"taskId={taskId}, bulk copy occured an error: {ex.Message}");
                throw ex;
            }
            finally
            {
                if (File.Exists(tempPath)) File.Delete(tempPath);
            }
            return effectRows;
        }

        public bool IsTableExists(Guid taskId, string tableFullName, MySqlCommand command, bool logSwitch = true)
        {
            bool exists = false;
            string sql = $"select `column_name` from `information_schema`.`columns` where `table_name` = '{tableFullName}' and `table_schema` = '{command.Connection.Database}'";
            using MySqlDataReader dataReader = ExecuteReader(taskId, command, sql);
            if (dataReader.HasRows)
            {
                exists = true;
            }
            dataReader.Close();
            return exists;
        }

        public bool IsFieldExists(Guid taskId, string tableFullName, string columnName, MySqlCommand command, bool logSwitch = true)
        {
            bool exists = false;
            string sql = $"select * from information_schema.columns where table_name = '{tableFullName}' and column_name = '{columnName}' and table_schema = '{command.Connection.Database}'";
            using MySqlDataReader dataReader = ExecuteReader(taskId, command, sql);
            if (dataReader.HasRows)
            {
                exists = true;
            }
            dataReader.Close();
            return exists;
        }

        public string GetIndexsAlterScheme(Guid taskId, MySqlConnection connection, string tableFullName, string newTableFullName)
        {
            string sql = @$"SELECT CONCAT('ALTER TABLE `',TABLE_NAME,'` ', 'ADD ', IF(NON_UNIQUE = 1, CASE UPPER(INDEX_TYPE) WHEN 'FULLTEXT' THEN 'FULLTEXT INDEX' WHEN 'SPATIAL' THEN 'SPATIAL INDEX' ELSE CONCAT('INDEX `', INDEX_NAME, '` USING ', INDEX_TYPE) END, IF(UPPER(INDEX_NAME) = 'PRIMARY', CONCAT('PRIMARY KEY USING ', INDEX_TYPE), CONCAT('UNIQUE INDEX `', INDEX_NAME, '` USING ', INDEX_TYPE))),'(', GROUP_CONCAT(DISTINCT CONCAT('`', COLUMN_NAME, '`')
ORDER BY SEQ_IN_INDEX ASC SEPARATOR ', '), ');') AS 'Show_Add_Indexes'
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA ='{connection.Database}' AND TABLE_NAME='{tableFullName}'
GROUP BY TABLE_NAME, INDEX_NAME
ORDER BY TABLE_NAME ASC, INDEX_NAME ASC";
            DataTable dataTable = ExecuteWithAdapter(taskId, connection, sql);
            IEnumerable<DataRow> dataRows = dataTable.Rows.Cast<DataRow>();
            if (dataRows.Any(_ => _["Show_Add_Indexes"].ToString().IndexOf(" PRIMARY KEY ") > 0))
            {
                if (ExecuteWithAdapter(taskId, connection, @$"select *
from information_schema.table_constraints
where table_name = '{newTableFullName}'
and table_schema = '{connection.Database}'
and constraint_name = 'PRIMARY'").Rows.Count > 0)
                {
                    dataRows = dataRows.Where(_ => !_["Show_Add_Indexes"].ToString().Contains(" PRIMARY KEY "));
                }
            }
            return string.Join(Environment.NewLine, dataRows.Select(_ => _["Show_Add_Indexes"].ToString())).Replace(tableFullName, newTableFullName);
        }

        public int GetRowCount(Guid taskId, MySqlCommand command, string tableName)
        {
            string sql = $"select count(*) as rowcount from `{tableName}`";
            _logger.LogInformation($"taskId={taskId}, execute sql: {sql}");
            command.CommandText = sql;
            return Convert.ToInt32(command.ExecuteScalar());
        }
    }
}
