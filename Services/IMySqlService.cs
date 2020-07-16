using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;

namespace Dispatcher.Services
{
    public interface IMySqlService
    {
        int ExecuteNonQuery(Guid taskId, MySqlCommand command, string sql, bool logSwitch = true);
        MySqlDataReader ExecuteReader(Guid taskId, MySqlCommand command, string sql, bool logSwitch = true);
        DataTable ExecuteWithAdapter(Guid taskId, MySqlConnection connection, string sql, bool logSwitch = true);
        int BulkCopy(Guid taskId, string tableFullName, MySqlConnection connection, DataTable dataTable, IEnumerable<string> columnFields);
        bool IsTableExists(Guid taskId, string tableFullName, MySqlCommand command, bool logSwitch = true);
        bool IsFieldExists(Guid taskId, string tableFullName, string columnName, MySqlCommand command, bool logSwitch = true);
        string GetIndexsAlterScheme(Guid taskId, MySqlConnection connection, string tableFullName, string newTableFullName);
        int GetRowCount(Guid taskId, MySqlCommand command, string tableName);
    }
}
