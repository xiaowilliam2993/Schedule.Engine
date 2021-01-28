namespace Dispatcher.Models
{
    public class Tenant
    {
        /// <summary>
        /// 租户名称
        /// </summary>
        public string Name { get; set; }
        /// <summary>
        /// 主库名字，用于接收来自TBB匹配租户的依据
        /// </summary>
        public string MasterData { get; set; }
        /// <summary>
        /// TBB网站地址，scheme://ip:port
        /// </summary>
        public string ApplicationUrl { get; set; }
        /// <summary>
        /// 数据库连接字符串
        /// </summary>
        public ConnectionString ConnectionStrings { get; set; }
    }
    public class ConnectionString
    {
        /// <summary>
        /// 主数据库
        /// </summary>
        public string Master { get; set; }
        /// <summary>
        /// data数据库
        /// </summary>
        public string Data { get; set; }
    }
}
