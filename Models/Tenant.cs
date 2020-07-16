namespace Dispatcher.Models
{
    public class Tenant
    {
        /// <summary>
        /// 租户名称
        /// </summary>
        public string Name { get; set; }
        /// <summary>
        /// TBB网站地址，scheme://ip:port
        /// </summary>
        public string ApplicationUrl { get; set; }
        /// <summary>
        /// 为了满足开发需求，增加该字段，描述开发环境与租户的对应关系
        /// </summary>
        public string DevelopUrl { get; set; }
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
