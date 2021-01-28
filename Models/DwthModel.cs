using Newtonsoft.Json;

namespace Dispatcher.Models
{
    public class DwthResponseModel
    {
        [JsonProperty("duration")]
        public int Duration { get; set; }
        [JsonProperty("statusDescription")]
        public string StatusDescription { get; set; }
        [JsonProperty("response")]
        public DwthResponse Response { get; set; }
        [JsonProperty("profile")]
        public DwthProfile Profile { get; set; }
        [JsonProperty("uuid")]
        public string Uuid { get; set; }
        [JsonProperty("status")]
        public int Status { get; set; }
    }
    public class DwthResponse
    {
        [JsonProperty("message")]
        public string Message { get; set; }
        [JsonProperty("success")]
        public bool Success { get; set; }
        [JsonProperty("data")]
        public object Data { get; set; }
    }
    public class DwthProfile
    {
        [JsonProperty("tenantName")]
        public string TenantName { get; set; }
        [JsonProperty("tenantSid")]
        public long TenantSid { get; set; }
        [JsonProperty("tenantId")]
        public string TenantId { get; set; }
        [JsonProperty("userName")]
        public string UserName { get; set; }
        [JsonProperty("userId")]
        public string UserId { get; set; }
    }
    public class PeerIndVal
    {
        [JsonProperty("ind_id")]
        public string IndId { get; set; }
        [JsonProperty("comp_id")]
        public string CompId { get; set; }
        [JsonProperty("comp_sname")]
        public string CompName { get; set; }
        [JsonProperty("yyyy_period")]
        public string Period { get; set; }
        [JsonProperty("ind_val")]
        public decimal IndVal { get; set; }
        [JsonProperty("ratio_unit")]
        public string RatioUnit { get; set; }
    }
    public class IndustryIndVal
    {
        [JsonProperty("ind_id")]
        public string IndId { get; set; }
        [JsonProperty("industry_id")]
        public string IndustryId { get; set; }
        [JsonProperty("industry_name")]
        public string IndustryName { get; set; }
        [JsonProperty("yyyy_period")]
        public string Period { get; set; }
        /// <summary>
        /// 均標
        /// </summary>
        [JsonProperty("avg_val")]
        public decimal AvgVal { get; set; }
        /// <summary>
        /// 頂標
        /// </summary>
        [JsonProperty("max_val")]
        public decimal MaxVal { get; set; }
        /// <summary>
        /// 底標
        /// </summary>
        [JsonProperty("min_val")]
        public decimal MinVal { get; set; }
        /// <summary>
        /// 前標
        /// </summary>
        [JsonProperty("1st_quartile")]
        public decimal Quartile1 { get; set; }
        [JsonProperty("2nd_quartile")]
        public decimal Quartile2 { get; set; }
        /// <summary>
        /// 後標
        /// </summary>
        [JsonProperty("3rd_quartile")]
        public decimal Quartile3 { get; set; }
        [JsonProperty("ratio_unit")]
        public string RatioUnit { get; set; }
    }
}
