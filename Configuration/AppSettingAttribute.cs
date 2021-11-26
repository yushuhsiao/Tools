using System;
using _DebuggerStepThrough = System.Diagnostics.FakeDebuggerStepThroughAttribute;

namespace Microsoft.Extensions.Configuration
{
    [_DebuggerStepThrough]
    public class AppSettingAttribute : Attribute//, IAppSettingAttribute
    {
        public const string ConnectionStrings = "ConnectionStrings";

        public string SectionName { get; set; }
        public string Key { get; set; }
        
        public AppSettingAttribute() { }
        public AppSettingAttribute(string key) : this(null, key) { }
        public AppSettingAttribute(string sectionName, string key) { this.SectionName = sectionName; this.Key = key; }
    }
}
