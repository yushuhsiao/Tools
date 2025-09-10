using System;
using _DebuggerStepThrough = System.Diagnostics.FakeDebuggerStepThroughAttribute;

namespace Microsoft.Extensions.Configuration
{
    [_DebuggerStepThrough]
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = true)]
    public class AppSettingProviderAttribute : Attribute//, IAppSettingAttribute
    {
        public const string ConnectionStrings = "ConnectionStrings";

        public Type Type { get; set; }
        public bool Enablewrite { get; set; } = true;

        public AppSettingProviderAttribute(Type type)
        {
            this.Type = type;
        }
    }
}
