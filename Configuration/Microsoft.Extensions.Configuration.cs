namespace Microsoft.Extensions.Configuration
{
    public static class Extensions
    {
        public static string[] GetArray(this IConfiguration configuration, string key)
        {
            string value = configuration.GetValue<string>(key);
            string[] result = value.Split(',');
            for (int i = 0; i < result.Length; i++)
                result[i] = result[i]?.Trim();
            return result;
        }
    }
}
