## AppSettingBinder
    提供 [AppSetting] 屬性和 IConfiguration<> 服務，可以使用比較簡便的方式讀取 appsettings.json

## 字串轉型延伸工具
    基本資料型態與字串間的轉換，通常使用 Parse, TryParse 等功能
    這一系列的延伸工具提供 "string".ToInt32, "string".ToInt64, "string".ToDecimal....等功能，提供另一種形式的字串轉換寫法
    另外也提供了 "string".Enum<T> 的轉換函式，簡化傳統的 (T)Enum.Prase(typeof(T), "string") 的寫法

## 資料庫相關
    DbConnectionPooling，重複使用資料庫連線
    
    針對 IDataReader 的延伸工具
        封裝 IDataReader.GetInt32(IDataReader.GetOrdinal("field_name"))
        讓 IDataReader 可以直接使用 IDataReader.GetInt32("field_name")

    針對 Dapper 的延伸工具
        Dapper 大部分的功能都是 IDbConnection 的 Extension，但有使用 IDbTransaction 時，常常容易在呼叫參數時忘記把 IDbTransaction 帶入
        所以時做了一系列的功能，讓 IDbTransaction 也可以直接呼叫 Execute, ExecuteReader, ExecuteScalar, Query, QueryFirst, QueryFirstOrDefault 等操作

    TableNameAttribute，輔助標記用，在跟資料列對應的 class 加上宣告，註明這個 class 是根據哪個資料表定義的

## 加密/解密
    靜態類別 System.Security.Cryptography.Crypto 封裝了加解密和雜湊功能，簡化相關的需求
        提供的方法：
            Base64
            MD5
            CFS
            SHA1
            AES
            TripleDES
            DES
            RSA

## redis
    RedisConnectionPoll - 重複使用 redis 連線
    RedisConnection - 重新封裝 StackExchange.Redis，搭配 RedisConnectionPoll，另外提供一些簡易方法操作 redis

## 其他
    StringFormatWith - 標準的 String.Format 使用 {0}, {1}... 的寫法，這邊提供 {name0}, {name1}... 的寫法
    TimeCounter - 輕量化的計時器，跟 StopWatch 類似，但佔用的資源較少
    UnixTimeStamp - UnixTimeStamp 轉換工具
    RandomValue - 亂數產生器，可以產生數值或字串
