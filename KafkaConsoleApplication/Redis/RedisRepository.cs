using StackExchange.Redis;

namespace KafkaConsoleApplication.Redis;

/// <inheritdoc />
public class RedisRepository : IRedisRepository
{
    private readonly IDatabase _database;

    public RedisRepository()
    {
        var redis = ConnectionMultiplexer.Connect("localhost");
        _database = redis.GetDatabase();
    }

    /// <inheritdoc />
    public Task<bool> KeyExistsAsync(string key)
    {
        return _database.KeyExistsAsync(key, CommandFlags.PreferMaster);
    }

    /// <inheritdoc />
    public Task AddKeyAsync(string key)
    {
        var byteArray = new[] { (byte)1 };
        return _database.StringSetAsync(key, byteArray);
    }

    /// <inheritdoc />
    public Task AddKeysAsync(string[] keys)
    {
        var tasks = new List<Task>(keys.Length);

        for (var i = 0; i < keys.Length; i++)
        {
            tasks.Add(AddKeyAsync(keys[i]));
        }

        return Task.WhenAll(tasks);
    }
}