namespace KafkaConsoleApplication.Redis;

/// <summary>
/// Redis repository
/// </summary>
public interface IRedisRepository
{
    /// <summary>
    /// Return true if the key exists, otherwise false
    /// </summary>
    Task<bool> KeyExistsAsync(string key);

    /// <summary>
    /// Add key
    /// </summary>
    Task AddKeyAsync(string key);

    /// <summary>
    /// Add range of keys
    /// </summary>
    Task AddKeysAsync(string[] keys);
}