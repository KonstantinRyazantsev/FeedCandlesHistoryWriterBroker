using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CandlesWriter.Core
{
    public class Utils
    {
        private static readonly object _sync = new object();
        private static Dictionary<string, DateTime> times = new Dictionary<string, DateTime>();

        /// <summary>
        /// Prohibits action execution more often than one time per 60 minutes. 
        /// Checks last execution time by specified key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="action"></param>
        /// <returns></returns>
        public static async Task ThrottleActionAsync(string key, Func<Task> action)
        {
            DateTime last = DateTime.MinValue;
            lock (_sync)
            {
                if (!times.TryGetValue(key, out last))
                {
                    times[key] = DateTime.UtcNow;
                }
                else if (DateTime.UtcNow - last < TimeSpan.FromMinutes(60))
                {
                    return;
                }
                else
                {
                    times[key] = DateTime.UtcNow;
                }
            }

            await action();
        }
    }
}
