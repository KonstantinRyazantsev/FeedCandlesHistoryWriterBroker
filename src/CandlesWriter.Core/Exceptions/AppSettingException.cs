using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CandlesWriter.Core
{
    public class AppSettingException : Exception
    {
        public AppSettingException() { }
        public AppSettingException(string message) : base(message) { }
        public AppSettingException(string message, Exception inner) : base(message, inner) { }
    }
}
