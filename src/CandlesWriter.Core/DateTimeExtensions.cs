using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common;
using Lykke.Domain.Prices;

namespace CandlesWriter.Core
{
    public static class DateTimeExtensions
    {
        public static DateTime RoundTo(this DateTime dateTime, TimeInterval interval)
        {
            switch (interval)
            {
                case TimeInterval.Month: return new DateTime(dateTime.Year, dateTime.Month, 1, 0, 0, 0, dateTime.Kind);
                case TimeInterval.Day: return new DateTime(dateTime.Year, dateTime.Month, dateTime.Day, 0, 0, 0, dateTime.Kind);
                case TimeInterval.Hour: return DateTime.SpecifyKind(dateTime.RoundToHour(), dateTime.Kind);
                case TimeInterval.Min30: return DateTime.SpecifyKind(dateTime.RoundToMinute(30), dateTime.Kind);
                case TimeInterval.Min15: return DateTime.SpecifyKind(dateTime.RoundToMinute(15), dateTime.Kind);
                case TimeInterval.Min5: return DateTime.SpecifyKind(dateTime.RoundToMinute(5), dateTime.Kind);
                case TimeInterval.Minute: return DateTime.SpecifyKind(dateTime.RoundToMinute(), dateTime.Kind);
                case TimeInterval.Sec: return DateTime.SpecifyKind(dateTime.TruncMiliseconds(), dateTime.Kind);
                default:
                    throw new ArgumentOutOfRangeException(nameof(interval), interval, "Unexpected TimeInterval value.");
            }
        }
    }
}
