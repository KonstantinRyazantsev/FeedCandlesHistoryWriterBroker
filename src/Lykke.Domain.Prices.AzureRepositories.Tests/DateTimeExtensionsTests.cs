using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Lykke.Domain.Prices.AzureProvider.History.Model;

namespace Lykke.Domain.Prices.AzureProvider.Tests
{
    public class DateTimeExtensionTests
    {
        [Fact]
        public void TestsAreCoveringAllIntervals()
        {
            // Tests are written for TimeInterval with 9 values
            Assert.Equal(9, Enum.GetValues(typeof(TimeInterval)).Cast<int>().Count());
        }

        [Fact]
        public void GetIntervalTick_BasicChecks()
        {
            // Sec
            Assert.Equal(0, new DateTime(2017, 2, 8, 11, 12, 0, 1).GetIntervalTick(TimeInterval.Sec));
            Assert.Equal(59, new DateTime(2017, 2, 8, 11, 12, 59, 1).GetIntervalTick(TimeInterval.Sec));
            // Minute
            Assert.Equal(0, new DateTime(2017, 2, 8, 11, 0, 59, 1).GetIntervalTick(TimeInterval.Minute));
            Assert.Equal(59, new DateTime(2017, 2, 8, 11, 59, 59, 1).GetIntervalTick(TimeInterval.Minute));
            // Min5
            Assert.Equal(0, new DateTime(2017, 2, 8, 11, 0, 13, 111).GetIntervalTick(TimeInterval.Min5));
            Assert.Equal(0, new DateTime(2017, 2, 8, 11, 1, 13, 111).GetIntervalTick(TimeInterval.Min5));
            Assert.Equal(2, new DateTime(2017, 2, 8, 11, 12, 13, 111).GetIntervalTick(TimeInterval.Min5));
            // Min15
            Assert.Equal(0, new DateTime(2017, 2, 8, 11, 12, 13, 111).GetIntervalTick(TimeInterval.Min15));
            Assert.Equal(3, new DateTime(2017, 2, 8, 11, 59, 13, 111).GetIntervalTick(TimeInterval.Min15));
            // Min30
            Assert.Equal(0, new DateTime(2017, 2, 8, 11, 12, 13, 111).GetIntervalTick(TimeInterval.Min30));
            Assert.Equal(1, new DateTime(2017, 2, 8, 11, 59, 13, 111).GetIntervalTick(TimeInterval.Min30));
            // Hour
            Assert.Equal(11, new DateTime(2017, 2, 8, 11, 50, 13, 111).GetIntervalTick(TimeInterval.Hour));
            // Day
            Assert.Equal(1, new DateTime(2017, 1, 1, 11, 50, 13, 111).GetIntervalTick(TimeInterval.Day));
            Assert.Equal(31, new DateTime(2017, 1, 31, 11, 50, 13, 111).GetIntervalTick(TimeInterval.Day));
            // Month
            Assert.Equal(1, new DateTime(2017, 1, 8, 11, 50, 13, 111).GetIntervalTick(TimeInterval.Month));
            Assert.Equal(12, new DateTime(2017, 12, 8, 11, 50, 13, 111).GetIntervalTick(TimeInterval.Month));
        }

        [Fact]
        public void AddIntervalTicks_BasicChecks()
        {
            DateTime baseTime = new DateTime(2017, 1, 1, 0, 0, 0);

            // Sec
            Assert.Equal(baseTime.AddSeconds(0), baseTime.AddIntervalTicks(0, TimeInterval.Sec));
            Assert.Equal(baseTime.AddSeconds(1), baseTime.AddIntervalTicks(1, TimeInterval.Sec));
            // Minute
            Assert.Equal(baseTime.AddMinutes(0), baseTime.AddIntervalTicks(0, TimeInterval.Minute));
            Assert.Equal(baseTime.AddMinutes(1), baseTime.AddIntervalTicks(1, TimeInterval.Minute));
            // Min5
            Assert.Equal(baseTime.AddMinutes(0), baseTime.AddIntervalTicks(0, TimeInterval.Min5));
            Assert.Equal(baseTime.AddMinutes(5), baseTime.AddIntervalTicks(1, TimeInterval.Min5));
            // Min15
            Assert.Equal(baseTime.AddMinutes(0), baseTime.AddIntervalTicks(0, TimeInterval.Min15));
            Assert.Equal(baseTime.AddMinutes(15), baseTime.AddIntervalTicks(1, TimeInterval.Min15));
            // Min30
            Assert.Equal(baseTime.AddMinutes(0), baseTime.AddIntervalTicks(0, TimeInterval.Min30));
            Assert.Equal(baseTime.AddMinutes(30), baseTime.AddIntervalTicks(1, TimeInterval.Min30));
            // Hour
            Assert.Equal(baseTime.AddHours(0), baseTime.AddIntervalTicks(0, TimeInterval.Hour));
            Assert.Equal(baseTime.AddHours(1), baseTime.AddIntervalTicks(1, TimeInterval.Hour));
            // Days ticks are in [1..31]
            Assert.Equal(baseTime.AddDays(0), baseTime.AddIntervalTicks(1, TimeInterval.Day));
            Assert.Equal(baseTime.AddDays(1), baseTime.AddIntervalTicks(2, TimeInterval.Day));
            // Months ticks are in [1..12]
            Assert.Equal(baseTime.AddMonths(0), baseTime.AddIntervalTicks(1, TimeInterval.Month));
            Assert.Equal(baseTime.AddMonths(1), baseTime.AddIntervalTicks(2, TimeInterval.Month));
        }
    }

}
