using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Lykke.Domain.Prices;

namespace CandlesWriter.Core.Tests
{
    public class DateTimeExtensionTests
    {
        public DateTimeExtensionTests()
        {
        }

        [Fact]
        public void TestsAreCoveringAllIntervals()
        {
            // Tests are written for TimeInterval with 13 values
            Assert.Equal(13, Enum.GetValues(typeof(TimeInterval)).Cast<int>().Count());
        }

        [Fact]
        public void BasicRoundChecks()
        {
            DateTime initialDt = new DateTime(2017, 2, 8, 11, 12, 13, 111);

            // Sec
            Assert.Equal(new DateTime(2017, 2, 8, 11, 12, 13, 0), initialDt.RoundTo(TimeInterval.Sec));
            // Minute
            Assert.Equal(new DateTime(2017, 2, 8, 11, 12, 0), initialDt.RoundTo(TimeInterval.Minute));
            // Min5
            Assert.Equal(new DateTime(2017, 2, 8, 11, 0, 0),
                         new DateTime(2017, 2, 8, 11, 0, 13, 111).RoundTo(TimeInterval.Min5));
            Assert.Equal(new DateTime(2017, 2, 8, 11, 0, 0),
                         new DateTime(2017, 2, 8, 11, 1, 13, 111).RoundTo(TimeInterval.Min5));
            Assert.Equal(new DateTime(2017, 2, 8, 11, 10, 0),
                         new DateTime(2017, 2, 8, 11, 12, 13, 111).RoundTo(TimeInterval.Min5));
            // Min15
            Assert.Equal(new DateTime(2017, 2, 8, 11, 00, 0),
                         new DateTime(2017, 2, 8, 11, 12, 13, 111).RoundTo(TimeInterval.Min15));
            Assert.Equal(new DateTime(2017, 2, 8, 11, 45, 0),
                         new DateTime(2017, 2, 8, 11, 50, 13, 111).RoundTo(TimeInterval.Min15));
            // Min30
            Assert.Equal(new DateTime(2017, 2, 8, 11, 00, 0),
                         new DateTime(2017, 2, 8, 11, 12, 13, 111).RoundTo(TimeInterval.Min30));
            Assert.Equal(new DateTime(2017, 2, 8, 11, 30, 0),
                         new DateTime(2017, 2, 8, 11, 50, 13, 111).RoundTo(TimeInterval.Min30));
            // Hour
            Assert.Equal(new DateTime(2017, 2, 8, 11, 0, 0),
                         new DateTime(2017, 2, 8, 11, 50, 13, 111).RoundTo(TimeInterval.Hour));
            // Hour4
            Assert.Equal(new DateTime(2017, 2, 8, 8, 0, 0),
                         new DateTime(2017, 2, 8, 11, 50, 13, 111).RoundTo(TimeInterval.Hour4));
            Assert.Equal(new DateTime(2017, 2, 8, 0, 0, 0),
                         new DateTime(2017, 2, 8, 1, 50, 13, 111).RoundTo(TimeInterval.Hour4));
            // Hour6
            Assert.Equal(new DateTime(2017, 2, 8, 6, 0, 0),
                         new DateTime(2017, 2, 8, 11, 50, 13, 111).RoundTo(TimeInterval.Hour6));
            Assert.Equal(new DateTime(2017, 2, 8, 18, 0, 0),
                         new DateTime(2017, 2, 8, 23, 50, 13, 111).RoundTo(TimeInterval.Hour6));
            // Hour12
            Assert.Equal(new DateTime(2017, 2, 8, 0, 0, 0),
                         new DateTime(2017, 2, 8, 11, 50, 13, 111).RoundTo(TimeInterval.Hour12));
            Assert.Equal(new DateTime(2017, 2, 8, 12, 0, 0),
                         new DateTime(2017, 2, 8, 21, 50, 13, 111).RoundTo(TimeInterval.Hour12));
            // Day
            Assert.Equal(new DateTime(2017, 2, 8, 0, 0, 0),
                         new DateTime(2017, 2, 8, 11, 50, 13, 111).RoundTo(TimeInterval.Day));
            // Week
            Assert.Equal(new DateTime(2016, 12, 19, 0, 0, 0),
                         new DateTime(2016, 12, 25, 11, 50, 13, 111).RoundTo(TimeInterval.Week));
            Assert.Equal(new DateTime(2016, 12, 26, 0, 0, 0),
                         new DateTime(2016, 12, 26, 11, 50, 13, 111).RoundTo(TimeInterval.Week));
            Assert.Equal(new DateTime(2016, 12, 26, 0, 0, 0),
                         new DateTime(2017, 1, 1, 11, 50, 13, 111).RoundTo(TimeInterval.Week));
            Assert.Equal(new DateTime(2017, 1, 2, 0, 0, 0),
                         new DateTime(2017, 1, 2, 11, 50, 13, 111).RoundTo(TimeInterval.Week));
            Assert.Equal(new DateTime(2017, 1, 2, 0, 0, 0),
                         new DateTime(2017, 1, 3, 11, 50, 13, 111).RoundTo(TimeInterval.Week));
            Assert.Equal(new DateTime(2017, 1, 30, 0, 0, 0),
                         new DateTime(2017, 2, 1, 11, 50, 13, 111).RoundTo(TimeInterval.Week));
            // Month
            Assert.Equal(new DateTime(2017, 2, 1, 0, 0, 0),
                         new DateTime(2017, 2, 8, 11, 50, 13, 111).RoundTo(TimeInterval.Month));
        }

        [Fact]
        public void RoundToPreservesKind()
        {
            DateTime dtUtc = new DateTime(2017, 2, 8, 11, 12, 13, 111, DateTimeKind.Utc);
            DateTime dtLocal = new DateTime(2017, 2, 8, 11, 12, 13, 111, DateTimeKind.Local);
            DateTime dtUnspecified = new DateTime(2017, 2, 8, 11, 12, 13, 111, DateTimeKind.Unspecified);

            var enumValues = Enum.GetValues(typeof(TimeInterval));

            foreach(var enumValue in enumValues)
            {
                if ((TimeInterval)enumValue != TimeInterval.Unspecified) {
                    Assert.Equal(DateTimeKind.Utc, dtUtc.RoundTo((TimeInterval)enumValue).Kind);
                    Assert.Equal(DateTimeKind.Local, dtLocal.RoundTo((TimeInterval)enumValue).Kind);
                    Assert.Equal(DateTimeKind.Unspecified, dtUnspecified.RoundTo((TimeInterval)enumValue).Kind);
                }
            }
        }
    }
}
