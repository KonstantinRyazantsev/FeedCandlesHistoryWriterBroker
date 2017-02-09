using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Lykke.Domain.Prices.AzureProvider.History.Model;

namespace Lykke.Domain.Prices.AzureProvider.Tests
{
    public class CandleTableEntityTests
    {

        [Fact]
        public void TestsAreCoveringAllIntervals()
        {
            // Tests are written for TimeInterval with 9 values
            Assert.Equal(9, Enum.GetValues(typeof(TimeInterval)).Cast<int>().Count());
        }

        [Fact]
        public void PropertiesParsedFromKeys()
        {
            // Month
            var entityMonth = new CandleTableEntity("BTCRUB_SELL_Month", "2017");
            Assert.Equal("BTCRUB", entityMonth.Asset);
            Assert.False(entityMonth.IsBuy);
            Assert.Equal(new DateTime(2017, 1, 1, 0, 0, 0, DateTimeKind.Utc), entityMonth.DateTime);

            // Day
            var entityDay = new CandleTableEntity("BTCRUB_BUY_Day", "2017-01");
            Assert.Equal("BTCRUB", entityDay.Asset);
            Assert.True(entityDay.IsBuy);
            Assert.Equal(new DateTime(2017, 1, 1, 0, 0, 0, DateTimeKind.Utc), entityDay.DateTime);

            // Hour
            var entityHour = new CandleTableEntity("BTCRUB_SELL_Hour", "2017-01-02");
            Assert.Equal("BTCRUB", entityHour.Asset);
            Assert.False(entityHour.IsBuy);
            Assert.Equal(new DateTime(2017, 1, 2, 0, 0, 0, DateTimeKind.Utc), entityHour.DateTime);

            // Min30
            var entityMin30 = new CandleTableEntity("BTCRUB_buy_Min30", "2017-01-02T00");
            Assert.Equal("BTCRUB", entityMin30.Asset);
            Assert.True(entityMin30.IsBuy);
            Assert.Equal(new DateTime(2017, 1, 2, 0, 0, 0, DateTimeKind.Utc), entityMin30.DateTime);

            // Min15
            var entityMin15 = new CandleTableEntity("BTCRUB_sell_Min15", "2017-01-02T01");
            Assert.Equal("BTCRUB", entityMin15.Asset);
            Assert.False(entityMin15.IsBuy);
            Assert.Equal(new DateTime(2017, 1, 2, 1, 0, 0, DateTimeKind.Utc), entityMin15.DateTime);

            // Min5
            var entityMin5 = new CandleTableEntity("BTCRUB_BUY_Min5", "2017-01-02T02");
            Assert.Equal("BTCRUB", entityMin5.Asset);
            Assert.True(entityMin5.IsBuy);
            Assert.Equal(new DateTime(2017, 1, 2, 2, 0, 0, DateTimeKind.Utc), entityMin5.DateTime);

            // Min
            var entityMinute = new CandleTableEntity("BTCRUB_SELL_Minute", "2017-01-02T03");
            Assert.Equal("BTCRUB", entityMinute.Asset);
            Assert.False(entityMinute.IsBuy);
            Assert.Equal(new DateTime(2017, 1, 2, 3, 0, 0, DateTimeKind.Utc), entityMinute.DateTime);

            // Sec
            var entitySec = new CandleTableEntity("BTCRUB_BUY_Sec", "2017-01-02T01:59");
            Assert.Equal("BTCRUB", entitySec.Asset);
            Assert.True(entitySec.IsBuy);
            Assert.Equal(new DateTime(2017, 1, 2, 1, 59, 0, DateTimeKind.Utc), entitySec.DateTime);
        }
    }
}
