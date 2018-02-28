namespace HelloToll
{
    using System;
    using System.ComponentModel;
    using System.Globalization;
    using System.Linq;
    using System.Reactive.Linq;
    using System.Reflection;

    using Microsoft.StreamProcessing;

    class Program
    {
        [DisplayName("Pass-through")]
        [Description("Pass-through query to just show input stream in the same form as we show output.")]
        static void PassThrough()
        {
            var inputStream = GetTollReadings();
            Display(inputStream);
        }

        [DisplayName("Tumbling Count")]
        [Description("Every 3 minutes, report the number of vehicles processed " +
                     "that were being processed at some point during that period at " +
                     "the toll station since the last result. Report the result at a " +
                     "point in time, at the end of the 3 minute window.")]
        static void TumblingCount()
        {
            var inputStream = GetTollReadings();
            var query = inputStream.TumblingWindowLifetime(TimeSpan.FromMinutes(3).Ticks).Count();
            Display(query);
        }

        [DisplayName("Hopping Count")]
        [Description("Report the count of vehicles being processed at some time over " +
                     "a 3 minute window, with the window moving in one minute hops. " +
                     "Provide the counts as of the last reported result as of a point " +
                     "in time, reflecting the vehicles processed over the last 3 minutes.")]
        static void HoppingCount()
        {
            var inputStream = GetTollReadings();
            var query =
                inputStream.HoppingWindowLifetime(TimeSpan.FromMinutes(3).Ticks, TimeSpan.FromMinutes(1).Ticks)
                           .Count()
                           .AlterEventDuration(1);
            Display(query);
        }

        [DisplayName("Partitioned Hopping window")]
        [Description("Find the toll generated from vehicles being processed at each " +
                     "toll station at some time over a 3 minute window, with the time advancing " +
                     "in one minute hops. Provide the value as of the last reported result.")]
        static void PartitionedHoppingWindow()
        {
            var inputStream = GetTollReadings();
            var query = inputStream.GroupApply(
                r => r.TollId,
                r =>
                r.HoppingWindowLifetime(TimeSpan.FromMinutes(3).Ticks, TimeSpan.FromMinutes(1).Ticks)
                 .Multicast(
                     perTollBoth =>
                     perTollBoth.Sum(e => e.Toll)
                                .Join(perTollBoth.Count(), (sum, count) => new { Sum = sum, Count = count })),
                (key, result) => new Toll { TollId = key.Key, TollAmount = result.Sum, VehicleCount = result.Count });
            Display(query);
        }

        [DisplayName("Partitioned Sliding window")]
        [Description("Find the most recent toll generated from vehicles being processed " +
                     "at each station over a 1 minute window reporting the result every time a " +
                     "change occurs in the input.")]
        static void PartitionedSlidingWindow()
        {
            var inputStream = GetTollReadings();
            var query =
                inputStream.AlterEventDuration((start, end) => end - start + TimeSpan.FromMinutes(1).Ticks)
                           .GroupApply(
                               r => r.TollId,
                               r =>
                               r.Multicast(
                                   perTollBoth =>
                                   perTollBoth.Sum(e => e.Toll)
                                              .Join(
                                                  perTollBoth.Count(), (sum, count) => new { Sum = sum, Count = count })),
                               (key, result) =>
                               new Toll { TollId = key.Key, TollAmount = result.Sum, VehicleCount = result.Count });
            Display(query);
        }

        [DisplayName("Partitioned Moving Average")]
        [Description("Moving average over the results of [Partitioned Sliding window].")]
        static void PartitionedMovingAverage()
        {
            var inputStream = GetTollReadings();
            var partitionedSlidingWindow =
                inputStream.AlterEventDuration((start, end) => end - start + TimeSpan.FromMinutes(1).Ticks)
                           .GroupApply(
                               r => r.TollId,
                               r =>
                               r.Multicast(
                                   perTollBoth =>
                                   perTollBoth.Sum(e => e.Toll)
                                              .Join(
                                                  perTollBoth.Count(), (sum, count) => new { Sum = sum, Count = count })),
                               (key, result) =>
                               new Toll { TollId = key.Key, TollAmount = result.Sum, VehicleCount = result.Count });
            var query =
                partitionedSlidingWindow.AlterEventDuration(1)
                                        .Select(
                                            r =>
                                            new TollAverage
                                                {
                                                    TollId = r.TollId,
                                                    AverageToll = r.TollAmount / r.VehicleCount
                                                });
            Display(query);
        }

        [DisplayName("Inner Join")]
        [Description("Report the output whenever Toll Booth 2 has processed the same number " +
                     "of vehicles as Toll Booth 1, computed over the last 1 minute, every time " +
                     "a change occurs in either stream.")]
        static void InnerJoin()
        {
            var inputStream = GetTollReadings();
            var partitionedSlidingWindow =
                inputStream.AlterEventDuration((start, end) => end - start + TimeSpan.FromMinutes(1).Ticks)
                           .GroupApply(
                               r => r.TollId,
                               r =>
                               r.Multicast(
                                   perTollBoth =>
                                   perTollBoth.Sum(e => e.Toll)
                                              .Join(
                                                  perTollBoth.Count(), (sum, count) => new { Sum = sum, Count = count })),
                               (key, result) =>
                               new Toll { TollId = key.Key, TollAmount = result.Sum, VehicleCount = result.Count });
            var stream1 = from e in partitionedSlidingWindow where e.TollId == "1" select e;
            var stream2 = from e in partitionedSlidingWindow where e.TollId == "2" select e;
            var query = from e1 in stream1
                        join e2 in stream2
                        on e1.VehicleCount equals e2.VehicleCount
                        select new TollCompare
                        {
                            TollId1 = e1.TollId,
                            TollId2 = e2.TollId,
                            VehicleCount = e1.VehicleCount
                        };
            Display(query);
        }

        [DisplayName("Cross Join")]
        [Description("Variation of [Inner Join] with cross join instead.")]
        static void CrossJoin()
        {
            var inputStream = GetTollReadings();
            var partitionedSlidingWindow =
                inputStream.AlterEventDuration((start, end) => end - start + TimeSpan.FromMinutes(1).Ticks)
                           .GroupApply(
                               r => r.TollId,
                               r =>
                               r.Multicast(
                                   perTollBoth =>
                                   perTollBoth.Sum(e => e.Toll)
                                              .Join(
                                                  perTollBoth.Count(), (sum, count) => new { Sum = sum, Count = count })),
                               (key, result) =>
                               new Toll { TollId = key.Key, TollAmount = result.Sum, VehicleCount = result.Count });
            var stream1 = from e in partitionedSlidingWindow where e.TollId == "1" select e;
            var stream2 = from e in partitionedSlidingWindow where e.TollId == "2" select e;
            var query = stream1.Join(
                stream2,
                (e1, e2) => new TollCompare { TollId1 = e1.TollId, TollId2 = e2.TollId, VehicleCount = e1.VehicleCount });
            Display(query);
        }

        [DisplayName("Left Anti Join")]
        [Description("Report toll violators – owners of vehicles that pass through an automated " +
                     "toll booth without a valid EZ-Pass tag read.")]
        static void LeftAntiJoin()
        {
            var inputStream = GetTollReadings();

            // Simulate the reference stream from inputStream itself - convert it to a point event stream
            var referenceStream = inputStream.AlterEventDuration(1);

            // Simulate the tag violations in the observed stream by filtering out specific
            // vehicles. Let us filter out all the events in the dataset with a Tag length of 0.
            // In a real scenario, these events will not exist at all – this simulation is only
            // because we are reusing the same input stream for this example.
            // The events that were filtered out should be the ones that show up in the output of Q7.
            var observedStream = inputStream.AlterEventDuration(1).Where(r => r.Tag.Length != 0);

            // Report tag violations
            var query = referenceStream.WhereNotExists(observedStream);
            Display(query);
        }

        [DisplayName("Outer Join")]
        [Description("Outer Join using query primitives.")]
        static void OuterJoin()
        {
            var inputStream = GetTollReadings();

            // Simulate the left stream input from inputStream
            var outerJoin_L =
                inputStream.Select(e => new { LicensePlate = e.LicensePlate, Make = e.Make, Model = e.Model, });

            // Simulate the right stream input from inputStream – eliminate all events with Toyota as the vehicle
            // These should be the rows in the outer joined result with NULL values for Toll and LicensePlate
            var outerJoin_R =
                inputStream.Where(e => e.Make != "Toyota")
                           .Select(e => new { LicensePlate = e.LicensePlate, Toll = e.Toll, TollId = e.TollId });

            // Inner join the two simulated input streams
            var innerJoin = outerJoin_L.Join(
                outerJoin_R,
                l => l.LicensePlate,
                r => r.LicensePlate,
                (l, r) =>
                new TollOuterJoin
                    {
                        LicensePlate = l.LicensePlate,
                        Make = l.Make,
                        Model = l.Model,
                        Toll = r.Toll,
                        TollId = r.TollId
                    });

            // Left anti join the two input simulated streams, and add the Project
            var leftAntiJoin = outerJoin_L
                .WhereNotExists(outerJoin_R, left => left.LicensePlate, right => right.LicensePlate)
                .Select(left => new TollOuterJoin
                {
                    LicensePlate = left.LicensePlate,
                    Make = left.Make,
                    Model = left.Model,
                    Toll = null,
                    TollId = null
                });

            // Union the two streams to complete a Left Outer Join operation
            var query = innerJoin.Union(leftAntiJoin);
            Display(query);
        }

        [DisplayName("UDF")]
        [Description("For each vehicle that is being processed at an EZ-Pass booth, report " +
                     "the TollReading if the tag does not exist, has expired, or is reported stolen.")]
        static void UDF()
        {
            var inputStream = GetTollReadings();
            var query =
                inputStream.Where(r => 0 == r.Tag.Length || TagInfo.IsLostOrStolen(r.Tag) || TagInfo.IsExpired(r.Tag))
                           .Select(
                               r =>
                               new TollViolation
                                   {
                                       LicensePlate = r.LicensePlate,
                                       Make = r.Make,
                                       Model = r.Model,
                                       State = r.State,
                                       Tag = r.Tag,
                                       TollId = r.TollId
                                   });
            Display(query);
        }

        /// <summary>
        /// Defines a stream of TollReadings used throughout examples. A simulated data array
        /// is wrapped into the Enumerable source which is then converted to a stream. 
        /// </summary>
        /// <returns>Returns stream of simulated TollReadings used in examples.</returns>
        static IStreamable<Empty, TollReading> GetTollReadings()
        {
            return // Simulated readings data defined as an array.
                new[]
                    {
                        StreamEvent.CreateInterval(
                            GetDateTimeTicks(2009, 06, 25, 12, 01, 0),
                            GetDateTimeTicks(2009, 06, 25, 12, 03, 0),
                            new TollReading
                                {
                                    TollId = "1",
                                    LicensePlate = "JNB 7001",
                                    State = "NY",
                                    Make = "Honda",
                                    Model = "CRV",
                                    VehicleType = 1,
                                    VehicleWeight = 0,
                                    Toll = 7.0f,
                                    Tag = ""
                                }),
                        StreamEvent.CreateInterval(
                            GetDateTimeTicks(2009, 06, 25, 12, 02, 0),
                            GetDateTimeTicks(2009, 06, 25, 12, 03, 0),
                            new TollReading
                                {
                                    TollId = "1",
                                    LicensePlate = "YXZ 1001",
                                    State = "NY",
                                    Make = "Toyota",
                                    Model = "Camry",
                                    VehicleType = 1,
                                    VehicleWeight = 0,
                                    Toll = 4.0f,
                                    Tag = "123456789"
                                }),
                        StreamEvent.CreateInterval(
                            GetDateTimeTicks(2009, 06, 25, 12, 02, 0),
                            GetDateTimeTicks(2009, 06, 25, 12, 04, 0),
                            new TollReading
                                {
                                    TollId = "3",
                                    LicensePlate = "ABC 1004",
                                    State = "CT",
                                    Make = "Ford",
                                    Model = "Taurus",
                                    VehicleType = 1,
                                    VehicleWeight = 0,
                                    Toll = 5.0f,
                                    Tag = "456789123"
                                }),
                        StreamEvent.CreateInterval(
                            GetDateTimeTicks(2009, 06, 25, 12, 03, 0),
                            GetDateTimeTicks(2009, 06, 25, 12, 07, 0),
                            new TollReading
                                {
                                    TollId = "2",
                                    LicensePlate = "XYZ 1003",
                                    State = "CT",
                                    Make = "Toyota",
                                    Model = "Corolla",
                                    VehicleType = 1,
                                    VehicleWeight = 0,
                                    Toll = 4.0f,
                                    Tag = ""
                                }),
                        StreamEvent.CreateInterval(
                            GetDateTimeTicks(2009, 06, 25, 12, 03, 0),
                            GetDateTimeTicks(2009, 06, 25, 12, 08, 0),
                            new TollReading
                                {
                                    TollId = "1",
                                    LicensePlate = "BNJ 1007",
                                    State = "NY",
                                    Make = "Honda",
                                    Model = "CRV",
                                    VehicleType = 1,
                                    VehicleWeight = 0,
                                    Toll = 5.0f,
                                    Tag = "789123456"
                                }),
                        StreamEvent.CreateInterval(
                            GetDateTimeTicks(2009, 06, 25, 12, 05, 0),
                            GetDateTimeTicks(2009, 06, 25, 12, 07, 0),
                            new TollReading
                                {
                                    TollId = "2",
                                    LicensePlate = "CDE 1007",
                                    State = "NJ",
                                    Make = "Toyota",
                                    Model = "4x4",
                                    VehicleType = 1,
                                    VehicleWeight = 0,
                                    Toll = 6.0f,
                                    Tag = "321987654"
                                }),
                        StreamEvent.CreateInterval(
                            GetDateTimeTicks(2009, 06, 25, 12, 06, 0),
                            GetDateTimeTicks(2009, 06, 25, 12, 09, 0),
                            new TollReading
                                {
                                    TollId = "2",
                                    LicensePlate = "BAC 1005",
                                    State = "NY",
                                    Make = "Toyota",
                                    Model = "Camry",
                                    VehicleType = 1,
                                    VehicleWeight = 0,
                                    Toll = 5.5f,
                                    Tag = "567891234"
                                }),
                        StreamEvent.CreateInterval(
                            GetDateTimeTicks(2009, 06, 25, 12, 07, 0),
                            GetDateTimeTicks(2009, 06, 25, 12, 10, 0),
                            new TollReading
                                {
                                    TollId = "1",
                                    LicensePlate = "ZYX 1002",
                                    State = "NY",
                                    Make = "Honda",
                                    Model = "Accord",
                                    VehicleType = 1,
                                    VehicleWeight = 0,
                                    Toll = 6.0f,
                                    Tag = "234567891"
                                }),
                        StreamEvent.CreateInterval(
                            GetDateTimeTicks(2009, 06, 25, 12, 07, 0),
                            GetDateTimeTicks(2009, 06, 25, 12, 10, 0),
                            new TollReading
                                {
                                    TollId = "2",
                                    LicensePlate = "ZXY 1001",
                                    State = "PA",
                                    Make = "Toyota",
                                    Model = "Camry",
                                    VehicleType = 1,
                                    VehicleWeight = 0,
                                    Toll = 4.0f,
                                    Tag = "987654321"
                                }),
                        StreamEvent.CreateInterval(
                            GetDateTimeTicks(2009, 06, 25, 12, 08, 0),
                            GetDateTimeTicks(2009, 06, 25, 12, 10, 0),
                            new TollReading
                                {
                                    TollId = "3",
                                    LicensePlate = "CBA 1008",
                                    State = "PA",
                                    Make = "Ford",
                                    Model = "Mustang",
                                    VehicleType = 1,
                                    VehicleWeight = 0,
                                    Toll = 4.5f,
                                    Tag = "891234567"
                                }),
                        StreamEvent.CreateInterval(
                            GetDateTimeTicks(2009, 06, 25, 12, 09, 0),
                            GetDateTimeTicks(2009, 06, 25, 12, 11, 0),
                            new TollReading
                                {
                                    TollId = "2",
                                    LicensePlate = "DCB 1004",
                                    State = "NY",
                                    Make = "Volvo",
                                    Model = "S80",
                                    VehicleType = 1,
                                    VehicleWeight = 0,
                                    Toll = 5.5f,
                                    Tag = "654321987"
                                }),
                        StreamEvent.CreateInterval(
                            GetDateTimeTicks(2009, 06, 25, 12, 09, 0),
                            GetDateTimeTicks(2009, 06, 25, 12, 16, 0),
                            new TollReading
                                {
                                    TollId = "2",
                                    LicensePlate = "CDB 1003",
                                    State = "PA",
                                    Make = "Volvo",
                                    Model = "C30",
                                    VehicleType = 1,
                                    VehicleWeight = 0,
                                    Toll = 5.0f,
                                    Tag = "765432198"
                                }),
                        StreamEvent.CreateInterval(
                            GetDateTimeTicks(2009, 06, 25, 12, 09, 0),
                            GetDateTimeTicks(2009, 06, 25, 12, 10, 0),
                            new TollReading
                                {
                                    TollId = "3",
                                    LicensePlate = "YZX 1009",
                                    State = "NY",
                                    Make = "Volvo",
                                    Model = "V70",
                                    VehicleType = 1,
                                    VehicleWeight = 0,
                                    Toll = 4.5f,
                                    Tag = "912345678"
                                }),
                        StreamEvent.CreateInterval(
                            GetDateTimeTicks(2009, 06, 25, 12, 10, 0),
                            GetDateTimeTicks(2009, 06, 25, 12, 12, 0),
                            new TollReading
                                {
                                    TollId = "3",
                                    LicensePlate = "BCD 1002",
                                    State = "NY",
                                    Make = "Toyota",
                                    Model = "Rav4",
                                    VehicleType = 1,
                                    VehicleWeight = 0,
                                    Toll = 5.5f,
                                    Tag = "876543219"
                                }),
                        StreamEvent.CreateInterval(
                            GetDateTimeTicks(2009, 06, 25, 12, 10, 0),
                            GetDateTimeTicks(2009, 06, 25, 12, 14, 0),
                            new TollReading
                                {
                                    TollId = "1",
                                    LicensePlate = "CBD 1005",
                                    State = "NY",
                                    Make = "Toyota",
                                    Model = "Camry",
                                    VehicleType = 1,
                                    VehicleWeight = 0,
                                    Toll = 4.0f,
                                    Tag = "543219876"
                                }),
                        StreamEvent.CreateInterval(
                            GetDateTimeTicks(2009, 06, 25, 12, 11, 0),
                            GetDateTimeTicks(2009, 06, 25, 12, 13, 0),
                            new TollReading
                                {
                                    TollId = "1",
                                    LicensePlate = "NJB 1006",
                                    State = "CT",
                                    Make = "Ford",
                                    Model = "Focus",
                                    VehicleType = 1,
                                    VehicleWeight = 0,
                                    Toll = 4.5f,
                                    Tag = "678912345"
                                }),
                        StreamEvent.CreateInterval(
                            GetDateTimeTicks(2009, 06, 25, 12, 12, 0),
                            GetDateTimeTicks(2009, 06, 25, 12, 15, 0),
                            new TollReading
                                {
                                    TollId = "3",
                                    LicensePlate = "PAC 1209",
                                    State = "NJ",
                                    Make = "Chevy",
                                    Model = "Malibu",
                                    VehicleType = 1,
                                    VehicleWeight = 0,
                                    Toll = 6.0f,
                                    Tag = "219876543"
                                }),
                        StreamEvent.CreateInterval(
                            GetDateTimeTicks(2009, 06, 25, 12, 15, 0),
                            GetDateTimeTicks(2009, 06, 25, 12, 22, 0),
                            new TollReading
                                {
                                    TollId = "2",
                                    LicensePlate = "BAC 1005",
                                    State = "PA",
                                    Make = "Peterbilt",
                                    Model = "389",
                                    VehicleType = 2,
                                    VehicleWeight = 2.675f,
                                    Toll = 15.5f,
                                    Tag = "567891234"
                                }),
                        StreamEvent.CreateInterval(
                            GetDateTimeTicks(2009, 06, 25, 12, 15, 0),
                            GetDateTimeTicks(2009, 06, 25, 12, 18, 0),
                            new TollReading
                                {
                                    TollId = "3",
                                    LicensePlate = "EDC 3109",
                                    State = "NJ",
                                    Make = "Ford",
                                    Model = "Focus",
                                    VehicleType = 1,
                                    VehicleWeight = 0,
                                    Toll = 4.0f,
                                    Tag = "198765432"
                                }),
                        StreamEvent.CreateInterval(
                            GetDateTimeTicks(2009, 06, 25, 12, 18, 0),
                            GetDateTimeTicks(2009, 06, 25, 12, 20, 0),
                            new TollReading
                                {
                                    TollId = "2",
                                    LicensePlate = "DEC 1008",
                                    State = "NY",
                                    Make = "Toyota",
                                    Model = "Corolla",
                                    VehicleType = 1,
                                    VehicleWeight = 0,
                                    Toll = 4.0f,
                                    Tag = ""
                                }),
                        StreamEvent.CreateInterval(
                            GetDateTimeTicks(2009, 06, 25, 12, 20, 0),
                            GetDateTimeTicks(2009, 06, 25, 12, 22, 0),
                            new TollReading
                                {
                                    TollId = "1",
                                    LicensePlate = "DBC 1006",
                                    State = "NY",
                                    Make = "Honda",
                                    Model = "Civic",
                                    VehicleType = 1,
                                    VehicleWeight = 0,
                                    Toll = 5.0f,
                                    Tag = "432198765"
                                }),
                        StreamEvent.CreateInterval(
                            GetDateTimeTicks(2009, 06, 25, 12, 20, 0),
                            GetDateTimeTicks(2009, 06, 25, 12, 23, 0),
                            new TollReading
                                {
                                    TollId = "2",
                                    LicensePlate = "APC 2019",
                                    State = "NJ",
                                    Make = "Honda",
                                    Model = "Civic",
                                    VehicleType = 1,
                                    VehicleWeight = 0,
                                    Toll = 4.0f,
                                    Tag = "345678912"
                                }),
                        StreamEvent.CreateInterval(
                            GetDateTimeTicks(2009, 06, 25, 12, 22, 0),
                            GetDateTimeTicks(2009, 06, 25, 12, 25, 0),
                            new TollReading
                                {
                                    TollId = "1",
                                    LicensePlate = "EDC 1019",
                                    State = "NJ",
                                    Make = "Honda",
                                    Model = "Accord",
                                    VehicleType = 1,
                                    VehicleWeight = 0,
                                    Toll = 4.0f,
                                    Tag = ""
                                }),
                        StreamEvent.CreatePunctuation<TollReading>(StreamEvent.InfinitySyncTime)
                    }.ToObservable().ToStreamable(OnCompletedPolicy.None());
        }

        private static long GetDateTimeTicks(int year, int month, int day, int hour, int minute, int second)
        {
            return new DateTime(year, month, day, hour, minute, second, DateTimeKind.Utc).Ticks;
        }

        private static void Display<T>(IStreamable<Empty, T> stream)
        {
            stream.ToStreamEventObservable().ForEachAsync(
                r =>
                    {
                        switch (r.Kind)
                        {
                            case StreamEventKind.Interval:
                                Console.WriteLine(
                                    "INTERVAL:    start={0}, end={1}, payload={2}", new DateTime(r.SyncTime), new DateTime(r.OtherTime), r.Payload);
                                break;
                            case StreamEventKind.Start:
                                Console.WriteLine("START EDGE:  start={0}, payload={1}", new DateTime(r.SyncTime), r.Payload);
                                break;
                            case StreamEventKind.End:
                                Console.WriteLine(
                                    "END EDGE:    end={0}, original start={1}, payload={2}",
                                    new DateTime(r.SyncTime),
                                    new DateTime(r.OtherTime),
                                    r.Payload);
                                break;
                            case StreamEventKind.Punctuation:
                                Console.WriteLine("PUNCTUATION: start={0}", new DateTime(r.SyncTime));
                                break;
                        }
                    }).Wait();
        }

        static void Main(string[] args)
        {
            var demos = (from mi in typeof(Program).GetMethods(BindingFlags.Static | BindingFlags.NonPublic)
                         let nameAttr = mi.GetCustomAttributes(typeof(DisplayNameAttribute), false)
                             .OfType<DisplayNameAttribute>()
                             .SingleOrDefault()
                         let descriptionAttr = mi.GetCustomAttributes(typeof(DescriptionAttribute), false)
                             .OfType<DescriptionAttribute>()
                             .SingleOrDefault()
                         where null != nameAttr
                         select new { Action = mi, Name = nameAttr.DisplayName, Description = descriptionAttr.Description }).ToArray();

            while (true)
            {
                Console.WriteLine();
                Console.WriteLine("Pick an action:");
                for (int demo = 0; demo < demos.Length; demo++)
                {
                    Console.WriteLine("{0,4} - {1}", demo, demos[demo].Name);
                }

                Console.WriteLine("Exit - Exit from Demo.");
                var response = Console.ReadLine().Trim();
                if (string.Equals(response, "exit", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(response, "e", StringComparison.OrdinalIgnoreCase))
                {
                    break;
                }

                int demoToRun;
                demoToRun = Int32.TryParse(response, NumberStyles.Integer, CultureInfo.InvariantCulture, out demoToRun)
                    ? demoToRun
                    : -1;

                if (0 <= demoToRun && demoToRun < demos.Length)
                {
                    Console.WriteLine();
                    Console.WriteLine(demos[demoToRun].Name);
                    Console.WriteLine(demos[demoToRun].Description);
                    demos[demoToRun].Action.Invoke(null, null);
                }
                else
                {
                    Console.WriteLine("Unknown Query Demo");
                }
            }
        }
    }
}
