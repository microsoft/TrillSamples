// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;

namespace HelloWorld
{
    // Input events to imitate sensor readings
    internal class SensorReading
    {
        public int Time { get; set; }

        public int Value { get; set; }

        public override string ToString() => new { this.Time, this.Value }.ToString();

        public override bool Equals(object obj) =>
            obj is SensorReading other && this.Time == other.Time && this.Value == other.Value;

        public override int GetHashCode() => this.Time.GetHashCode() ^ this.Value.GetHashCode();
    }

    public class Program
    {
        public static void Main(string[] args)
        {
            // We will be building a query that takes a stream of SensorReading events.
            // It will work the same way on real-time data or past recorded events.
            Console.WriteLine("Press L for Live or H for Historic Data");
            ConsoleKeyInfo key = Console.ReadKey();
            Console.WriteLine();

            IStreamable<Empty, SensorReading> inputStream;
            if (key.Key == ConsoleKey.L)
            {
                inputStream = CreateStream(true);
            }
            else if (key.Key == ConsoleKey.H)
            {
                inputStream = CreateStream(false);
            }
            else
            {
                Console.WriteLine("invalid key");
                return;
            }

            // The query is detecting when a threshold is crossed upwards.
            const int threshold = 42;

            var crossedThreshold = inputStream.Multicast(
                input =>
                    {
                        // Alter all events 1 sec in the future.
                        var alteredForward = input.AlterEventLifetime(s => s + 1, 1);

                        // Compare each event that occurs at input with the previous event.
                        // Note that, this one works for strictly ordered, strictly (e.g 1 sec) regular streams.
                        var filteredInputStream = input.Where(s => s.Value > threshold);
                        var filteredAlteredStream = alteredForward.Where(s => s.Value < threshold);
                        return filteredInputStream.Join(
                            filteredAlteredStream,
                            (evt, prev) => new { evt.Time, Low = prev.Value, High = evt.Value });
                    });

            crossedThreshold.ToStreamEventObservable().ForEachAsync(r => Console.WriteLine(r)).Wait();

            Console.WriteLine("Done. Press ENTER to terminate");
            Console.ReadLine();
        }

        private static readonly SensorReading[] HistoricData = new[]
        {
            new SensorReading { Time = 1, Value = 0 },
            new SensorReading { Time = 2, Value = 20 },
            new SensorReading { Time = 3, Value = 15 },
            new SensorReading { Time = 4, Value = 30 },
            new SensorReading { Time = 5, Value = 45 }, // Here we crossed the threshold upward
            new SensorReading { Time = 6, Value = 50 },
            new SensorReading { Time = 7, Value = 30 }, // Here we crossed downward. Note that the current query logic only detects upward swings.
            new SensorReading { Time = 8, Value = 35 },
            new SensorReading { Time = 9, Value = 60 }, // Here we crossed upward again
            new SensorReading { Time = 10, Value = 20 }
        };

        private static IObservable<SensorReading> SimulateLiveData()
        {
            return ToObservableInterval(HistoricData, TimeSpan.FromMilliseconds(1000));
        }

        private static IObservable<T> ToObservableInterval<T>(IEnumerable<T> source, TimeSpan period)
        {
            return Observable.Using(
                source.GetEnumerator,
                it => Observable.Generate(
                    default(object),
                    _ => it.MoveNext(),
                    _ => _,
                    _ =>
                    {
                        Console.WriteLine("Input {0}", it.Current);
                        return it.Current;
                    },
                    _ => period));
        }

        private static IStreamable<Empty, SensorReading> CreateStream(bool isRealTime)
        {
            if (isRealTime)
            {
                return SimulateLiveData()
                        .Select(r => StreamEvent.CreateInterval(r.Time, r.Time + 1, r))
                        .ToStreamable();
            }

            return HistoricData
                .ToObservable()
                .Select(r => StreamEvent.CreateInterval(r.Time, r.Time + 1, r))
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None);
        }
    }
}
