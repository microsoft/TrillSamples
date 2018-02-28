using System;
using System.Linq;
using System.Reactive.Linq;

using Microsoft.StreamProcessing;


namespace ToyExample
{
    class Program
    {
        public struct MyStruct
        {
            public int field1;
            public double field2;
            public string field3;

            public override string ToString()
            {
                return "" + field1 + "\t" + field2 + "\t" + field3;
            }
        }

        static void Main(string[] args)
        {
            // ingress data
            var input =
                Observable
                    .Range(0, 100)
                    .Select(e => new MyStruct { field1 = e % 10, field2 = e + 0.5, field3 = "blah" })
                    .Select(e => StreamEvent.CreateStart(StreamEvent.MinSyncTime, e))
                    .ToStreamable(OnCompletedPolicy.EndOfStream(), PeriodicPunctuationPolicy.None())
                    .Cache();

            // query
            var query =
                input.SetProperty().IsConstantDuration(true)
                     .GroupBy(e => e.field2)
                     .SelectMany(str => str.Distinct(f => f.field1), (g, c) => c);

            // egress data
            query
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .ForEachAsync(e => Console.WriteLine("{0}\t{1}\t{2}", e.SyncTime, e.OtherTime, e.Payload)).Wait();

            Console.ReadLine();
        }
    }
}
