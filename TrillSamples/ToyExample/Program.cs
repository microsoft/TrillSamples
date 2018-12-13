// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;

namespace ToyExample
{
    public class Program
    {
        public struct MyStruct
        {
            public int field1;
            public double field2;
            public string field3;

            public override string ToString()
            {
                return this.field1 + "\t" + this.field2 + "\t" + this.field3;
            }
        }

        public static void Main(string[] args)
        {
            // ingress data
            var input =
                Observable
                    .Range(0, 100)
                    .Select(e => new MyStruct { field1 = e % 10, field2 = e + 0.5, field3 = "blah" })
                    .Select(e => StreamEvent.CreateStart(StreamEvent.MinSyncTime, e))
                    .ToStreamable()
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
                .ForEachAsync(e => Console.WriteLine("{0}\t{1}\t{2}", e.StartTime, e.EndTime, e.Payload)).Wait();

            Console.ReadLine();
        }
    }
}
