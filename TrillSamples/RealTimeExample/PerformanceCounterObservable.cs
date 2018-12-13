// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics;
using System.Threading;

namespace RealTimeExample
{
    /// <summary>
    /// Struct that holds the actual performance counter
    /// </summary>
    internal struct PerformanceCounterSample
    {
        public DateTime StartTime;
        public float Value;

        public override string ToString()
        {
            return new { this.StartTime, this.Value }.ToString();
        }
    }

    /// <summary>
    /// An observable source based on a local machine performance counter.
    /// </summary>
    internal sealed class PerformanceCounterObservable : IObservable<PerformanceCounterSample>
    {
        private readonly Func<PerformanceCounter> createCounter;
        private readonly TimeSpan pollingInterval;

        public PerformanceCounterObservable(
            string categoryName,
            string counterName,
            string instanceName,
            TimeSpan pollingInterval)
        {
            // create a new performance counter for every subscription
            this.createCounter = () => new PerformanceCounter(categoryName, counterName, instanceName, true);
            this.pollingInterval = pollingInterval;
        }

        public IDisposable Subscribe(IObserver<PerformanceCounterSample> observer)
        {
            return new Subscription(this, observer);
        }

        private sealed class Subscription : IDisposable
        {
            private readonly PerformanceCounter counter;
            private readonly TimeSpan pollingInterval;
            private readonly IObserver<PerformanceCounterSample> observer;
            private readonly Timer timer;
            private readonly object sync = new object();
            private CounterSample previousSample;
            private bool isDisposed;

            public Subscription(PerformanceCounterObservable observable, IObserver<PerformanceCounterSample> observer)
            {
                // create a new counter for this subscription
                this.counter = observable.createCounter();
                this.pollingInterval = observable.pollingInterval;
                this.observer = observer;

                // seed previous sample to support computation
                this.previousSample = this.counter.NextSample();

                // create a timer to support polling counter at an interval
                this.timer = new Timer(Sample);
                this.timer.Change(this.pollingInterval.Milliseconds, -1);
            }

            private void Sample(object state)
            {
                lock (this.sync)
                {
                    if (!this.isDisposed)
                    {
                        var startTime = DateTime.UtcNow;
                        CounterSample currentSample = this.counter.NextSample();
                        float value = CounterSample.Calculate(this.previousSample, currentSample);
                        this.observer.OnNext(new PerformanceCounterSample { StartTime = startTime, Value = value });
                        this.previousSample = currentSample;
                        this.timer.Change(this.pollingInterval.Milliseconds, -1);
                    }
                }
            }

            public void Dispose()
            {
                lock (this.sync)
                {
                    if (!this.isDisposed)
                    {
                        this.isDisposed = true;
                        this.timer.Dispose();
                        this.counter.Dispose();
                    }
                }
            }
        }
    }
}
