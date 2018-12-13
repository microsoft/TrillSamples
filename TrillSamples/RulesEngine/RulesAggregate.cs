// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Microsoft.StreamProcessing;
using Microsoft.StreamProcessing.Aggregates;

namespace RulesEngine
{
    internal sealed class RulesAggregate<TRulesKey, TData, TOutput> :
        IAggregate<ValueTuple<TRulesKey, TData>, ValueTuple<TRulesKey, SortedMultiSet<TData>>, IEnumerable<ValueTuple<string, TOutput>>>
    {
        private readonly Func<TRulesKey, SortedMultiSet<TData>, IEnumerable<ValueTuple<string, TOutput>>> func;

        public RulesAggregate(Func<TRulesKey, SortedMultiSet<TData>, IEnumerable<ValueTuple<string, TOutput>>> func)
            => this.func = func;

        public Expression<Func<
            ValueTuple<TRulesKey, SortedMultiSet<TData>>,
            long,
            ValueTuple<TRulesKey, TData>,
            ValueTuple<TRulesKey, SortedMultiSet<TData>>>> Accumulate()
            => (state, time, input) => ValueTuple.Create(input.Item1, state.Item2.Add(input.Item2));

        public Expression<Func<
            ValueTuple<TRulesKey, SortedMultiSet<TData>>,
            IEnumerable<ValueTuple<string, TOutput>>>> ComputeResult()
            => (state) => this.func(state.Item1, state.Item2);

        public Expression<Func<
            ValueTuple<TRulesKey, SortedMultiSet<TData>>,
            long,
            ValueTuple<TRulesKey, TData>,
            ValueTuple<TRulesKey, SortedMultiSet<TData>>>> Deaccumulate()
            => (state, time, input) => ValueTuple.Create(input.Item1, state.Item2.Remove(input.Item2));

        public Expression<Func<
            ValueTuple<TRulesKey, SortedMultiSet<TData>>,
            ValueTuple<TRulesKey, SortedMultiSet<TData>>,
            ValueTuple<TRulesKey, SortedMultiSet<TData>>>> Difference()
            => (left, right) => ValueTuple.Create(left.Item1, left.Item2.RemoveAll(right.Item2));

        public Expression<Func<
            ValueTuple<TRulesKey, SortedMultiSet<TData>>>> InitialState()
            => () => ValueTuple.Create(default(TRulesKey), new SortedMultiSet<TData>());
    }
}
