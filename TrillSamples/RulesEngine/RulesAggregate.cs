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
        IAggregate<Tuple<TRulesKey, TData>, Tuple<TRulesKey, SortedMultiSet<TData>>, IEnumerable<Tuple<string, TOutput>>>
    {
        private readonly Func<TRulesKey, SortedMultiSet<TData>, IEnumerable<Tuple<string, TOutput>>> func;

        public RulesAggregate(Func<TRulesKey, SortedMultiSet<TData>, IEnumerable<Tuple<string, TOutput>>> func)
            => this.func = func;

        public Expression<Func<
            Tuple<TRulesKey, SortedMultiSet<TData>>,
            long,
            Tuple<TRulesKey, TData>,
            Tuple<TRulesKey, SortedMultiSet<TData>>>> Accumulate()
            => (state, time, input) => Tuple.Create(input.Item1, state.Item2.Add(input.Item2));

        public Expression<Func<
            Tuple<TRulesKey, SortedMultiSet<TData>>,
            IEnumerable<Tuple<string, TOutput>>>> ComputeResult()
            => (state) => this.func(state.Item1, state.Item2);

        public Expression<Func<
            Tuple<TRulesKey, SortedMultiSet<TData>>,
            long,
            Tuple<TRulesKey, TData>,
            Tuple<TRulesKey, SortedMultiSet<TData>>>> Deaccumulate()
            => (state, time, input) => Tuple.Create(input.Item1, state.Item2.Remove(input.Item2));

        public Expression<Func<
            Tuple<TRulesKey, SortedMultiSet<TData>>,
            Tuple<TRulesKey, SortedMultiSet<TData>>,
            Tuple<TRulesKey, SortedMultiSet<TData>>>> Difference()
            => (left, right) => Tuple.Create(left.Item1, left.Item2.RemoveAll(right.Item2));

        public Expression<Func<
            Tuple<TRulesKey, SortedMultiSet<TData>>>> InitialState()
            => () => Tuple.Create(default(TRulesKey), new SortedMultiSet<TData>());
    }
}
