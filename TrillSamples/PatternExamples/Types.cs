// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.Collections.Generic;

namespace PatternExamples
{
    internal struct Payload
    {
        public string Field1;
        public int Field2;

        public override string ToString() => new { this.Field1, this.Field2 }.ToString();
    }

    internal sealed class FList<T> : List<T>
    {
        public FList<T> FAdd(T t)
        {
            var ret = new FList<T>();
            ret.AddRange(this);
            ret.Add(t);
            return ret;
        }

        public override string ToString()
        {
            string str = "{ ";
            bool first = true;
            foreach (var e in this)
            {
                if (!first) str += ", ";
                str += "[" + e.ToString() + "]";
                first = false;
            }
            str += " }";
            return str;
        }
    }
}
