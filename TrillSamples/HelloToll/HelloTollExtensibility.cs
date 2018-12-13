// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq;

namespace HelloToll
{
    internal sealed class TagInfo
    {
        public string TagId { get; set; }

        public DateTime RenewalDate { get; set; }

        public bool IsReportedLostOrStolen { get; set; }

        public string AccountId { get; set; }

        public static bool IsLostOrStolen(string tagId)
        {
            return Tags.Any(tag => tag.TagId == tagId && tag.IsReportedLostOrStolen);
        }

        public static bool IsExpired(string tagId)
        {
            return Tags.Any(tag => tag.TagId == tagId && tag.RenewalDate.AddYears(1) > DateTime.Now);
        }

        public static TagInfo[] Tags { get; } = new[]
        {
           new TagInfo
           {
               TagId = "123456789",
               RenewalDate = new DateTime(2009, 02, 20),
               IsReportedLostOrStolen = false,
               AccountId = "NJ100001JET1109"
           },
           new TagInfo
           {
               TagId = "234567891",
               RenewalDate = new DateTime(2008, 12, 06),
               IsReportedLostOrStolen = true,
               AccountId = "NY100002GNT0109"
           },
           new TagInfo
           {
               TagId = "345678912",
               RenewalDate = new DateTime(2008, 09, 01),
               IsReportedLostOrStolen = true,
               AccountId = "CT100003YNK0210"
           },
        };
    }
}
