// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
namespace HelloToll
{
    internal struct TollReading
    {
        public string TollId { get; set; }
        public string LicensePlate { get; set; }
        public string State { get; set; }
        public string Make { get; set; }
        public string Model { get; set; }
        public int VehicleType { get; set; } // 1 for passenger, 2 for commercial
        public float VehicleWeight { get; set; } // vehicle weight in tons, 0 for passenger vehicles
        public float Toll { get; set; }
        public string Tag { get; set; }

        public override string ToString()
        {
            return new { this.TollId, this.LicensePlate, this.State, this.Make,
                this.Model,
                this.VehicleType,
                this.VehicleWeight,
                this.Toll,
                this.Tag }.ToString();
        }
    }

    internal sealed class Toll
    {
        public string TollId { get; set; }
        public double TollAmount { get; set; }
        public ulong VehicleCount { get; set; }

        public override string ToString()
        {
            return new { this.TollId, this.TollAmount, this.VehicleCount }.ToString();
        }

        public override bool Equals(object obj)
        {
            if (!(obj is Toll other)) return false;
            return this.TollId.Equals(other.TollId) && this.TollAmount.Equals(other.TollAmount) && this.VehicleCount.Equals(other.VehicleCount);
        }
        public override int GetHashCode()
        {
            return this.TollId.GetHashCode() ^ this.TollAmount.GetHashCode() ^ this.VehicleCount.GetHashCode();
        }
    }

    internal sealed class TollAverage
    {
        public string TollId { get; set; }
        public double AverageToll { get; set; }

        public override string ToString()
        {
            return new { this.TollId, this.AverageToll }.ToString();
        }
    }

    internal sealed class TollCompare
    {
        public string TollId1 { get; set; }
        public string TollId2 { get; set; }
        public ulong VehicleCount { get; set; }

        public override string ToString()
        {
            return new { this.TollId1, this.TollId2, this.VehicleCount }.ToString();
        }
    }

    internal sealed class TopEvents
    {
        public int TollRank { get; set; }
        public string TollId { get; set; }
        public double TollAmount { get; set; }
        public long VehicleCount { get; set; }

        public override string ToString()
        {
            return new { this.TollRank, this.TollId, this.TollAmount, this.VehicleCount }.ToString();
        }
    }

    internal sealed class AccountInfo
    {
        public string AccountId { get; set; }
        public string Name { get; set; }
        public string ZipCode { get; set; }
        public string Address1 { get; set; }
        public string Address2 { get; set; }

        public override string ToString()
        {
            return new { this.AccountId, this.Name, this.ZipCode, this.Address1,
                this.Address2 }.ToString();
        }
    }

    internal sealed class TollViolation
    {
        public string TollId { get; set; }
        public string LicensePlate { get; set; }
        public string State { get; set; }
        public string Make { get; set; }
        public string Model { get; set; }
        public string Tag { get; set; }

        public override string ToString()
        {
            return new { this.TollId, this.LicensePlate, this.State, this.Make,
                this.Model,
                this.Tag }.ToString();
        }
    }

    internal sealed class TollOuterJoin
    {
        public string LicensePlate { get; set; }
        public string Make { get; set; }
        public string Model { get; set; }
        public float? Toll { get; set; }
        public string TollId { get; set; }

        public override string ToString()
        {
            return new { this.LicensePlate, this.Make, this.Model, this.Toll,
                this.TollId }.ToString();
        }
    }

    internal sealed class VehicleWeightInfo
    {
        public string LicensePlate { get; set; }
        public double? Weight { get; set; }
        public double? WeightCharge { get; set; }

        public override string ToString()
        {
            return new { this.LicensePlate, this.Weight, this.WeightCharge }.ToString();
        }
    }
}
