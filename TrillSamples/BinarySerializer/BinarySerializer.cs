// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.IO;
using System.Runtime.Serialization;
using System.Xml;
using Microsoft.StreamProcessing;

public static class BinarySerializer
{
    public static byte[] Serialize<T>(T obj)
    {
        var serializer = new DataContractSerializer(typeof(T));
        var stream = new MemoryStream();
        using (var writer = XmlDictionaryWriter.CreateBinaryWriter(stream))
        {
            serializer.WriteObject(writer, obj);
        }

        return stream.ToArray();
    }

    public static T Deserialize<T>(byte[] data)
    {
        return (T)Deserialize(typeof(T), data);
    }

    public static object Deserialize(Type type, byte[] data)
    {
        var serializer = new DataContractSerializer(type);
        using (var stream = new MemoryStream(data))
        using (var reader = XmlDictionaryReader.CreateBinaryReader(stream, XmlDictionaryReaderQuotas.Max))
        {
            return serializer.ReadObject(reader);
        }
    }

    public static byte[] Serialize(StreamEvent<long> message)
    {
        var stream = new MemoryStream();
        using (var writer = new BinaryWriter(stream))
        {
            writer.Write(message.IsEnd ? message.EndTime : message.StartTime);
            writer.Write(message.IsEnd ? message.StartTime : message.EndTime);
            writer.Write(message.Payload);
        }
        return stream.ToArray();
    }

    public static StreamEvent<long> DeserializeStreamEventLong(byte[] data)
    {
        var stream = new MemoryStream(data);
        using (var reader = new BinaryReader(stream))
        {
            var syncTime = reader.ReadInt64();
            var otherTime = reader.ReadInt64();
            var payload = reader.ReadInt64();
            return new StreamEvent<long>(syncTime, otherTime, payload);
        }
    }
}