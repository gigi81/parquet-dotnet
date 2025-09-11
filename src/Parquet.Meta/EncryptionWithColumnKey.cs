using System.Collections.Generic;
using Parquet.Meta.Proto;

namespace Parquet.Meta;

public class EncryptionWithColumnKey {
    /// <summary>
    /// Column path in schema.
    /// </summary>
    public List<string> PathInSchema { get; set; } = new List<string>();

    /// <summary>
    /// Retrieval metadata of column encryption key.
    /// </summary>
    public byte[]? KeyMetadata { get; set; }


    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.StructBegin();

        // 1: PathInSchema, list
        proto.WriteListBegin(1, 8, PathInSchema.Count);
        foreach(string element in PathInSchema) {
            proto.WriteStringValue(element);
        }
        // 2: KeyMetadata, binary
        if(KeyMetadata != null) {
            proto.WriteBinaryField(2, KeyMetadata);
        }

        proto.StructEnd();
    }

    internal static EncryptionWithColumnKey Read(ThriftCompactProtocolReader proto) {
        var r = new EncryptionWithColumnKey();
        proto.StructBegin();
        int elementCount = 0;
        while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
            switch(fieldId) {
                case 1: // PathInSchema, list
                    elementCount = proto.ReadListHeader(out _);
                    r.PathInSchema = new List<string>(elementCount);
                    for(int i = 0; i < elementCount; i++) { r.PathInSchema.Add(proto.ReadString()); }
                    break;
                case 2: // KeyMetadata, binary
                    r.KeyMetadata = proto.ReadBinary();
                    break;
                default:
                    proto.SkipField(compactType);
                    break;
            }
        }
        proto.StructEnd();
        return r;
    }
}