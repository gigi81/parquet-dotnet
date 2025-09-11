using Parquet.Meta.Proto;

namespace Parquet.Meta;

/// <summary>
/// Hash strategy type annotation. xxHash is an extremely fast non-cryptographic hash algorithm. It uses 64 bits version of xxHash.
/// </summary>
public class XxHash {

    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.WriteEmptyStruct();
    }

    internal static XxHash Read(ThriftCompactProtocolReader proto) {
        var r = new XxHash();
        proto.StructBegin();
        while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
            switch(fieldId) {
                default:
                    proto.SkipField(compactType);
                    break;
            }
        }
        proto.StructEnd();
        return r;
    }
}