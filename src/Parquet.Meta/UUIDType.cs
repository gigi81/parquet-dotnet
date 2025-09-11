#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member
using Parquet.Meta.Proto;
namespace Parquet.Meta;

public class UUIDType {

    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.WriteEmptyStruct();
    }

    internal static UUIDType Read(ThriftCompactProtocolReader proto) {
        var r = new UUIDType();
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

#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
