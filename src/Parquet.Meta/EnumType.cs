using Parquet.Meta.Proto;
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Parquet.Meta;

public class EnumType {

    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.WriteEmptyStruct();
    }

    internal static EnumType Read(ThriftCompactProtocolReader proto) {
        var r = new EnumType();
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