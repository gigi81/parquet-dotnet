using Parquet.Meta.Proto;

namespace Parquet.Meta;

/// <summary>
/// Empty struct to signal the order defined by the physical or logical type.
/// </summary>
public class TypeDefinedOrder {

    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.WriteEmptyStruct();
    }

    internal static TypeDefinedOrder Read(ThriftCompactProtocolReader proto) {
        var r = new TypeDefinedOrder();
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