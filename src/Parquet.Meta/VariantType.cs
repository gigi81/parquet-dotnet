using Parquet.Meta.Proto;

namespace Parquet.Meta;

/// <summary>
/// Embedded Variant logical type annotation.
/// </summary>
public class VariantType {

    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.WriteEmptyStruct();
    }

    internal static VariantType Read(ThriftCompactProtocolReader proto) {
        var r = new VariantType();
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