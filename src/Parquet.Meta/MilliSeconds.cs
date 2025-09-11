using Parquet.Meta.Proto;

namespace Parquet.Meta;

/// <summary>
/// Time units for logical types.
/// </summary>
public class MilliSeconds {

    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.WriteEmptyStruct();
    }

    internal static MilliSeconds Read(ThriftCompactProtocolReader proto) {
        var r = new MilliSeconds();
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