using Parquet.Meta.Proto;

namespace Parquet.Meta;

/// <summary>
/// Embedded JSON logical type annotation  Allowed for physical types: BYTE_ARRAY.
/// </summary>
public class JsonType {

    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.WriteEmptyStruct();
    }

    internal static JsonType Read(ThriftCompactProtocolReader proto) {
        var r = new JsonType();
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