using Parquet.Meta.Proto;

namespace Parquet.Meta;

/// <summary>
/// Embedded BSON logical type annotation  Allowed for physical types: BYTE_ARRAY.
/// </summary>
public class BsonType {

    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.WriteEmptyStruct();
    }

    internal static BsonType Read(ThriftCompactProtocolReader proto) {
        var r = new BsonType();
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