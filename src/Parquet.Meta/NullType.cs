using Parquet.Meta.Proto;

namespace Parquet.Meta;

/// <summary>
/// Logical type to annotate a column that is always null.  Sometimes when discovering the schema of existing data, values are always null and the physical type can&#39;t be determined. This annotation signals the case where the physical type was guessed from all null values.
/// </summary>
public class NullType {

    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.WriteEmptyStruct();
    }

    internal static NullType Read(ThriftCompactProtocolReader proto) {
        var r = new NullType();
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