using Parquet.Meta.Proto;

namespace Parquet.Meta;
#pragma warning disable CS1591
/// <summary>
/// Empty structs to use as logical type annotations.
/// </summary>
public class StringType {

    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.WriteEmptyStruct();
    }

    internal static StringType Read(ThriftCompactProtocolReader proto) {
        var r = new StringType();
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