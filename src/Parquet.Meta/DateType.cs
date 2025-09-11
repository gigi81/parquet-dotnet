using Parquet.Meta.Proto;

namespace Parquet.Meta;

public class DateType {

    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.WriteEmptyStruct();
    }

    internal static DateType Read(ThriftCompactProtocolReader proto) {
        var r = new DateType();
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