using Parquet.Meta.Proto;

namespace Parquet.Meta;

public class IndexPageHeader {

    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.WriteEmptyStruct();
    }

    internal static IndexPageHeader Read(ThriftCompactProtocolReader proto) {
        var r = new IndexPageHeader();
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