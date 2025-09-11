using Parquet.Meta.Proto;

namespace Parquet.Meta;

public class Float16Type {

    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.WriteEmptyStruct();
    }

    internal static Float16Type Read(ThriftCompactProtocolReader proto) {
        var r = new Float16Type();
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