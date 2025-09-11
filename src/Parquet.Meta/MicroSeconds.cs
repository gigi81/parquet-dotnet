using Parquet.Meta.Proto;

namespace Parquet.Meta;

public class MicroSeconds {

    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.WriteEmptyStruct();
    }

    internal static MicroSeconds Read(ThriftCompactProtocolReader proto) {
        var r = new MicroSeconds();
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