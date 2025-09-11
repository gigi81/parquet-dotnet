using Parquet.Meta.Proto;

namespace Parquet.Meta;

public class NanoSeconds {

    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.WriteEmptyStruct();
    }

    internal static NanoSeconds Read(ThriftCompactProtocolReader proto) {
        var r = new NanoSeconds();
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