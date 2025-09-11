using Parquet.Meta.Proto;

namespace Parquet.Meta;

public class EncryptionWithFooterKey {

    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.WriteEmptyStruct();
    }

    internal static EncryptionWithFooterKey Read(ThriftCompactProtocolReader proto) {
        var r = new EncryptionWithFooterKey();
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