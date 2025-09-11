using Parquet.Meta.Proto;

namespace Parquet.Meta;

public class EncryptionAlgorithm {
    public AesGcmV1? AESGCMV1 { get; set; }

    public AesGcmCtrV1? AESGCMCTRV1 { get; set; }


    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.StructBegin();

        // 1: AESGCMV1, id
        if(AESGCMV1 != null) {
            proto.BeginInlineStruct(1);
            AESGCMV1.Write(proto);
        }
        // 2: AESGCMCTRV1, id
        if(AESGCMCTRV1 != null) {
            proto.BeginInlineStruct(2);
            AESGCMCTRV1.Write(proto);
        }

        proto.StructEnd();
    }

    internal static EncryptionAlgorithm Read(ThriftCompactProtocolReader proto) {
        var r = new EncryptionAlgorithm();
        proto.StructBegin();
        while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
            switch(fieldId) {
                case 1: // AESGCMV1, id
                    r.AESGCMV1 = AesGcmV1.Read(proto);
                    break;
                case 2: // AESGCMCTRV1, id
                    r.AESGCMCTRV1 = AesGcmCtrV1.Read(proto);
                    break;
                default:
                    proto.SkipField(compactType);
                    break;
            }
        }
        proto.StructEnd();
        return r;
    }
}