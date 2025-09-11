using Parquet.Meta.Proto;

namespace Parquet.Meta;

public class ColumnCryptoMetaData {
    public EncryptionWithFooterKey? ENCRYPTIONWITHFOOTERKEY { get; set; }

    public EncryptionWithColumnKey? ENCRYPTIONWITHCOLUMNKEY { get; set; }


    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.StructBegin();

        // 1: ENCRYPTIONWITHFOOTERKEY, id
        if(ENCRYPTIONWITHFOOTERKEY != null) {
            proto.BeginInlineStruct(1);
            ENCRYPTIONWITHFOOTERKEY.Write(proto);
        }
        // 2: ENCRYPTIONWITHCOLUMNKEY, id
        if(ENCRYPTIONWITHCOLUMNKEY != null) {
            proto.BeginInlineStruct(2);
            ENCRYPTIONWITHCOLUMNKEY.Write(proto);
        }

        proto.StructEnd();
    }

    internal static ColumnCryptoMetaData Read(ThriftCompactProtocolReader proto) {
        var r = new ColumnCryptoMetaData();
        proto.StructBegin();
        while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
            switch(fieldId) {
                case 1: // ENCRYPTIONWITHFOOTERKEY, id
                    r.ENCRYPTIONWITHFOOTERKEY = EncryptionWithFooterKey.Read(proto);
                    break;
                case 2: // ENCRYPTIONWITHCOLUMNKEY, id
                    r.ENCRYPTIONWITHCOLUMNKEY = EncryptionWithColumnKey.Read(proto);
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