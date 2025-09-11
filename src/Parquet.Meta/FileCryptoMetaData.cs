using Parquet.Meta.Proto;

namespace Parquet.Meta;

/// <summary>
/// Crypto metadata for files with encrypted footer.
/// </summary>
public class FileCryptoMetaData {
    /// <summary>
    /// Encryption algorithm. This field is only used for files with encrypted footer. Files with plaintext footer store algorithm id inside footer (FileMetaData structure).
    /// </summary>
    public EncryptionAlgorithm EncryptionAlgorithm { get; set; } = new EncryptionAlgorithm();

    /// <summary>
    /// Retrieval metadata of key used for encryption of footer, and (possibly) columns.
    /// </summary>
    public byte[]? KeyMetadata { get; set; }


    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.StructBegin();

        // 1: EncryptionAlgorithm, id
        proto.BeginInlineStruct(1);
        EncryptionAlgorithm.Write(proto);
        // 2: KeyMetadata, binary
        if(KeyMetadata != null) {
            proto.WriteBinaryField(2, KeyMetadata);
        }

        proto.StructEnd();
    }

    internal static FileCryptoMetaData Read(ThriftCompactProtocolReader proto) {
        var r = new FileCryptoMetaData();
        proto.StructBegin();
        while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
            switch(fieldId) {
                case 1: // EncryptionAlgorithm, id
                    r.EncryptionAlgorithm = EncryptionAlgorithm.Read(proto);
                    break;
                case 2: // KeyMetadata, binary
                    r.KeyMetadata = proto.ReadBinary();
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