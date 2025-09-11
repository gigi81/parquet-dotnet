using Parquet.Meta.Proto;

namespace Parquet.Meta;

public class ColumnChunk {
    /// <summary>
    /// File where column data is stored.  If not set, assumed to be same file as metadata.  This path is relative to the current file.
    /// </summary>
    public string? FilePath { get; set; }

    /// <summary>
    /// Deprecated: Byte offset in file_path to the ColumnMetaData  Past use of this field has been inconsistent, with some implementations using it to point to the ColumnMetaData and some using it to point to the first page in the column chunk. In many cases, the ColumnMetaData at this location is wrong. This field is now deprecated and should not be used. Writers should set this field to 0 if no ColumnMetaData has been written outside the footer.
    /// </summary>
    public long FileOffset { get; set; }

    /// <summary>
    /// Column metadata for this chunk. Some writers may also replicate this at the location pointed to by file_path/file_offset. Note: while marked as optional, this field is in fact required by most major Parquet implementations. As such, writers MUST populate this field.
    /// </summary>
    public ColumnMetaData? MetaData { get; set; }

    /// <summary>
    /// File offset of ColumnChunk&#39;s OffsetIndex.
    /// </summary>
    public long? OffsetIndexOffset { get; set; }

    /// <summary>
    /// Size of ColumnChunk&#39;s OffsetIndex, in bytes.
    /// </summary>
    public int? OffsetIndexLength { get; set; }

    /// <summary>
    /// File offset of ColumnChunk&#39;s ColumnIndex.
    /// </summary>
    public long? ColumnIndexOffset { get; set; }

    /// <summary>
    /// Size of ColumnChunk&#39;s ColumnIndex, in bytes.
    /// </summary>
    public int? ColumnIndexLength { get; set; }

    /// <summary>
    /// Crypto metadata of encrypted columns.
    /// </summary>
    public ColumnCryptoMetaData? CryptoMetadata { get; set; }

    /// <summary>
    /// Encrypted column metadata for this chunk.
    /// </summary>
    public byte[]? EncryptedColumnMetadata { get; set; }


    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.StructBegin();

        // 1: FilePath, string
        if(FilePath != null) {
            proto.WriteStringField(1, FilePath);
        }
        // 2: FileOffset, i64
        proto.WriteI64Field(2, FileOffset);
        // 3: MetaData, id
        if(MetaData != null) {
            proto.BeginInlineStruct(3);
            MetaData.Write(proto);
        }
        // 4: OffsetIndexOffset, i64
        if(OffsetIndexOffset != null) {
            proto.WriteI64Field(4, OffsetIndexOffset.Value);
        }
        // 5: OffsetIndexLength, i32
        if(OffsetIndexLength != null) {
            proto.WriteI32Field(5, OffsetIndexLength.Value);
        }
        // 6: ColumnIndexOffset, i64
        if(ColumnIndexOffset != null) {
            proto.WriteI64Field(6, ColumnIndexOffset.Value);
        }
        // 7: ColumnIndexLength, i32
        if(ColumnIndexLength != null) {
            proto.WriteI32Field(7, ColumnIndexLength.Value);
        }
        // 8: CryptoMetadata, id
        if(CryptoMetadata != null) {
            proto.BeginInlineStruct(8);
            CryptoMetadata.Write(proto);
        }
        // 9: EncryptedColumnMetadata, binary
        if(EncryptedColumnMetadata != null) {
            proto.WriteBinaryField(9, EncryptedColumnMetadata);
        }

        proto.StructEnd();
    }

    internal static ColumnChunk Read(ThriftCompactProtocolReader proto) {
        var r = new ColumnChunk();
        proto.StructBegin();
        while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
            switch(fieldId) {
                case 1: // FilePath, string
                    r.FilePath = proto.ReadString();
                    break;
                case 2: // FileOffset, i64
                    r.FileOffset = proto.ReadI64();
                    break;
                case 3: // MetaData, id
                    r.MetaData = ColumnMetaData.Read(proto);
                    break;
                case 4: // OffsetIndexOffset, i64
                    r.OffsetIndexOffset = proto.ReadI64();
                    break;
                case 5: // OffsetIndexLength, i32
                    r.OffsetIndexLength = proto.ReadI32();
                    break;
                case 6: // ColumnIndexOffset, i64
                    r.ColumnIndexOffset = proto.ReadI64();
                    break;
                case 7: // ColumnIndexLength, i32
                    r.ColumnIndexLength = proto.ReadI32();
                    break;
                case 8: // CryptoMetadata, id
                    r.CryptoMetadata = ColumnCryptoMetaData.Read(proto);
                    break;
                case 9: // EncryptedColumnMetadata, binary
                    r.EncryptedColumnMetadata = proto.ReadBinary();
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