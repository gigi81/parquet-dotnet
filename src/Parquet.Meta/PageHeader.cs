using Parquet.Meta.Proto;

namespace Parquet.Meta;

public class PageHeader {
    /// <summary>
    /// The type of the page: indicates which of the *_header fields is set.
    /// </summary>
    public PageType Type { get; set; } = new PageType();

    /// <summary>
    /// Uncompressed page size in bytes (not including this header).
    /// </summary>
    public int UncompressedPageSize { get; set; }

    /// <summary>
    /// Compressed (and potentially encrypted) page size in bytes, not including this header.
    /// </summary>
    public int CompressedPageSize { get; set; }

    /// <summary>
    /// The 32-bit CRC checksum for the page, to be be calculated as follows:  - The standard CRC32 algorithm is used (with polynomial 0x04C11DB7,   the same as in e.g. GZip). - All page types can have a CRC (v1 and v2 data pages, dictionary pages,   etc.). - The CRC is computed on the serialization binary representation of the page   (as written to disk), excluding the page header. For example, for v1   data pages, the CRC is computed on the concatenation of repetition levels,   definition levels and column values (optionally compressed, optionally   encrypted). - The CRC computation therefore takes place after any compression   and encryption steps, if any.  If enabled, this allows for disabling checksumming in HDFS if only a few pages need to be read.
    /// </summary>
    public int? Crc { get; set; }

    public DataPageHeader? DataPageHeader { get; set; }

    public IndexPageHeader? IndexPageHeader { get; set; }

    public DictionaryPageHeader? DictionaryPageHeader { get; set; }

    public DataPageHeaderV2? DataPageHeaderV2 { get; set; }
    
    public void Write(ThriftCompactProtocolWriter proto) {
        proto.StructBegin();

        // 1: Type, id
        proto.WriteI32Field(1, (int)Type);
        // 2: UncompressedPageSize, i32
        proto.WriteI32Field(2, UncompressedPageSize);
        // 3: CompressedPageSize, i32
        proto.WriteI32Field(3, CompressedPageSize);
        // 4: Crc, i32
        if(Crc != null) {
            proto.WriteI32Field(4, Crc.Value);
        }
        // 5: DataPageHeader, id
        if(DataPageHeader != null) {
            proto.BeginInlineStruct(5);
            DataPageHeader.Write(proto);
        }
        // 6: IndexPageHeader, id
        if(IndexPageHeader != null) {
            proto.BeginInlineStruct(6);
            IndexPageHeader.Write(proto);
        }
        // 7: DictionaryPageHeader, id
        if(DictionaryPageHeader != null) {
            proto.BeginInlineStruct(7);
            DictionaryPageHeader.Write(proto);
        }
        // 8: DataPageHeaderV2, id
        if(DataPageHeaderV2 != null) {
            proto.BeginInlineStruct(8);
            DataPageHeaderV2.Write(proto);
        }

        proto.StructEnd();
    }

    public static PageHeader Read(ThriftCompactProtocolReader proto) {
        var r = new PageHeader();
        proto.StructBegin();
        while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
            switch(fieldId) {
                case 1: // Type, id
                    r.Type = (PageType)proto.ReadI32();
                    break;
                case 2: // UncompressedPageSize, i32
                    r.UncompressedPageSize = proto.ReadI32();
                    break;
                case 3: // CompressedPageSize, i32
                    r.CompressedPageSize = proto.ReadI32();
                    break;
                case 4: // Crc, i32
                    r.Crc = proto.ReadI32();
                    break;
                case 5: // DataPageHeader, id
                    r.DataPageHeader = DataPageHeader.Read(proto);
                    break;
                case 6: // IndexPageHeader, id
                    r.IndexPageHeader = IndexPageHeader.Read(proto);
                    break;
                case 7: // DictionaryPageHeader, id
                    r.DictionaryPageHeader = DictionaryPageHeader.Read(proto);
                    break;
                case 8: // DataPageHeaderV2, id
                    r.DataPageHeaderV2 = DataPageHeaderV2.Read(proto);
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