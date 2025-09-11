using Parquet.Meta.Proto;

namespace Parquet.Meta;

public class PageLocation {
    /// <summary>
    /// Offset of the page in the file.
    /// </summary>
    public long Offset { get; set; }

    /// <summary>
    /// Size of the page, including header. Sum of compressed_page_size and header length.
    /// </summary>
    public int CompressedPageSize { get; set; }

    /// <summary>
    /// Index within the RowGroup of the first row of the page. When an OffsetIndex is present, pages must begin on row boundaries (repetition_level = 0).
    /// </summary>
    public long FirstRowIndex { get; set; }


    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.StructBegin();

        // 1: Offset, i64
        proto.WriteI64Field(1, Offset);
        // 2: CompressedPageSize, i32
        proto.WriteI32Field(2, CompressedPageSize);
        // 3: FirstRowIndex, i64
        proto.WriteI64Field(3, FirstRowIndex);

        proto.StructEnd();
    }

    internal static PageLocation Read(ThriftCompactProtocolReader proto) {
        var r = new PageLocation();
        proto.StructBegin();
        while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
            switch(fieldId) {
                case 1: // Offset, i64
                    r.Offset = proto.ReadI64();
                    break;
                case 2: // CompressedPageSize, i32
                    r.CompressedPageSize = proto.ReadI32();
                    break;
                case 3: // FirstRowIndex, i64
                    r.FirstRowIndex = proto.ReadI64();
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