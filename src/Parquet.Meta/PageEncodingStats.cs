using Parquet.Meta.Proto;

namespace Parquet.Meta;

/// <summary>
/// Statistics of a given page type and encoding.
/// </summary>
public class PageEncodingStats {
    /// <summary>
    /// The page type (data/dic/...).
    /// </summary>
    public PageType PageType { get; set; } = new PageType();

    /// <summary>
    /// Encoding of the page.
    /// </summary>
    public Encoding Encoding { get; set; } = new Encoding();

    /// <summary>
    /// Number of pages of this type with this encoding.
    /// </summary>
    public int Count { get; set; }


    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.StructBegin();

        // 1: PageType, id
        proto.WriteI32Field(1, (int)PageType);
        // 2: Encoding, id
        proto.WriteI32Field(2, (int)Encoding);
        // 3: Count, i32
        proto.WriteI32Field(3, Count);

        proto.StructEnd();
    }

    internal static PageEncodingStats Read(ThriftCompactProtocolReader proto) {
        var r = new PageEncodingStats();
        proto.StructBegin();
        while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
            switch(fieldId) {
                case 1: // PageType, id
                    r.PageType = (PageType)proto.ReadI32();
                    break;
                case 2: // Encoding, id
                    r.Encoding = (Encoding)proto.ReadI32();
                    break;
                case 3: // Count, i32
                    r.Count = proto.ReadI32();
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