using Parquet.Meta.Proto;

namespace Parquet.Meta;

/// <summary>
/// Data page header.
/// </summary>
public class DataPageHeader {
    /// <summary>
    /// Number of values, including NULLs, in this data page.  If a OffsetIndex is present, a page must begin at a row boundary (repetition_level = 0). Otherwise, pages may begin within a row (repetition_level &gt; 0).
    /// </summary>
    public int NumValues { get; set; }

    /// <summary>
    /// Encoding used for this data page.
    /// </summary>
    public Encoding Encoding { get; set; } = new Encoding();

    /// <summary>
    /// Encoding used for definition levels.
    /// </summary>
    public Encoding DefinitionLevelEncoding { get; set; } = new Encoding();

    /// <summary>
    /// Encoding used for repetition levels.
    /// </summary>
    public Encoding RepetitionLevelEncoding { get; set; } = new Encoding();

    /// <summary>
    /// Optional statistics for the data in this page.
    /// </summary>
    public Statistics? Statistics { get; set; }


    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.StructBegin();

        // 1: NumValues, i32
        proto.WriteI32Field(1, NumValues);
        // 2: Encoding, id
        proto.WriteI32Field(2, (int)Encoding);
        // 3: DefinitionLevelEncoding, id
        proto.WriteI32Field(3, (int)DefinitionLevelEncoding);
        // 4: RepetitionLevelEncoding, id
        proto.WriteI32Field(4, (int)RepetitionLevelEncoding);
        // 5: Statistics, id
        if(Statistics != null) {
            proto.BeginInlineStruct(5);
            Statistics.Write(proto);
        }

        proto.StructEnd();
    }

    internal static DataPageHeader Read(ThriftCompactProtocolReader proto) {
        var r = new DataPageHeader();
        proto.StructBegin();
        while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
            switch(fieldId) {
                case 1: // NumValues, i32
                    r.NumValues = proto.ReadI32();
                    break;
                case 2: // Encoding, id
                    r.Encoding = (Encoding)proto.ReadI32();
                    break;
                case 3: // DefinitionLevelEncoding, id
                    r.DefinitionLevelEncoding = (Encoding)proto.ReadI32();
                    break;
                case 4: // RepetitionLevelEncoding, id
                    r.RepetitionLevelEncoding = (Encoding)proto.ReadI32();
                    break;
                case 5: // Statistics, id
                    r.Statistics = Statistics.Read(proto);
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