using Parquet.Meta.Proto;

namespace Parquet.Meta;

/// <summary>
/// New page format allowing reading levels without decompressing the data Repetition and definition levels are uncompressed The remaining section containing the data is compressed if is_compressed is true.
/// </summary>
public class DataPageHeaderV2 {
    /// <summary>
    /// Number of values, including NULLs, in this data page.
    /// </summary>
    public int NumValues { get; set; }

    /// <summary>
    /// Number of NULL values, in this data page. Number of non-null = num_values - num_nulls which is also the number of values in the data section.
    /// </summary>
    public int NumNulls { get; set; }

    /// <summary>
    /// Number of rows in this data page. Every page must begin at a row boundary (repetition_level = 0): rows must **not** be split across page boundaries when using V2 data pages.
    /// </summary>
    public int NumRows { get; set; }

    /// <summary>
    /// Encoding used for data in this page.
    /// </summary>
    public Encoding Encoding { get; set; } = new Encoding();

    /// <summary>
    /// Length of the definition levels.
    /// </summary>
    public int DefinitionLevelsByteLength { get; set; }

    /// <summary>
    /// Length of the repetition levels.
    /// </summary>
    public int RepetitionLevelsByteLength { get; set; }

    /// <summary>
    /// Whether the values are compressed. Which means the section of the page between definition_levels_byte_length + repetition_levels_byte_length + 1 and compressed_page_size (included) is compressed with the compression_codec. If missing it is considered compressed.
    /// </summary>
    public bool? IsCompressed { get; set; }

    /// <summary>
    /// Optional statistics for the data in this page.
    /// </summary>
    public Statistics? Statistics { get; set; }


    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.StructBegin();

        // 1: NumValues, i32
        proto.WriteI32Field(1, NumValues);
        // 2: NumNulls, i32
        proto.WriteI32Field(2, NumNulls);
        // 3: NumRows, i32
        proto.WriteI32Field(3, NumRows);
        // 4: Encoding, id
        proto.WriteI32Field(4, (int)Encoding);
        // 5: DefinitionLevelsByteLength, i32
        proto.WriteI32Field(5, DefinitionLevelsByteLength);
        // 6: RepetitionLevelsByteLength, i32
        proto.WriteI32Field(6, RepetitionLevelsByteLength);
        // 7: IsCompressed, bool
        if(IsCompressed != null) {
            proto.WriteBoolField(7, IsCompressed.Value);
        }
        // 8: Statistics, id
        if(Statistics != null) {
            proto.BeginInlineStruct(8);
            Statistics.Write(proto);
        }

        proto.StructEnd();
    }

    internal static DataPageHeaderV2 Read(ThriftCompactProtocolReader proto) {
        var r = new DataPageHeaderV2();
        proto.StructBegin();
        while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
            switch(fieldId) {
                case 1: // NumValues, i32
                    r.NumValues = proto.ReadI32();
                    break;
                case 2: // NumNulls, i32
                    r.NumNulls = proto.ReadI32();
                    break;
                case 3: // NumRows, i32
                    r.NumRows = proto.ReadI32();
                    break;
                case 4: // Encoding, id
                    r.Encoding = (Encoding)proto.ReadI32();
                    break;
                case 5: // DefinitionLevelsByteLength, i32
                    r.DefinitionLevelsByteLength = proto.ReadI32();
                    break;
                case 6: // RepetitionLevelsByteLength, i32
                    r.RepetitionLevelsByteLength = proto.ReadI32();
                    break;
                case 7: // IsCompressed, bool
                    r.IsCompressed = compactType == CompactType.BooleanTrue;
                    break;
                case 8: // Statistics, id
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