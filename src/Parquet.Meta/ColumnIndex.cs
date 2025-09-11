using System.Collections.Generic;
using Parquet.Meta.Proto;

namespace Parquet.Meta;

/// <summary>
/// Optional statistics for each data page in a ColumnChunk.  Forms part the page index, along with OffsetIndex.  If this structure is present, OffsetIndex must also be present.  For each field in this structure, &lt;field&gt;[i] refers to the page at OffsetIndex.page_locations[i].
/// </summary>
public class ColumnIndex {
    /// <summary>
    /// A list of Boolean values to determine the validity of the corresponding min and max values. If true, a page contains only null values, and writers have to set the corresponding entries in min_values and max_values to byte[0], so that all lists have the same length. If false, the corresponding entries in min_values and max_values must be valid.
    /// </summary>
    public List<bool> NullPages { get; set; } = new List<bool>();

    /// <summary>
    /// Two lists containing lower and upper bounds for the values of each page determined by the ColumnOrder of the column. These may be the actual minimum and maximum values found on a page, but can also be (more compact) values that do not exist on a page. For example, instead of storing &quot;&quot;Blart Versenwald III&quot;, a writer may set min_values[i]=&quot;B&quot;, max_values[i]=&quot;C&quot;. Such more compact values must still be valid values within the column&#39;s logical type. Readers must make sure that list entries are populated before using them by inspecting null_pages.
    /// </summary>
    public List<byte[]> MinValues { get; set; } = new List<byte[]>();

    public List<byte[]> MaxValues { get; set; } = new List<byte[]>();

    /// <summary>
    /// Stores whether both min_values and max_values are ordered and if so, in which direction. This allows readers to perform binary searches in both lists. Readers cannot assume that max_values[i] &lt;= min_values[i+1], even if the lists are ordered.
    /// </summary>
    public BoundaryOrder BoundaryOrder { get; set; } = new BoundaryOrder();

    /// <summary>
    /// A list containing the number of null values for each page  Writers SHOULD always write this field even if no null values are present or the column is not nullable. Readers MUST distinguish between null_counts not being present and null_count being 0. If null_counts are not present, readers MUST NOT assume all null counts are 0.
    /// </summary>
    public List<long>? NullCounts { get; set; }

    /// <summary>
    /// Contains repetition level histograms for each page concatenated together.  The repetition_level_histogram field on SizeStatistics contains more details.  When present the length should always be (number of pages * (max_repetition_level + 1)) elements.  Element 0 is the first element of the histogram for the first page. Element (max_repetition_level + 1) is the first element of the histogram for the second page.
    /// </summary>
    public List<long>? RepetitionLevelHistograms { get; set; }

    /// <summary>
    /// Same as repetition_level_histograms except for definitions levels.
    /// </summary>
    public List<long>? DefinitionLevelHistograms { get; set; }


    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.StructBegin();

        // 1: NullPages, list
        proto.WriteListBegin(1, 1, NullPages.Count);
        foreach(bool element in NullPages) {
            proto.WriteBoolValue(element);
        }
        // 2: MinValues, list
        proto.WriteListBegin(2, 8, MinValues.Count);
        foreach(byte[] element in MinValues) {
            proto.WriteBinaryValue(element);
        }
        // 3: MaxValues, list
        proto.WriteListBegin(3, 8, MaxValues.Count);
        foreach(byte[] element in MaxValues) {
            proto.WriteBinaryValue(element);
        }
        // 4: BoundaryOrder, id
        proto.WriteI32Field(4, (int)BoundaryOrder);
        // 5: NullCounts, list
        if(NullCounts != null) {
            proto.WriteListBegin(5, 6, NullCounts.Count);
            foreach(long element in NullCounts) {
                proto.WriteI64Value(element);
            }
        }
        // 6: RepetitionLevelHistograms, list
        if(RepetitionLevelHistograms != null) {
            proto.WriteListBegin(6, 6, RepetitionLevelHistograms.Count);
            foreach(long element in RepetitionLevelHistograms) {
                proto.WriteI64Value(element);
            }
        }
        // 7: DefinitionLevelHistograms, list
        if(DefinitionLevelHistograms != null) {
            proto.WriteListBegin(7, 6, DefinitionLevelHistograms.Count);
            foreach(long element in DefinitionLevelHistograms) {
                proto.WriteI64Value(element);
            }
        }

        proto.StructEnd();
    }

    internal static ColumnIndex Read(ThriftCompactProtocolReader proto) {
        var r = new ColumnIndex();
        proto.StructBegin();
        int elementCount = 0;
        while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
            switch(fieldId) {
                case 1: // NullPages, list
                    elementCount = proto.ReadListHeader(out _);
                    r.NullPages = new List<bool>(elementCount);
                    for(int i = 0; i < elementCount; i++) { r.NullPages.Add(proto.ReadBool()); }
                    break;
                case 2: // MinValues, list
                    elementCount = proto.ReadListHeader(out _);
                    r.MinValues = new List<byte[]>(elementCount);
                    for(int i = 0; i < elementCount; i++) { r.MinValues.Add(proto.ReadBinary()); }
                    break;
                case 3: // MaxValues, list
                    elementCount = proto.ReadListHeader(out _);
                    r.MaxValues = new List<byte[]>(elementCount);
                    for(int i = 0; i < elementCount; i++) { r.MaxValues.Add(proto.ReadBinary()); }
                    break;
                case 4: // BoundaryOrder, id
                    r.BoundaryOrder = (BoundaryOrder)proto.ReadI32();
                    break;
                case 5: // NullCounts, list
                    elementCount = proto.ReadListHeader(out _);
                    r.NullCounts = new List<long>(elementCount);
                    for(int i = 0; i < elementCount; i++) { r.NullCounts.Add(proto.ReadI64()); }
                    break;
                case 6: // RepetitionLevelHistograms, list
                    elementCount = proto.ReadListHeader(out _);
                    r.RepetitionLevelHistograms = new List<long>(elementCount);
                    for(int i = 0; i < elementCount; i++) { r.RepetitionLevelHistograms.Add(proto.ReadI64()); }
                    break;
                case 7: // DefinitionLevelHistograms, list
                    elementCount = proto.ReadListHeader(out _);
                    r.DefinitionLevelHistograms = new List<long>(elementCount);
                    for(int i = 0; i < elementCount; i++) { r.DefinitionLevelHistograms.Add(proto.ReadI64()); }
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