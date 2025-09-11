using Parquet.Meta.Proto;

namespace Parquet.Meta;
#pragma warning disable CS1591
/// <summary>
/// Statistics per row group and per page All fields are optional.
/// </summary>
public class Statistics {
    /// <summary>
    /// DEPRECATED: min and max value of the column. Use min_value and max_value.  Values are encoded using PLAIN encoding, except that variable-length byte arrays do not include a length prefix.  These fields encode min and max values determined by signed comparison only. New files should use the correct order for a column&#39;s logical type and store the values in the min_value and max_value fields.  To support older readers, these may be set when the column order is signed.
    /// </summary>
    public byte[]? Max { get; set; }

    public byte[]? Min { get; set; }

    /// <summary>
    /// Count of null values in the column.  Writers SHOULD always write this field even if it is zero (i.e. no null value) or the column is not nullable. Readers MUST distinguish between null_count not being present and null_count == 0. If null_count is not present, readers MUST NOT assume null_count == 0.
    /// </summary>
    public long? NullCount { get; set; }

    /// <summary>
    /// Count of distinct values occurring.
    /// </summary>
    public long? DistinctCount { get; set; }

    /// <summary>
    /// Lower and upper bound values for the column, determined by its ColumnOrder.  These may be the actual minimum and maximum values found on a page or column chunk, but can also be (more compact) values that do not exist on a page or column chunk. For example, instead of storing &quot;Blart Versenwald III&quot;, a writer may set min_value=&quot;B&quot;, max_value=&quot;C&quot;. Such more compact values must still be valid values within the column&#39;s logical type.  Values are encoded using PLAIN encoding, except that variable-length byte arrays do not include a length prefix.
    /// </summary>
    public byte[]? MaxValue { get; set; }

    public byte[]? MinValue { get; set; }

    /// <summary>
    /// If true, max_value is the actual maximum value for a column.
    /// </summary>
    public bool? IsMaxValueExact { get; set; }

    /// <summary>
    /// If true, min_value is the actual minimum value for a column.
    /// </summary>
    public bool? IsMinValueExact { get; set; }


    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.StructBegin();

        // 1: Max, binary
        if(Max != null) {
            proto.WriteBinaryField(1, Max);
        }
        // 2: Min, binary
        if(Min != null) {
            proto.WriteBinaryField(2, Min);
        }
        // 3: NullCount, i64
        if(NullCount != null) {
            proto.WriteI64Field(3, NullCount.Value);
        }
        // 4: DistinctCount, i64
        if(DistinctCount != null) {
            proto.WriteI64Field(4, DistinctCount.Value);
        }
        // 5: MaxValue, binary
        if(MaxValue != null) {
            proto.WriteBinaryField(5, MaxValue);
        }
        // 6: MinValue, binary
        if(MinValue != null) {
            proto.WriteBinaryField(6, MinValue);
        }
        // 7: IsMaxValueExact, bool
        if(IsMaxValueExact != null) {
            proto.WriteBoolField(7, IsMaxValueExact.Value);
        }
        // 8: IsMinValueExact, bool
        if(IsMinValueExact != null) {
            proto.WriteBoolField(8, IsMinValueExact.Value);
        }

        proto.StructEnd();
    }

    internal static Statistics Read(ThriftCompactProtocolReader proto) {
        var r = new Statistics();
        proto.StructBegin();
        while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
            switch(fieldId) {
                case 1: // Max, binary
                    r.Max = proto.ReadBinary();
                    break;
                case 2: // Min, binary
                    r.Min = proto.ReadBinary();
                    break;
                case 3: // NullCount, i64
                    r.NullCount = proto.ReadI64();
                    break;
                case 4: // DistinctCount, i64
                    r.DistinctCount = proto.ReadI64();
                    break;
                case 5: // MaxValue, binary
                    r.MaxValue = proto.ReadBinary();
                    break;
                case 6: // MinValue, binary
                    r.MinValue = proto.ReadBinary();
                    break;
                case 7: // IsMaxValueExact, bool
                    r.IsMaxValueExact = compactType == CompactType.BooleanTrue;
                    break;
                case 8: // IsMinValueExact, bool
                    r.IsMinValueExact = compactType == CompactType.BooleanTrue;
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