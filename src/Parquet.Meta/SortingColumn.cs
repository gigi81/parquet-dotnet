using Parquet.Meta.Proto;

namespace Parquet.Meta;

/// <summary>
/// Sort order within a RowGroup of a leaf column.
/// </summary>
public class SortingColumn {
    /// <summary>
    /// The ordinal position of the column (in this row group).
    /// </summary>
    public int ColumnIdx { get; set; }

    /// <summary>
    /// If true, indicates this column is sorted in descending order.
    /// </summary>
    public bool Descending { get; set; }

    /// <summary>
    /// If true, nulls will come before non-null values, otherwise, nulls go at the end.
    /// </summary>
    public bool NullsFirst { get; set; }


    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.StructBegin();

        // 1: ColumnIdx, i32
        proto.WriteI32Field(1, ColumnIdx);
        // 2: Descending, bool
        proto.WriteBoolField(2, Descending);
        // 3: NullsFirst, bool
        proto.WriteBoolField(3, NullsFirst);

        proto.StructEnd();
    }

    internal static SortingColumn Read(ThriftCompactProtocolReader proto) {
        var r = new SortingColumn();
        proto.StructBegin();
        while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
            switch(fieldId) {
                case 1: // ColumnIdx, i32
                    r.ColumnIdx = proto.ReadI32();
                    break;
                case 2: // Descending, bool
                    r.Descending = compactType == CompactType.BooleanTrue;
                    break;
                case 3: // NullsFirst, bool
                    r.NullsFirst = compactType == CompactType.BooleanTrue;
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