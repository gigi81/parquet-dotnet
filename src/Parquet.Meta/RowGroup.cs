using System.Collections.Generic;
using Parquet.Meta.Proto;

namespace Parquet.Meta;

public class RowGroup {
    /// <summary>
    /// Metadata for each column chunk in this row group. This list must have the same order as the SchemaElement list in FileMetaData.
    /// </summary>
    public List<ColumnChunk> Columns { get; set; } = new List<ColumnChunk>();

    /// <summary>
    /// Total byte size of all the uncompressed column data in this row group.
    /// </summary>
    public long TotalByteSize { get; set; }

    /// <summary>
    /// Number of rows in this row group.
    /// </summary>
    public long NumRows { get; set; }

    /// <summary>
    /// If set, specifies a sort ordering of the rows in this RowGroup. The sorting columns can be a subset of all the columns.
    /// </summary>
    public List<SortingColumn>? SortingColumns { get; set; }

    /// <summary>
    /// Byte offset from beginning of file to first page (data or dictionary) in this row group.
    /// </summary>
    public long? FileOffset { get; set; }

    /// <summary>
    /// Total byte size of all compressed (and potentially encrypted) column data in this row group.
    /// </summary>
    public long? TotalCompressedSize { get; set; }

    /// <summary>
    /// Row group ordinal in the file.
    /// </summary>
    public short? Ordinal { get; set; }


    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.StructBegin();

        // 1: Columns, list
        proto.WriteListBegin(1, 12, Columns.Count);
        foreach(ColumnChunk element in Columns) {
            element.Write(proto);
        }
        // 2: TotalByteSize, i64
        proto.WriteI64Field(2, TotalByteSize);
        // 3: NumRows, i64
        proto.WriteI64Field(3, NumRows);
        // 4: SortingColumns, list
        if(SortingColumns != null) {
            proto.WriteListBegin(4, 12, SortingColumns.Count);
            foreach(SortingColumn element in SortingColumns) {
                element.Write(proto);
            }
        }
        // 5: FileOffset, i64
        if(FileOffset != null) {
            proto.WriteI64Field(5, FileOffset.Value);
        }
        // 6: TotalCompressedSize, i64
        if(TotalCompressedSize != null) {
            proto.WriteI64Field(6, TotalCompressedSize.Value);
        }
        // 7: Ordinal, i16
        if(Ordinal != null) {
            proto.WriteI16Field(7, Ordinal.Value);
        }

        proto.StructEnd();
    }

    internal static RowGroup Read(ThriftCompactProtocolReader proto) {
        var r = new RowGroup();
        proto.StructBegin();
        int elementCount = 0;
        while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
            switch(fieldId) {
                case 1: // Columns, list
                    elementCount = proto.ReadListHeader(out _);
                    r.Columns = new List<ColumnChunk>(elementCount);
                    for(int i = 0; i < elementCount; i++) { r.Columns.Add(ColumnChunk.Read(proto)); }
                    break;
                case 2: // TotalByteSize, i64
                    r.TotalByteSize = proto.ReadI64();
                    break;
                case 3: // NumRows, i64
                    r.NumRows = proto.ReadI64();
                    break;
                case 4: // SortingColumns, list
                    elementCount = proto.ReadListHeader(out _);
                    r.SortingColumns = new List<SortingColumn>(elementCount);
                    for(int i = 0; i < elementCount; i++) { r.SortingColumns.Add(SortingColumn.Read(proto)); }
                    break;
                case 5: // FileOffset, i64
                    r.FileOffset = proto.ReadI64();
                    break;
                case 6: // TotalCompressedSize, i64
                    r.TotalCompressedSize = proto.ReadI64();
                    break;
                case 7: // Ordinal, i16
                    r.Ordinal = proto.ReadI16();
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