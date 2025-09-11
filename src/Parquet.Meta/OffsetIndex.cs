using System.Collections.Generic;
using Parquet.Meta.Proto;

namespace Parquet.Meta;

/// <summary>
/// Optional offsets for each data page in a ColumnChunk.  Forms part of the page index, along with ColumnIndex.  OffsetIndex may be present even if ColumnIndex is not.
/// </summary>
public class OffsetIndex {
    /// <summary>
    /// PageLocations, ordered by increasing PageLocation.offset. It is required that page_locations[i].first_row_index &lt; page_locations[i+1].first_row_index.
    /// </summary>
    public List<PageLocation> PageLocations { get; set; } = new List<PageLocation>();

    /// <summary>
    /// Unencoded/uncompressed size for BYTE_ARRAY types.  See documention for unencoded_byte_array_data_bytes in SizeStatistics for more details on this field.
    /// </summary>
    public List<long>? UnencodedByteArrayDataBytes { get; set; }


    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.StructBegin();

        // 1: PageLocations, list
        proto.WriteListBegin(1, 12, PageLocations.Count);
        foreach(PageLocation element in PageLocations) {
            element.Write(proto);
        }
        // 2: UnencodedByteArrayDataBytes, list
        if(UnencodedByteArrayDataBytes != null) {
            proto.WriteListBegin(2, 6, UnencodedByteArrayDataBytes.Count);
            foreach(long element in UnencodedByteArrayDataBytes) {
                proto.WriteI64Value(element);
            }
        }

        proto.StructEnd();
    }

    internal static OffsetIndex Read(ThriftCompactProtocolReader proto) {
        var r = new OffsetIndex();
        proto.StructBegin();
        int elementCount = 0;
        while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
            switch(fieldId) {
                case 1: // PageLocations, list
                    elementCount = proto.ReadListHeader(out _);
                    r.PageLocations = new List<PageLocation>(elementCount);
                    for(int i = 0; i < elementCount; i++) { r.PageLocations.Add(PageLocation.Read(proto)); }
                    break;
                case 2: // UnencodedByteArrayDataBytes, list
                    elementCount = proto.ReadListHeader(out _);
                    r.UnencodedByteArrayDataBytes = new List<long>(elementCount);
                    for(int i = 0; i < elementCount; i++) { r.UnencodedByteArrayDataBytes.Add(proto.ReadI64()); }
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