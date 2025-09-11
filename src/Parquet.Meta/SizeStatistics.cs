using System.Collections.Generic;
using Parquet.Meta.Proto;

namespace Parquet.Meta;
#pragma warning disable CS1591
/// <summary>
/// A structure for capturing metadata for estimating the unencoded, uncompressed size of data written. This is useful for readers to estimate how much memory is needed to reconstruct data in their memory model and for fine grained filter pushdown on nested structures (the histograms contained in this structure can help determine the number of nulls at a particular nesting level and maximum length of lists).
/// </summary>
public class SizeStatistics {
    /// <summary>
    /// The number of physical bytes stored for BYTE_ARRAY data values assuming no encoding. This is exclusive of the bytes needed to store the length of each byte array. In other words, this field is equivalent to the `(size of PLAIN-ENCODING the byte array values) - (4 bytes * number of values written)`. To determine unencoded sizes of other types readers can use schema information multiplied by the number of non-null and null values. The number of null/non-null values can be inferred from the histograms below.  For example, if a column chunk is dictionary-encoded with dictionary [&quot;a&quot;, &quot;bc&quot;, &quot;cde&quot;], and a data page contains the indices [0, 0, 1, 2], then this value for that data page should be 7 (1 + 1 + 2 + 3).  This field should only be set for types that use BYTE_ARRAY as their physical type.
    /// </summary>
    public long? UnencodedByteArrayDataBytes { get; set; }

    /// <summary>
    /// When present, there is expected to be one element corresponding to each repetition (i.e. size=max repetition_level+1) where each element represents the number of times the repetition level was observed in the data.  This field may be omitted if max_repetition_level is 0 without loss of information.
    /// </summary>
    public List<long>? RepetitionLevelHistogram { get; set; }

    /// <summary>
    /// Same as repetition_level_histogram except for definition levels.  This field may be omitted if max_definition_level is 0 or 1 without loss of information.
    /// </summary>
    public List<long>? DefinitionLevelHistogram { get; set; }


    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.StructBegin();

        // 1: UnencodedByteArrayDataBytes, i64
        if(UnencodedByteArrayDataBytes != null) {
            proto.WriteI64Field(1, UnencodedByteArrayDataBytes.Value);
        }
        // 2: RepetitionLevelHistogram, list
        if(RepetitionLevelHistogram != null) {
            proto.WriteListBegin(2, 6, RepetitionLevelHistogram.Count);
            foreach(long element in RepetitionLevelHistogram) {
                proto.WriteI64Value(element);
            }
        }
        // 3: DefinitionLevelHistogram, list
        if(DefinitionLevelHistogram != null) {
            proto.WriteListBegin(3, 6, DefinitionLevelHistogram.Count);
            foreach(long element in DefinitionLevelHistogram) {
                proto.WriteI64Value(element);
            }
        }

        proto.StructEnd();
    }

    internal static SizeStatistics Read(ThriftCompactProtocolReader proto) {
        var r = new SizeStatistics();
        proto.StructBegin();
        int elementCount = 0;
        while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
            switch(fieldId) {
                case 1: // UnencodedByteArrayDataBytes, i64
                    r.UnencodedByteArrayDataBytes = proto.ReadI64();
                    break;
                case 2: // RepetitionLevelHistogram, list
                    elementCount = proto.ReadListHeader(out _);
                    r.RepetitionLevelHistogram = new List<long>(elementCount);
                    for(int i = 0; i < elementCount; i++) { r.RepetitionLevelHistogram.Add(proto.ReadI64()); }
                    break;
                case 3: // DefinitionLevelHistogram, list
                    elementCount = proto.ReadListHeader(out _);
                    r.DefinitionLevelHistogram = new List<long>(elementCount);
                    for(int i = 0; i < elementCount; i++) { r.DefinitionLevelHistogram.Add(proto.ReadI64()); }
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