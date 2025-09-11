using System.Collections.Generic;
using Parquet.Meta.Proto;

namespace Parquet.Meta;

/// <summary>
/// Description for column metadata.
/// </summary>
public class ColumnMetaData {
    /// <summary>
    /// Type of this column.
    /// </summary>
    public Type Type { get; set; } = new Type();

    /// <summary>
    /// Set of all encodings used for this column. The purpose is to validate whether we can decode those pages.
    /// </summary>
    public List<Encoding> Encodings { get; set; } = new List<Encoding>();

    /// <summary>
    /// Path in schema.
    /// </summary>
    public List<string> PathInSchema { get; set; } = new List<string>();

    /// <summary>
    /// Compression codec.
    /// </summary>
    public CompressionCodec Codec { get; set; } = new CompressionCodec();

    /// <summary>
    /// Number of values in this column.
    /// </summary>
    public long NumValues { get; set; }

    /// <summary>
    /// Total byte size of all uncompressed pages in this column chunk (including the headers).
    /// </summary>
    public long TotalUncompressedSize { get; set; }

    /// <summary>
    /// Total byte size of all compressed, and potentially encrypted, pages in this column chunk (including the headers).
    /// </summary>
    public long TotalCompressedSize { get; set; }

    /// <summary>
    /// Optional key/value metadata.
    /// </summary>
    public List<KeyValue>? KeyValueMetadata { get; set; }

    /// <summary>
    /// Byte offset from beginning of file to first data page.
    /// </summary>
    public long DataPageOffset { get; set; }

    /// <summary>
    /// Byte offset from beginning of file to root index page.
    /// </summary>
    public long? IndexPageOffset { get; set; }

    /// <summary>
    /// Byte offset from the beginning of file to first (only) dictionary page.
    /// </summary>
    public long? DictionaryPageOffset { get; set; }

    /// <summary>
    /// Optional statistics for this column chunk.
    /// </summary>
    public Statistics? Statistics { get; set; }

    /// <summary>
    /// Set of all encodings used for pages in this column chunk. This information can be used to determine if all data pages are dictionary encoded for example.
    /// </summary>
    public List<PageEncodingStats>? EncodingStats { get; set; }

    /// <summary>
    /// Byte offset from beginning of file to Bloom filter data.
    /// </summary>
    public long? BloomFilterOffset { get; set; }

    /// <summary>
    /// Size of Bloom filter data including the serialized header, in bytes. Added in 2.10 so readers may not read this field from old files and it can be obtained after the BloomFilterHeader has been deserialized. Writers should write this field so readers can read the bloom filter in a single I/O.
    /// </summary>
    public int? BloomFilterLength { get; set; }

    /// <summary>
    /// Optional statistics to help estimate total memory when converted to in-memory representations. The histograms contained in these statistics can also be useful in some cases for more fine-grained nullability/list length filter pushdown.
    /// </summary>
    public SizeStatistics? SizeStatistics { get; set; }


    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.StructBegin();

        // 1: Type, id
        proto.WriteI32Field(1, (int)Type);
        // 2: Encodings, list
        proto.WriteListBegin(2, 5, Encodings.Count);
        foreach(Encoding element in Encodings) {
            proto.WriteI32Value((int)element);
        }
        // 3: PathInSchema, list
        proto.WriteListBegin(3, 8, PathInSchema.Count);
        foreach(string element in PathInSchema) {
            proto.WriteStringValue(element);
        }
        // 4: Codec, id
        proto.WriteI32Field(4, (int)Codec);
        // 5: NumValues, i64
        proto.WriteI64Field(5, NumValues);
        // 6: TotalUncompressedSize, i64
        proto.WriteI64Field(6, TotalUncompressedSize);
        // 7: TotalCompressedSize, i64
        proto.WriteI64Field(7, TotalCompressedSize);
        // 8: KeyValueMetadata, list
        if(KeyValueMetadata != null) {
            proto.WriteListBegin(8, 12, KeyValueMetadata.Count);
            foreach(KeyValue element in KeyValueMetadata) {
                element.Write(proto);
            }
        }
        // 9: DataPageOffset, i64
        proto.WriteI64Field(9, DataPageOffset);
        // 10: IndexPageOffset, i64
        if(IndexPageOffset != null) {
            proto.WriteI64Field(10, IndexPageOffset.Value);
        }
        // 11: DictionaryPageOffset, i64
        if(DictionaryPageOffset != null) {
            proto.WriteI64Field(11, DictionaryPageOffset.Value);
        }
        // 12: Statistics, id
        if(Statistics != null) {
            proto.BeginInlineStruct(12);
            Statistics.Write(proto);
        }
        // 13: EncodingStats, list
        if(EncodingStats != null) {
            proto.WriteListBegin(13, 12, EncodingStats.Count);
            foreach(PageEncodingStats element in EncodingStats) {
                element.Write(proto);
            }
        }
        // 14: BloomFilterOffset, i64
        if(BloomFilterOffset != null) {
            proto.WriteI64Field(14, BloomFilterOffset.Value);
        }
        // 15: BloomFilterLength, i32
        if(BloomFilterLength != null) {
            proto.WriteI32Field(15, BloomFilterLength.Value);
        }
        // 16: SizeStatistics, id
        if(SizeStatistics != null) {
            proto.BeginInlineStruct(16);
            SizeStatistics.Write(proto);
        }

        proto.StructEnd();
    }

    internal static ColumnMetaData Read(ThriftCompactProtocolReader proto) {
        var r = new ColumnMetaData();
        proto.StructBegin();
        int elementCount = 0;
        while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
            switch(fieldId) {
                case 1: // Type, id
                    r.Type = (Type)proto.ReadI32();
                    break;
                case 2: // Encodings, list
                    elementCount = proto.ReadListHeader(out _);
                    r.Encodings = new List<Encoding>(elementCount);
                    for(int i = 0; i < elementCount; i++) { r.Encodings.Add((Encoding)proto.ReadI32()); }
                    break;
                case 3: // PathInSchema, list
                    elementCount = proto.ReadListHeader(out _);
                    r.PathInSchema = new List<string>(elementCount);
                    for(int i = 0; i < elementCount; i++) { r.PathInSchema.Add(proto.ReadString()); }
                    break;
                case 4: // Codec, id
                    r.Codec = (CompressionCodec)proto.ReadI32();
                    break;
                case 5: // NumValues, i64
                    r.NumValues = proto.ReadI64();
                    break;
                case 6: // TotalUncompressedSize, i64
                    r.TotalUncompressedSize = proto.ReadI64();
                    break;
                case 7: // TotalCompressedSize, i64
                    r.TotalCompressedSize = proto.ReadI64();
                    break;
                case 8: // KeyValueMetadata, list
                    elementCount = proto.ReadListHeader(out _);
                    r.KeyValueMetadata = new List<KeyValue>(elementCount);
                    for(int i = 0; i < elementCount; i++) { r.KeyValueMetadata.Add(KeyValue.Read(proto)); }
                    break;
                case 9: // DataPageOffset, i64
                    r.DataPageOffset = proto.ReadI64();
                    break;
                case 10: // IndexPageOffset, i64
                    r.IndexPageOffset = proto.ReadI64();
                    break;
                case 11: // DictionaryPageOffset, i64
                    r.DictionaryPageOffset = proto.ReadI64();
                    break;
                case 12: // Statistics, id
                    r.Statistics = Statistics.Read(proto);
                    break;
                case 13: // EncodingStats, list
                    elementCount = proto.ReadListHeader(out _);
                    r.EncodingStats = new List<PageEncodingStats>(elementCount);
                    for(int i = 0; i < elementCount; i++) { r.EncodingStats.Add(PageEncodingStats.Read(proto)); }
                    break;
                case 14: // BloomFilterOffset, i64
                    r.BloomFilterOffset = proto.ReadI64();
                    break;
                case 15: // BloomFilterLength, i32
                    r.BloomFilterLength = proto.ReadI32();
                    break;
                case 16: // SizeStatistics, id
                    r.SizeStatistics = SizeStatistics.Read(proto);
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