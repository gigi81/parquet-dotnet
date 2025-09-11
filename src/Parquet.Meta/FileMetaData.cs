using System.Collections.Generic;
using Parquet.Meta.Proto;

namespace Parquet.Meta;

/// <summary>
/// Description for file metadata.
/// </summary>
public class FileMetaData {
    /// <summary>
    /// Version of this file.
    /// </summary>
    public int Version { get; set; }

    /// <summary>
    /// Parquet schema for this file.  This schema contains metadata for all the columns. The schema is represented as a tree with a single root.  The nodes of the tree are flattened to a list by doing a depth-first traversal. The column metadata contains the path in the schema for that column which can be used to map columns to nodes in the schema. The first element is the root.
    /// </summary>
    public List<SchemaElement> Schema { get; set; } = new List<SchemaElement>();

    /// <summary>
    /// Number of rows in this file.
    /// </summary>
    public long NumRows { get; set; }

    /// <summary>
    /// Row groups in this file.
    /// </summary>
    public List<RowGroup> RowGroups { get; set; } = new List<RowGroup>();

    /// <summary>
    /// Optional key/value metadata.
    /// </summary>
    public List<KeyValue>? KeyValueMetadata { get; set; }

    /// <summary>
    /// String for application that wrote this file.  This should be in the format &lt;Application&gt; version &lt;App Version&gt; (build &lt;App Build Hash&gt;). e.g. impala version 1.0 (build 6cf94d29b2b7115df4de2c06e2ab4326d721eb55).
    /// </summary>
    public string? CreatedBy { get; set; }

    /// <summary>
    /// Sort order used for the min_value and max_value fields in the Statistics objects and the min_values and max_values fields in the ColumnIndex objects of each column in this file. Sort orders are listed in the order matching the columns in the schema. The indexes are not necessary the same though, because only leaf nodes of the schema are represented in the list of sort orders.  Without column_orders, the meaning of the min_value and max_value fields in the Statistics object and the ColumnIndex object is undefined. To ensure well-defined behaviour, if these fields are written to a Parquet file, column_orders must be written as well.  The obsolete min and max fields in the Statistics object are always sorted by signed comparison regardless of column_orders.
    /// </summary>
    public List<ColumnOrder>? ColumnOrders { get; set; }

    /// <summary>
    /// Encryption algorithm. This field is set only in encrypted files with plaintext footer. Files with encrypted footer store algorithm id in FileCryptoMetaData structure.
    /// </summary>
    public EncryptionAlgorithm? EncryptionAlgorithm { get; set; }

    /// <summary>
    /// Retrieval metadata of key used for signing the footer. Used only in encrypted files with plaintext footer.
    /// </summary>
    public byte[]? FooterSigningKeyMetadata { get; set; }
    
    public void Write(ThriftCompactProtocolWriter proto) {
        proto.StructBegin();

        // 1: Version, i32
        proto.WriteI32Field(1, Version);
        // 2: Schema, list
        proto.WriteListBegin(2, 12, Schema.Count);
        foreach(SchemaElement element in Schema) {
            element.Write(proto);
        }
        // 3: NumRows, i64
        proto.WriteI64Field(3, NumRows);
        // 4: RowGroups, list
        proto.WriteListBegin(4, 12, RowGroups.Count);
        foreach(RowGroup element in RowGroups) {
            element.Write(proto);
        }
        // 5: KeyValueMetadata, list
        if(KeyValueMetadata != null) {
            proto.WriteListBegin(5, 12, KeyValueMetadata.Count);
            foreach(KeyValue element in KeyValueMetadata) {
                element.Write(proto);
            }
        }
        // 6: CreatedBy, string
        if(CreatedBy != null) {
            proto.WriteStringField(6, CreatedBy);
        }
        // 7: ColumnOrders, list
        if(ColumnOrders != null) {
            proto.WriteListBegin(7, 12, ColumnOrders.Count);
            foreach(ColumnOrder element in ColumnOrders) {
                element.Write(proto);
            }
        }
        // 8: EncryptionAlgorithm, id
        if(EncryptionAlgorithm != null) {
            proto.BeginInlineStruct(8);
            EncryptionAlgorithm.Write(proto);
        }
        // 9: FooterSigningKeyMetadata, binary
        if(FooterSigningKeyMetadata != null) {
            proto.WriteBinaryField(9, FooterSigningKeyMetadata);
        }

        proto.StructEnd();
    }

    public static FileMetaData Read(ThriftCompactProtocolReader proto) {
        var r = new FileMetaData();
        proto.StructBegin();
        int elementCount = 0;
        while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
            switch(fieldId) {
                case 1: // Version, i32
                    r.Version = proto.ReadI32();
                    break;
                case 2: // Schema, list
                    elementCount = proto.ReadListHeader(out _);
                    r.Schema = new List<SchemaElement>(elementCount);
                    for(int i = 0; i < elementCount; i++) { r.Schema.Add(SchemaElement.Read(proto)); }
                    break;
                case 3: // NumRows, i64
                    r.NumRows = proto.ReadI64();
                    break;
                case 4: // RowGroups, list
                    elementCount = proto.ReadListHeader(out _);
                    r.RowGroups = new List<RowGroup>(elementCount);
                    for(int i = 0; i < elementCount; i++) { r.RowGroups.Add(RowGroup.Read(proto)); }
                    break;
                case 5: // KeyValueMetadata, list
                    elementCount = proto.ReadListHeader(out _);
                    r.KeyValueMetadata = new List<KeyValue>(elementCount);
                    for(int i = 0; i < elementCount; i++) { r.KeyValueMetadata.Add(KeyValue.Read(proto)); }
                    break;
                case 6: // CreatedBy, string
                    r.CreatedBy = proto.ReadString();
                    break;
                case 7: // ColumnOrders, list
                    elementCount = proto.ReadListHeader(out _);
                    r.ColumnOrders = new List<ColumnOrder>(elementCount);
                    for(int i = 0; i < elementCount; i++) { r.ColumnOrders.Add(ColumnOrder.Read(proto)); }
                    break;
                case 8: // EncryptionAlgorithm, id
                    r.EncryptionAlgorithm = EncryptionAlgorithm.Read(proto);
                    break;
                case 9: // FooterSigningKeyMetadata, binary
                    r.FooterSigningKeyMetadata = proto.ReadBinary();
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