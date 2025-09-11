using Parquet.Meta.Proto;

namespace Parquet.Meta;

/// <summary>
/// Represents a element inside a schema definition.  - if it is a group (inner node) then type is undefined and num_children is defined  - if it is a primitive type (leaf) then type is defined and num_children is undefined the nodes are listed in depth first traversal order.
/// </summary>
public class SchemaElement {
    /// <summary>
    /// Data type for this field. Not set if the current element is a non-leaf node.
    /// </summary>
    public Type? Type { get; set; }

    /// <summary>
    /// If type is FIXED_LEN_BYTE_ARRAY, this is the byte length of the values. Otherwise, if specified, this is the maximum bit length to store any of the values. (e.g. a low cardinality INT col could have this set to 3).  Note that this is in the schema, and therefore fixed for the entire file.
    /// </summary>
    public int? TypeLength { get; set; }

    /// <summary>
    /// Repetition of the field. The root of the schema does not have a repetition_type. All other nodes must have one.
    /// </summary>
    public FieldRepetitionType? RepetitionType { get; set; }

    /// <summary>
    /// Name of the field in the schema.
    /// </summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>
    /// Nested fields.  Since thrift does not support nested fields, the nesting is flattened to a single list by a depth-first traversal. The children count is used to construct the nested relationship. This field is not set when the element is a primitive type.
    /// </summary>
    public int? NumChildren { get; set; }

    /// <summary>
    /// DEPRECATED: When the schema is the result of a conversion from another model. Used to record the original type to help with cross conversion.  This is superseded by logicalType.
    /// </summary>
    public ConvertedType? ConvertedType { get; set; }

    /// <summary>
    /// DEPRECATED: Used when this column contains decimal data. See the DECIMAL converted type for more details.  This is superseded by using the DecimalType annotation in logicalType.
    /// </summary>
    public int? Scale { get; set; }

    public int? Precision { get; set; }

    /// <summary>
    /// When the original schema supports field ids, this will save the original field id in the parquet schema.
    /// </summary>
    public int? FieldId { get; set; }

    /// <summary>
    /// The logical type of this SchemaElement  LogicalType replaces ConvertedType, but ConvertedType is still required for some logical types to ensure forward-compatibility in format v1.
    /// </summary>
    public LogicalType? LogicalType { get; set; }


    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.StructBegin();

        // 1: Type, id
        if(Type != null) {
            proto.WriteI32Field(1, (int)Type);
        }
        // 2: TypeLength, i32
        if(TypeLength != null) {
            proto.WriteI32Field(2, TypeLength.Value);
        }
        // 3: RepetitionType, id
        if(RepetitionType != null) {
            proto.WriteI32Field(3, (int)RepetitionType);
        }
        // 4: Name, string
        proto.WriteStringField(4, Name ?? string.Empty);
        // 5: NumChildren, i32
        if(NumChildren != null) {
            proto.WriteI32Field(5, NumChildren.Value);
        }
        // 6: ConvertedType, id
        if(ConvertedType != null) {
            proto.WriteI32Field(6, (int)ConvertedType);
        }
        // 7: Scale, i32
        if(Scale != null) {
            proto.WriteI32Field(7, Scale.Value);
        }
        // 8: Precision, i32
        if(Precision != null) {
            proto.WriteI32Field(8, Precision.Value);
        }
        // 9: FieldId, i32
        if(FieldId != null) {
            proto.WriteI32Field(9, FieldId.Value);
        }
        // 10: LogicalType, id
        if(LogicalType != null) {
            proto.BeginInlineStruct(10);
            LogicalType.Write(proto);
        }

        proto.StructEnd();
    }

    internal static SchemaElement Read(ThriftCompactProtocolReader proto) {
        var r = new SchemaElement();
        proto.StructBegin();
        while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
            switch(fieldId) {
                case 1: // Type, id
                    r.Type = (Type)proto.ReadI32();
                    break;
                case 2: // TypeLength, i32
                    r.TypeLength = proto.ReadI32();
                    break;
                case 3: // RepetitionType, id
                    r.RepetitionType = (FieldRepetitionType)proto.ReadI32();
                    break;
                case 4: // Name, string
                    r.Name = proto.ReadString();
                    break;
                case 5: // NumChildren, i32
                    r.NumChildren = proto.ReadI32();
                    break;
                case 6: // ConvertedType, id
                    r.ConvertedType = (ConvertedType)proto.ReadI32();
                    break;
                case 7: // Scale, i32
                    r.Scale = proto.ReadI32();
                    break;
                case 8: // Precision, i32
                    r.Precision = proto.ReadI32();
                    break;
                case 9: // FieldId, i32
                    r.FieldId = proto.ReadI32();
                    break;
                case 10: // LogicalType, id
                    r.LogicalType = LogicalType.Read(proto);
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