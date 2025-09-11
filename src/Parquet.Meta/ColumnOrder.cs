using Parquet.Meta.Proto;

namespace Parquet.Meta;

/// <summary>
/// Union to specify the order used for the min_value and max_value fields for a column. This union takes the role of an enhanced enum that allows rich elements (which will be needed for a collation-based ordering in the future).  Possible values are: * TypeDefinedOrder - the column uses the order defined by its logical or                      physical type (if there is no logical type).  If the reader does not support the value of this union, min and max stats for this column should be ignored.
/// </summary>
public class ColumnOrder {
    /// <summary>
    /// The sort orders for logical types are:   UTF8 - unsigned byte-wise comparison   INT8 - signed comparison   INT16 - signed comparison   INT32 - signed comparison   INT64 - signed comparison   UINT8 - unsigned comparison   UINT16 - unsigned comparison   UINT32 - unsigned comparison   UINT64 - unsigned comparison   DECIMAL - signed comparison of the represented value   DATE - signed comparison   TIME_MILLIS - signed comparison   TIME_MICROS - signed comparison   TIMESTAMP_MILLIS - signed comparison   TIMESTAMP_MICROS - signed comparison   INTERVAL - undefined   JSON - unsigned byte-wise comparison   BSON - unsigned byte-wise comparison   ENUM - unsigned byte-wise comparison   LIST - undefined   MAP - undefined   VARIANT - undefined  In the absence of logical types, the sort order is determined by the physical type:   BOOLEAN - false, true   INT32 - signed comparison   INT64 - signed comparison   INT96 (only used for legacy timestamps) - undefined   FLOAT - signed comparison of the represented value (*)   DOUBLE - signed comparison of the represented value (*)   BYTE_ARRAY - unsigned byte-wise comparison   FIXED_LEN_BYTE_ARRAY - unsigned byte-wise comparison  (*) Because the sorting order is not specified properly for floating     point values (relations vs. total ordering) the following     compatibility rules should be applied when reading statistics:     - If the min is a NaN, it should be ignored.     - If the max is a NaN, it should be ignored.     - If the min is +0, the row group may contain -0 values as well.     - If the max is -0, the row group may contain +0 values as well.     - When looking for NaN values, min and max should be ignored.      When writing statistics the following rules should be followed:     - NaNs should not be written to min or max statistics fields.     - If the computed max value is zero (whether negative or positive),       `+0.0` should be written into the max statistics field.     - If the computed min value is zero (whether negative or positive),       `-0.0` should be written into the min statistics field.
    /// </summary>
    public TypeDefinedOrder? TYPEORDER { get; set; }


    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.StructBegin();

        // 1: TYPEORDER, id
        if(TYPEORDER != null) {
            proto.BeginInlineStruct(1);
            TYPEORDER.Write(proto);
        }

        proto.StructEnd();
    }

    internal static ColumnOrder Read(ThriftCompactProtocolReader proto) {
        var r = new ColumnOrder();
        proto.StructBegin();
        while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
            switch(fieldId) {
                case 1: // TYPEORDER, id
                    r.TYPEORDER = TypeDefinedOrder.Read(proto);
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