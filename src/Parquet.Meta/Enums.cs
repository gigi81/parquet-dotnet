#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member
namespace Parquet.Meta;

/// <summary>
/// Types supported by Parquet.  These types are intended to be used in combination with the encodings to control the on disk storage format. For example INT16 is not included as a type since a good encoding of INT32 would handle this.
/// </summary>
public enum Type {
    BOOLEAN = 0,

    INT32 = 1,

    INT64 = 2,

    INT96 = 3,

    FLOAT = 4,

    DOUBLE = 5,

    BYTE_ARRAY = 6,

    FIXED_LEN_BYTE_ARRAY = 7,

}

/// <summary>
/// DEPRECATED: Common types used by frameworks(e.g. hive, pig) using parquet. ConvertedType is superseded by LogicalType.  This enum should not be extended.  See LogicalTypes.md for conversion between ConvertedType and LogicalType.
/// </summary>
public enum ConvertedType {
    /// <summary>
    /// A BYTE_ARRAY actually contains UTF8 encoded chars.
    /// </summary>
    UTF8 = 0,

    /// <summary>
    /// A map is converted as an optional field containing a repeated key/value pair.
    /// </summary>
    MAP = 1,

    /// <summary>
    /// A key/value pair is converted into a group of two fields.
    /// </summary>
    MAP_KEY_VALUE = 2,

    /// <summary>
    /// A list is converted into an optional field containing a repeated field for its values.
    /// </summary>
    LIST = 3,

    /// <summary>
    /// An enum is converted into a BYTE_ARRAY field.
    /// </summary>
    ENUM = 4,

    /// <summary>
    /// A decimal value.  This may be used to annotate BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY primitive types. The underlying byte array stores the unscaled value encoded as two&#39;s complement using big-endian byte order (the most significant byte is the zeroth element). The value of the decimal is the value * 10^{-scale}.  This must be accompanied by a (maximum) precision and a scale in the SchemaElement. The precision specifies the number of digits in the decimal and the scale stores the location of the decimal point. For example 1.23 would have precision 3 (3 total digits) and scale 2 (the decimal point is 2 digits over).
    /// </summary>
    DECIMAL = 5,

    /// <summary>
    /// A Date  Stored as days since Unix epoch, encoded as the INT32 physical type.
    /// </summary>
    DATE = 6,

    /// <summary>
    /// A time  The total number of milliseconds since midnight.  The value is stored as an INT32 physical type.
    /// </summary>
    TIME_MILLIS = 7,

    /// <summary>
    /// A time.  The total number of microseconds since midnight.  The value is stored as an INT64 physical type.
    /// </summary>
    TIME_MICROS = 8,

    /// <summary>
    /// A date/time combination  Date and time recorded as milliseconds since the Unix epoch.  Recorded as a physical type of INT64.
    /// </summary>
    TIMESTAMP_MILLIS = 9,

    /// <summary>
    /// A date/time combination  Date and time recorded as microseconds since the Unix epoch.  The value is stored as an INT64 physical type.
    /// </summary>
    TIMESTAMP_MICROS = 10,

    /// <summary>
    /// An unsigned integer value.  The number describes the maximum number of meaningful data bits in the stored value. 8, 16 and 32 bit values are stored using the INT32 physical type.  64 bit values are stored using the INT64 physical type.
    /// </summary>
    UINT_8 = 11,

    UINT_16 = 12,

    UINT_32 = 13,

    UINT_64 = 14,

    /// <summary>
    /// A signed integer value.  The number describes the maximum number of meaningful data bits in the stored value. 8, 16 and 32 bit values are stored using the INT32 physical type.  64 bit values are stored using the INT64 physical type.
    /// </summary>
    INT_8 = 15,

    INT_16 = 16,

    INT_32 = 17,

    INT_64 = 18,

    /// <summary>
    /// An embedded JSON document  A JSON document embedded within a single UTF8 column.
    /// </summary>
    JSON = 19,

    /// <summary>
    /// An embedded BSON document  A BSON document embedded within a single BYTE_ARRAY column.
    /// </summary>
    BSON = 20,

    /// <summary>
    /// An interval of time  This type annotates data stored as a FIXED_LEN_BYTE_ARRAY of length 12 This data is composed of three separate little endian unsigned integers.  Each stores a component of a duration of time.  The first integer identifies the number of months associated with the duration, the second identifies the number of days associated with the duration and the third identifies the number of milliseconds associated with the provided duration.  This duration of time is independent of any particular timezone or date.
    /// </summary>
    INTERVAL = 21,

}

/// <summary>
/// Representation of Schemas.
/// </summary>
public enum FieldRepetitionType {
    /// <summary>
    /// This field is required (can not be null) and each row has exactly 1 value.
    /// </summary>
    REQUIRED = 0,

    /// <summary>
    /// The field is optional (can be null) and each row has 0 or 1 values.
    /// </summary>
    OPTIONAL = 1,

    /// <summary>
    /// The field is repeated and can contain 0 or more values.
    /// </summary>
    REPEATED = 2,

}

/// <summary>
/// Encodings supported by Parquet.  Not all encodings are valid for all types.  These enums are also used to specify the encoding of definition and repetition levels. See the accompanying doc for the details of the more complicated encodings.
/// </summary>
public enum Encoding {
    /// <summary>
    /// Default encoding. BOOLEAN - 1 bit per value. 0 is false; 1 is true. INT32 - 4 bytes per value.  Stored as little-endian. INT64 - 8 bytes per value.  Stored as little-endian. FLOAT - 4 bytes per value.  IEEE. Stored as little-endian. DOUBLE - 8 bytes per value.  IEEE. Stored as little-endian. BYTE_ARRAY - 4 byte length stored as little endian, followed by bytes. FIXED_LEN_BYTE_ARRAY - Just the bytes.
    /// </summary>
    PLAIN = 0,

    /// <summary>
    /// Deprecated: Dictionary encoding. The values in the dictionary are encoded in the plain type. in a data page use RLE_DICTIONARY instead. in a Dictionary page use PLAIN instead.
    /// </summary>
    PLAIN_DICTIONARY = 2,

    /// <summary>
    /// Group packed run length encoding. Usable for definition/repetition levels encoding and Booleans (on one bit: 0 is false; 1 is true.).
    /// </summary>
    RLE = 3,

    /// <summary>
    /// Bit packed encoding.  This can only be used if the data has a known max width.  Usable for definition/repetition levels encoding.
    /// </summary>
    BIT_PACKED = 4,

    /// <summary>
    /// Delta encoding for integers. This can be used for int columns and works best on sorted data.
    /// </summary>
    DELTA_BINARY_PACKED = 5,

    /// <summary>
    /// Encoding for byte arrays to separate the length values and the data. The lengths are encoded using DELTA_BINARY_PACKED.
    /// </summary>
    DELTA_LENGTH_BYTE_ARRAY = 6,

    /// <summary>
    /// Incremental-encoded byte array. Prefix lengths are encoded using DELTA_BINARY_PACKED. Suffixes are stored as delta length byte arrays.
    /// </summary>
    DELTA_BYTE_ARRAY = 7,

    /// <summary>
    /// Dictionary encoding: the ids are encoded using the RLE encoding.
    /// </summary>
    RLE_DICTIONARY = 8,

    /// <summary>
    /// Encoding for fixed-width data (FLOAT, DOUBLE, INT32, INT64, FIXED_LEN_BYTE_ARRAY). K byte-streams are created where K is the size in bytes of the data type. The individual bytes of a value are scattered to the corresponding stream and the streams are concatenated. This itself does not reduce the size of the data but can lead to better compression afterwards.  Added in 2.8 for FLOAT and DOUBLE. Support for INT32, INT64 and FIXED_LEN_BYTE_ARRAY added in 2.11.
    /// </summary>
    BYTE_STREAM_SPLIT = 9,

}

/// <summary>
/// Supported compression algorithms.  Codecs added in format version X.Y can be read by readers based on X.Y and later. Codec support may vary between readers based on the format version and libraries available at runtime.  See Compression.md for a detailed specification of these algorithms.
/// </summary>
public enum CompressionCodec {
    UNCOMPRESSED = 0,

    SNAPPY = 1,

    GZIP = 2,

    LZO = 3,

    BROTLI = 4,

    LZ4 = 5,

    ZSTD = 6,

    LZ4_RAW = 7,

}

public enum PageType {
    DATA_PAGE = 0,

    INDEX_PAGE = 1,

    DICTIONARY_PAGE = 2,

    DATA_PAGE_V2 = 3,

}

/// <summary>
/// Enum to annotate whether lists of min/max elements inside ColumnIndex are ordered and if so, in which direction.
/// </summary>
public enum BoundaryOrder {
    UNORDERED = 0,

    ASCENDING = 1,

    DESCENDING = 2,

}