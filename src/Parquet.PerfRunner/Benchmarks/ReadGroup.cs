using System.Diagnostics;
using BenchmarkDotNet.Attributes;
using Parquet.Schema;
using ParquetSharp;

namespace Parquet.PerfRunner.Benchmarks
{
    public class ReadGroupInt : ReadGroup<int>
    {
        public ReadGroupInt()
            : base((random, _) => random.Next()) { }
    }

    public class ReadGroupLong : ReadGroup<long>
    {
        public ReadGroupLong()
            : base((random, _) => random.NextInt64()) { }
    }

    public class ReadGroupDecimal : ReadGroup<decimal>
    {
        public ReadGroupDecimal()
            : base(GetRandomDecimal, LogicalType.Decimal(precision: 29, scale: 3)) { }

        private static decimal GetRandomDecimal(Random random, int i)
        {
            int n = random.Next();
            decimal sign = random.NextDouble() < 0.5 ? -1M : +1M;
            return sign * ((decimal) n * n * n) / 1000M;
        }
    }

    public class ReadGroup<T>
    {
        private static readonly string _filename = typeof(T).Name + "_timeseries.parquet";

        private readonly MemoryStream _memoryStream = new();
        private readonly T[] _values;
        
        public ReadGroup(Func<Random, int, T> generator, LogicalType? logicalType = null)
        {
            Console.WriteLine("Writing data...");

            var timer = Stopwatch.StartNew();
            var rand = new Random(12345678);

            _values = Enumerable.Range(0, 1_000_000)
                .Select(i => generator.Invoke(rand, i))
                .ToArray();

            var columns = new Column[]
            {
                logicalType != null ? new Column<T>("Value", logicalType) : new Column<T>("Value")
            };
            
            using (var fileWriter = new ParquetFileWriter(_filename, columns, Compression.Uncompressed))
            {
                using var rowGroupWriter = fileWriter.AppendRowGroup();
                using var valueWriter = rowGroupWriter.NextColumn().LogicalWriter<T>();
                valueWriter.WriteBatch(_values);
                fileWriter.Close();
            }

            Console.WriteLine("Wrote {0:N0} rows in {1:N2} sec", _values.Length, timer.Elapsed.TotalSeconds);
            Console.WriteLine();

            using(var fileReader = new FileStream(_filename, FileMode.Open)) {
                fileReader.CopyTo(_memoryStream);
            }
        }

        [Benchmark(Baseline = true)]
        public T[] ParquetSharp() {
            _memoryStream.Position = 0;
            using var fileReader = new ParquetFileReader(_memoryStream, null, true);
            using var groupReader = fileReader.RowGroup(0);
            using var columnReader = groupReader.Column(0).LogicalReader<T>();
            T[] results = columnReader.ReadAll((int)groupReader.MetaData.NumRows);

            if(results.Length != _values.Length)
                throw new InvalidOperationException("Row count mismatch for type " + typeof(T).Name + "in ParquetSharp");
            
            if (Check.Enabled)
            {
                Check.ArraysAreEqual(_values, results);
            }

            return results;
        }

        [Benchmark]
        public async Task<T[]> ParquetDotNet() {
            _memoryStream.Position = 0;
            using var parquetReader = await ParquetReader.CreateAsync(_memoryStream, null, true);
            var field = (DataField)parquetReader.Schema.Fields[0];
            var data = await parquetReader.RowGroups[0].ReadColumnAsync(field);
            var results = (T[])data.Data;

            if(results.Length != _values.Length)
                throw new InvalidOperationException("Row count mismatch for type " + typeof(T).Name + "in ParquetDotNet");

            if (Check.Enabled)
            {
                Check.ArraysAreEqual(_values, results);
            }

            return results;
        }
    }
}
