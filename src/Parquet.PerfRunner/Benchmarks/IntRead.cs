using System.Diagnostics;
using BenchmarkDotNet.Attributes;
using Parquet.Schema;
using ParquetSharp;

namespace Parquet.PerfRunner.Benchmarks
{
    public class IntRead
    {
        private readonly MemoryStream _memoryStream = new();
        
        public IntRead()
        {
            Console.WriteLine("Writing data...");

            var timer = Stopwatch.StartNew();
            var rand = new Random(123);

            _values = Enumerable.Range(0, 1_000_000).Select(i =>
            {
                return rand.Next();
            }).ToArray();

            var columns = new Column[]
            {
                new Column<int>("Value")
            };
            
            using (var fileWriter = new ParquetFileWriter(Filename, columns, Compression.Uncompressed))
            {
                using var rowGroupWriter = fileWriter.AppendRowGroup();
                using var valueWriter = rowGroupWriter.NextColumn().LogicalWriter<int>();
                valueWriter.WriteBatch(_values);
                fileWriter.Close();
            }

            Console.WriteLine("Wrote {0:N0} rows in {1:N2} sec", _values.Length, timer.Elapsed.TotalSeconds);
            Console.WriteLine();

            using(var fileReader = new FileStream(Filename, FileMode.Open)) {
                fileReader.CopyTo(_memoryStream);
            }
        }

        [Benchmark(Baseline = true)]
        public int[] ParquetSharp() {
            _memoryStream.Position = 0;
            using var fileReader = new ParquetFileReader(_memoryStream, null, true);
            using var groupReader = fileReader.RowGroup(0);
            using var dateTimeReader = groupReader.Column(0).LogicalReader<int>();
            int[] results = dateTimeReader.ReadAll(_values.Length);

            if (Check.Enabled)
            {
                Check.ArraysAreEqual(_values, results);
            }

            return results;
        }

        [Benchmark]
        public async Task<int[]> ParquetDotNet() {
            _memoryStream.Position = 0;
            using var parquetReader = await ParquetReader.CreateAsync(_memoryStream, null, true);
            var field = (DataField)parquetReader.Schema.Fields[0];
            var data = await parquetReader.RowGroups[0].ReadColumnAsync(field);
            int[] results = (int[])data.Data;

            if (Check.Enabled)
            {
                Check.ArraysAreEqual(_values, results);
            }

            return results;
        }

        private const string Filename = "int_timeseries.parquet";

        private readonly int[] _values;
    }
}
