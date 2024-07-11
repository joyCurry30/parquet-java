package org.wtz.parquet;

import java.io.IOException;
import java.nio.file.Paths;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;

public class _01_Parquet_2948 {
  public static void main(String[] args) throws IOException {
    InputFile inputFile = new LocalInputFile(
        Paths.get("/Users/lapata/joy/parquet-mr/parquet-examples/src/main/resources/tianzhu_test_01.parquet"));
    try (ParquetReader<GenericRecord> reader =
        AvroParquetReader.<GenericRecord>builder(inputFile).build()) {
      while (reader.getCurrentRowIndex() > 0) {
        System.out.println(reader.read());
      }
    }
  }
}
