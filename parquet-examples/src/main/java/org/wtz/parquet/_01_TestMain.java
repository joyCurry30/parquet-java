package org.wtz.parquet;

import com.google.common.collect.Lists;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class _01_TestMain {
  public static void main(String[] args) throws IOException {

    ParquetConfiguration conf = new PlainParquetConfiguration();

    Schema decimalSchema = Schema.createRecord("myrecord", null, null, false);
    Schema decimal = LogicalTypes.decimal(9, 2).addToSchema(Schema.create(Schema.Type.BYTES));
    decimalSchema.setFields(Collections.singletonList(new Schema.Field("dec", decimal, null, null)));

    // add the decimal conversion to a generic data model
    GenericData decimalSupport = new GenericData();
    decimalSupport.addLogicalTypeConversion(new Conversions.DecimalConversion());

    Path path = new File("/tmp/test.parquet").toPath();
    LocalOutputFile localOutputFile = new LocalOutputFile(path);
    List<GenericRecord> expected = Lists.newArrayList();
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(localOutputFile)
        .withDataModel(decimalSupport)
        .withSchema(decimalSchema)
        .withConf(conf)
        .build()) {

      Random random = new Random(34L);
      GenericRecordBuilder builder = new GenericRecordBuilder(decimalSchema);
      for (int i = 0; i < 1000; i += 1) {
        // Generating Integers between -(2^29) and (2^29 - 1) to ensure the number of digits <= 9
        BigDecimal dec = new BigDecimal(new BigInteger(30, random).subtract(BigInteger.valueOf(1L << 28)), 2);
        builder.set("dec", dec);

        GenericRecord rec = builder.build();
        expected.add(rec);
        writer.write(builder.build());
      }
    }
    List<GenericRecord> records = Lists.newArrayList();

    LocalInputFile localInputFile = new LocalInputFile(path);
    try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(localInputFile)
        .withDataModel(decimalSupport)
        .disableCompatibility()
        .withConf(conf)
        .build()) {
      GenericRecord rec;
      while ((rec = reader.read()) != null) {
        records.add(rec);
        System.out.println(rec);
      }
    }
  }
}
