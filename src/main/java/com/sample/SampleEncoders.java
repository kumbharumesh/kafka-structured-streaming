package com.sample;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SampleEncoders {

	public static StructType getEventSchemaEncoder() {
		return new StructType(
				new StructField[] { new StructField("value", DataTypes.StringType, true, Metadata.empty()) });
	}
}
