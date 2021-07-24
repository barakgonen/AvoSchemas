package org.bg.cpp;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.reflect.AvroSchema;

import javax.xml.validation.SchemaFactory;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main {

    public static void main(String[] args){
        // Assuming user wants to have all schemas in sub directory

        final org.apache.avro.Schema one = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Coordinate\",\"namespace\":\"org.bg.avro.structures.base.objects\",\"fields\":[{\"name\":\"lat\",\"type\":\"double\",\"doc\":\"bgbgbg\",\"default\":1.9},{\"name\":\"lon\",\"type\":\"double\"},{\"name\":\"altitude\",\"type\":\"double\"}]}");
        final org.apache.avro.Schema two = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"CoordinateEnumJson\",\"namespace\":\"org.bg.avro.structures.base.objects\",\"fields\":[{\"name\":\"pos\",\"type\":{\"type\":\"record\",\"name\":\"Coordinate\",\"fields\":[{\"name\":\"latitude\",\"type\":\"double\"},{\"name\":\"longitude\",\"type\":\"double\"},{\"name\":\"altitude\",\"type\":\"double\"}]}},{\"name\":\"enum\",\"type\":{\"type\":\"enum\",\"name\":\"Suit\",\"symbols\":[\"SQUARE\",\"TRIANGLE\",\"CIRCLE\",\"FOO\"],\"default\":\"TRIANGLE\"},\"default\":\"TRIANGLE\"}]}");
        final org.apache.avro.Schema three = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Employee\",\"namespace\":\"org.bg.avro.structures.base.objects\",\"fields\":[{\"name\":\"name\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"active\",\"type\":\"boolean\",\"default\":true},{\"name\":\"salary\",\"type\":\"long\"}]}");
        final org.apache.avro.Schema four = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"NullableTime\",\"namespace\":\"org.bg.avro.structures.base.objects\",\"fields\":[{\"name\":\"timeMillis\",\"type\":[\"null\",\"long\"]}]}");
        final org.apache.avro.Schema five = new org.apache.avro.Schema.Parser().parse("{\"type\":\"enum\",\"name\":\"Suit\",\"namespace\":\"org.bg.avro.structures.base.objects\",\"symbols\":[\"SQUARE\",\"TRIANGLE\",\"CIRCLE\",\"FOO\"],\"default\":\"TRIANGLE\"}");
        final org.apache.avro.Schema six = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Time\",\"namespace\":\"org.bg.avro.structures.base.objects\",\"fields\":[{\"name\":\"timeMllis\",\"type\":\"long\"}]}");

        List<Schema> schemas = new ArrayList<>();
        schemas.add(one);
        schemas.add(two);
        Schema unified = Schema.createUnion(one, two);
        Schema.Parser s = new Schema.Parser();
        Schema seven = s.parse(one.toString(), two.toString(), three.toString(), four.toString(), five.toString(),six.toString());
        System.out.println(seven);
        // Assume you got it from args[1]
//        String path = "/home/barak/AvroSchemas/schemas/baseObjects/src/main/avro/base_objects/";
//        File schemasFolder = new File(path);
//        File[] listOfFiles = schemasFolder.listFiles();
//        ArrayList<File> schemaFiles = new ArrayList<>();
//
//        Arrays.stream(listOfFiles).forEach(file -> {
//            if (file.getName().contains("avsc"))
//                schemaFiles.add(file);
//        });
//
//        ArrayList<Schema> schemas = new ArrayList<>();
//
//        schemaFiles.stream().forEach(file -> {
//
//            try {
//                Schema.Parser parser = new Schema.Parser();
//                Schema toAdd = parser.parse(file);
//                schemas.add(toAdd);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//
//        });

        System.out.println("BGBG");
    }
}
