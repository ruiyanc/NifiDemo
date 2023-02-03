///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package org.apache.nifi.processors.custom;
//
//import org.apache.commons.io.IOUtils;
//import org.apache.nifi.flowfile.attributes.CoreAttributes;
//import org.apache.nifi.util.MockFlowFile;
//import org.apache.nifi.util.TestRunner;
//import org.apache.nifi.util.TestRunners;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.io.*;
//import java.nio.file.Paths;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//import static org.junit.Assert.assertTrue;
//
//
//public class MyProcessorTest {
//
//    private TestRunner testRunner;
//
//    @Before
//    public void init() {
//        testRunner = TestRunners.newTestRunner(MyUnTarProcessor111.class);
//    }
//
//    @Test
//    public void testProcessor() {
//        init();
//    }
//
//    @org.junit.Test
//    public void testOnTrigger() throws IOException {
//        init();
//        // Content to be mock a json file
//        File file = new File("/Users/xurui/png.tar");
//        InputStream content = new FileInputStream(file);
//
//        // Generate a test runner to mock a processor in a flow
////        TestRunner runner = TestRunners.newTestRunner(new JsonProcessor());
//
//        // Add properites
////        testRunner.setProperty(JsonProcessor.JSON_PATH, "$.hello");
//
//        // Add the content to the runner
//        testRunner.enqueue(content);
//
//        // Run the enqueued content, it also takes an int = number of contents queued
//        testRunner.run(1);
//
//        // All results were processed with out failure
//        testRunner.assertQueueEmpty();
//
//        // If you need to read or do aditional tests on results you can access the content
//        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(MyUnTarProcessor111.REL_SUCCESS);
//        assertTrue("1 match", results.size() == 1);
//        MockFlowFile result = results.get(0);
//        String resultValue = new String(testRunner.getContentAsByteArray(result));
//        System.out.println("Match: " + IOUtils.toString(testRunner.getContentAsByteArray(result)));
//
//        // Test attributes and content
////        result.assertAttributeEquals(testRunner.MATCH_ATTR, "nifi rocks");
//        result.assertContentEquals("nifi rocks");
//    }
//
//    @Test
//    public void testGzipDecompress() throws Exception {
//        final TestRunner runner = TestRunners.newTestRunner(MyCompressContent.class);
//        runner.setProperty(MyCompressContent.MODE, "decompress");
//        runner.setProperty(MyCompressContent.COMPRESSION_FORMAT, "tar");
//        assertTrue(runner.setProperty(MyCompressContent.UPDATE_FILENAME, "true").isValid());
//
//        runner.enqueue(Paths.get("src/test/png.tar"));
//        runner.run();
//
//        runner.assertAllFlowFilesTransferred(MyCompressContent.REL_SUCCESS, 1);
//        MockFlowFile flowFile = runner.getFlowFilesForRelationship(MyCompressContent.REL_SUCCESS).get(0);
//        flowFile.assertContentEquals(Paths.get("src/test/*.png"));
//        flowFile.assertAttributeEquals("filename", "*.png");
//
////        runner.clearTransferState();
////        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile1.txt.gz"));
////        runner.run();
//
//        runner.assertAllFlowFilesTransferred(MyCompressContent.REL_SUCCESS, 1);
//        flowFile = runner.getFlowFilesForRelationship(MyCompressContent.REL_SUCCESS).get(0);
//        flowFile.assertContentEquals(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
//        flowFile.assertAttributeEquals("filename", "SampleFile1.txt");
//
//        runner.clearTransferState();
//        runner.setProperty(MyCompressContent.COMPRESSION_FORMAT, MyCompressContent.COMPRESSION_FORMAT_ATTRIBUTE);
//        Map<String, String> attributes = new HashMap<>();
//        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/tar");
//        runner.enqueue(Paths.get("src/test/png.tar"), attributes);
//        runner.run();
//
//        runner.assertAllFlowFilesTransferred(MyCompressContent.REL_SUCCESS, 1);
//        flowFile = runner.getFlowFilesForRelationship(MyCompressContent.REL_SUCCESS).get(0);
//        flowFile.assertContentEquals(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
//        flowFile.assertAttributeEquals("filename", "SampleFile.txt");
//    }
//}
