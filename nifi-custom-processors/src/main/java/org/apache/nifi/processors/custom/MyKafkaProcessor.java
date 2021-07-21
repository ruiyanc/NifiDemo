/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.custom;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.utils.Bytes;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class MyKafkaProcessor extends AbstractProcessor {

    Logger log = Logger.getLogger(String.valueOf((MyKafkaProcessor.class)));

    public static final String REPLACE_RESOLUTION = "replace";
    public static final String IGNORE_RESOLUTION = "ignore";
    public static final String FAIL_RESOLUTION = "fail";

    public static final String FILE_MODIFY_DATE_ATTRIBUTE = "file.lastModifiedTime";
    public static final String FILE_MODIFY_DATE_ATTR_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";

    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
            .name("Directory")
            .description("The directory to which files should be written. You may use expression language such as /aa/bb/${path}")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();
    public static final PropertyDescriptor MAX_DESTINATION_FILES = new PropertyDescriptor.Builder()
            .name("Maximum File Count")
            .description("Specifies the maximum number of files that can exist in the output directory")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();
    public static final PropertyDescriptor CONFLICT_RESOLUTION = new PropertyDescriptor.Builder()
            .name("Conflict Resolution Strategy")
            .description("Indicates what should happen when a file with the same name already exists in the output directory")
            .required(true)
            .defaultValue(FAIL_RESOLUTION)
            .allowableValues(REPLACE_RESOLUTION, IGNORE_RESOLUTION, FAIL_RESOLUTION)
            .build();
    public static final PropertyDescriptor CHANGE_LAST_MODIFIED_TIME = new PropertyDescriptor.Builder()
            .name("Last Modified Time")
            .description("Sets the lastModifiedTime on the output file to the value of this attribute.  Format must be yyyy-MM-dd'T'HH:mm:ssZ.  "
                    + "You may also use expression language such as ${file.lastModifiedTime}.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();
    public static final PropertyDescriptor CHANGE_PERMISSIONS = new PropertyDescriptor.Builder()
            .name("Permissions")
            .description("Sets the permissions on the output file to the value of this attribute.  Format must be either UNIX rwxrwxrwx with a - in "
                    + "place of denied permissions (e.g. rw-r--r--) or an octal number (e.g. 644).  You may also use expression language such as "
                    + "${file.permissions}.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();
    public static final PropertyDescriptor CHANGE_OWNER = new PropertyDescriptor.Builder()
            .name("Owner")
            .description("Sets the owner on the output file to the value of this attribute.  You may also use expression language such as "
                    + "${file.owner}.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();
    public static final PropertyDescriptor CHANGE_GROUP = new PropertyDescriptor.Builder()
            .name("Group")
            .description("Sets the group on the output file to the value of this attribute.  You may also use expression language such "
                    + "as ${file.group}.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();
    public static final PropertyDescriptor CREATE_DIRS = new PropertyDescriptor.Builder()
            .name("Create Missing Directories")
            .description("If true, then missing destination directories will be created. If false, flowfiles are penalized and sent to failure.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final int MAX_FILE_LOCK_ATTEMPTS = 10;
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Files that have been successfully written to the output directory are transferred to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Files that could not be written to the output directory for some reason are transferred to this relationship")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        // relationships
        final Set<Relationship> procRels = new HashSet<>();
        procRels.add(REL_SUCCESS);
        procRels.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(procRels);

        // descriptors
        final List<PropertyDescriptor> supDescriptors = new ArrayList<>();
        supDescriptors.add(DIRECTORY);
        supDescriptors.add(CONFLICT_RESOLUTION);
        supDescriptors.add(CREATE_DIRS);
        supDescriptors.add(MAX_DESTINATION_FILES);
        supDescriptors.add(CHANGE_LAST_MODIFIED_TIME);
        supDescriptors.add(CHANGE_PERMISSIONS);
        supDescriptors.add(CHANGE_OWNER);
        supDescriptors.add(CHANGE_GROUP);
        properties = Collections.unmodifiableList(supDescriptors);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        long startTime = System.currentTimeMillis();
//        log.info("context:" + context.getName());
        Map<String, String> allProperties = context.getAllProperties();
//        log.info("allProperties：" + allProperties);
//        log.info("session：" + session.toString());
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        log.info("flowFile：" + flowFile);
        Map<String, String> attributes = flowFile.getAttributes();
        log.info("attributes：" + attributes);
        String filepath = attributes.get("absolute.path") + attributes.get("filename");
        log.info("文件路径：" + filepath);

        // TODO implement
        final StopWatch stopWatch = new StopWatch(true);
        //根据数据库自定义目标路径
//        Address address = selectAddress("/HTData/hxlcTest", "*");
//        DIRECTORY = address.getMonitoringAddress();
        final Path configuredRootDirPath = Paths.get(context.getProperty(DIRECTORY).evaluateAttributeExpressions(flowFile).getValue());
        log.info("rootDirPath:" + configuredRootDirPath);
        final String conflictResponse = context.getProperty(CONFLICT_RESOLUTION).getValue();
        log.info("conflictResponse:" + conflictResponse);
        final Integer maxDestinationFiles = context.getProperty(MAX_DESTINATION_FILES).asInteger();
        log.info("maxDestinationFiles" + maxDestinationFiles);

        Path tempDotCopyFile = null;
        try {
            final Path rootDirPath = configuredRootDirPath;
            final Path tempCopyFile = rootDirPath.resolve("." + flowFile.getAttribute(CoreAttributes.FILENAME.key()));
            log.info("tempCopyFile:" + tempCopyFile);
            final Path copyFile = rootDirPath.resolve(flowFile.getAttribute(CoreAttributes.FILENAME.key()));
            log.info("copyFile:" + copyFile);

            if (!Files.exists(rootDirPath)) {
                if (context.getProperty(CREATE_DIRS).asBoolean()) {
                    Files.createDirectories(rootDirPath);
                } else {
                    flowFile = session.penalize(flowFile);
                    session.transfer(flowFile, REL_FAILURE);
                    return;
                }
            }

            final Path dotCopyFile = tempCopyFile;
            tempDotCopyFile = dotCopyFile;
            Path finalCopyFile = copyFile;

            final Path finalCopyFileDir = finalCopyFile.getParent();
            // check if too many files already
            if (Files.exists(finalCopyFileDir) && maxDestinationFiles != null) {
                final int numFiles = finalCopyFileDir.toFile().list().length;

                if (numFiles >= maxDestinationFiles) {
                    flowFile = session.penalize(flowFile);
                    session.transfer(flowFile, REL_FAILURE);
                    return;
                }
            }

            if (Files.exists(finalCopyFile)) {
                switch (conflictResponse) {
                    case REPLACE_RESOLUTION:
                        Files.delete(finalCopyFile);
                        break;
                    case IGNORE_RESOLUTION:
                        session.transfer(flowFile, REL_SUCCESS);
                        return;
                    case FAIL_RESOLUTION:
                        flowFile = session.penalize(flowFile);
                        session.transfer(flowFile, REL_FAILURE);
                        return;
                    default:
                        break;
                }
            }

            session.exportTo(flowFile, dotCopyFile, false);

            final String lastModifiedTime = context.getProperty(CHANGE_LAST_MODIFIED_TIME).evaluateAttributeExpressions(flowFile).getValue();
            if (lastModifiedTime != null && !lastModifiedTime.trim().isEmpty()) {
                try {
                    final DateFormat formatter = new SimpleDateFormat(FILE_MODIFY_DATE_ATTR_FORMAT, Locale.US);
                    final Date fileModifyTime = formatter.parse(lastModifiedTime);
                    dotCopyFile.toFile().setLastModified(fileModifyTime.getTime());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            final String permissions = context.getProperty(CHANGE_PERMISSIONS).evaluateAttributeExpressions(flowFile).getValue();
            if (permissions != null && !permissions.trim().isEmpty()) {
                try {
                    String perms = stringPermissions(permissions);
                    if (!perms.isEmpty()) {
                        Files.setPosixFilePermissions(dotCopyFile, PosixFilePermissions.fromString(perms));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            final String owner = context.getProperty(CHANGE_OWNER).evaluateAttributeExpressions(flowFile).getValue();
            if (owner != null && !owner.trim().isEmpty()) {
                try {
                    UserPrincipalLookupService lookupService = dotCopyFile.getFileSystem().getUserPrincipalLookupService();
                    Files.setOwner(dotCopyFile, lookupService.lookupPrincipalByName(owner));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            final String group = context.getProperty(CHANGE_GROUP).evaluateAttributeExpressions(flowFile).getValue();
            if (group != null && !group.trim().isEmpty()) {
                try {
                    UserPrincipalLookupService lookupService = dotCopyFile.getFileSystem().getUserPrincipalLookupService();
                    PosixFileAttributeView view = Files.getFileAttributeView(dotCopyFile, PosixFileAttributeView.class);
                    view.setGroup(lookupService.lookupPrincipalByGroupName(group));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            boolean renamed = false;
            for (int i = 0; i < 10; i++) { // try rename up to 10 times.
                if (dotCopyFile.toFile().renameTo(finalCopyFile.toFile())) {
                    renamed = true;
                    break;// rename was successful
                }
                Thread.sleep(100L);// try waiting a few ms to let whatever might cause rename failure to resolve
            }

            if (!renamed) {
                if (Files.exists(dotCopyFile) && dotCopyFile.toFile().delete()) {
                    log.info("Deleted dot copy file " + Arrays.toString(new Object[]{dotCopyFile}));
                }
                throw new ProcessException("Could not rename: " + dotCopyFile);
            } else {
                log.info("Produced copy of at location " + Arrays.toString(new Object[]{flowFile, finalCopyFile}));
            }

            session.getProvenanceReporter().send(flowFile, finalCopyFile.toFile().toURI().toString(), stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(flowFile, REL_SUCCESS);

            //移动文件后推送kafka逻辑


        } catch (final Throwable t) {
            if (tempDotCopyFile != null) {
                try {
                    Files.deleteIfExists(tempDotCopyFile);
                } catch (final Exception e) {
                    log.info(String.format("Unable to remove temporary file due to %s", Arrays.toString(new Object[]{tempDotCopyFile, e})));
                }
            }

            flowFile = session.penalize(flowFile);
            log.info(String.format("Penalizing  and transferring to failure due to %s", Arrays.toString(new Object[]{flowFile, t})));
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    protected String stringPermissions(String perms) {
        String permissions = "";
        final Pattern rwxPattern = Pattern.compile("^[rwx-]{9}$");
        final Pattern numPattern = Pattern.compile("\\d+");
        if (rwxPattern.matcher(perms).matches()) {
            permissions = perms;
        } else if (numPattern.matcher(perms).matches()) {
            try {
                int number = Integer.parseInt(perms, 8);
                StringBuilder permBuilder = new StringBuilder();
                if ((number & 0x100) > 0) {
                    permBuilder.append('r');
                } else {
                    permBuilder.append('-');
                }
                if ((number & 0x80) > 0) {
                    permBuilder.append('w');
                } else {
                    permBuilder.append('-');
                }
                if ((number & 0x40) > 0) {
                    permBuilder.append('x');
                } else {
                    permBuilder.append('-');
                }
                if ((number & 0x20) > 0) {
                    permBuilder.append('r');
                } else {
                    permBuilder.append('-');
                }
                if ((number & 0x10) > 0) {
                    permBuilder.append('w');
                } else {
                    permBuilder.append('-');
                }
                if ((number & 0x8) > 0) {
                    permBuilder.append('x');
                } else {
                    permBuilder.append('-');
                }
                if ((number & 0x4) > 0) {
                    permBuilder.append('r');
                } else {
                    permBuilder.append('-');
                }
                if ((number & 0x2) > 0) {
                    permBuilder.append('w');
                } else {
                    permBuilder.append('-');
                }
                if ((number & 0x8) > 0) {
                    permBuilder.append('x');
                } else {
                    permBuilder.append('-');
                }
                permissions = permBuilder.toString();
            } catch (NumberFormatException ignore) {
            }
        }
        return permissions;
    }

    /**
     * 发送到宏图kafka
     *
     * @param filePath:文件的绝对路径
     * @param topic：四级编码
     * @param isStructuring：是否是结构化数据，结构化为：1，非结构化为：2
     */
    public void sendHTKafka(String filePath, String topic, String isStructuring) {
        //判断结构化发送kafka和非结构化发送
        if ("1".equals(isStructuring)) {
            //读取文件内容
            try {
                byte[] readFile = readFile2(filePath);
                //对象转体
                HashMap<String, Value> map = ObjectToJsonStruct(topic, readFile, filePath);
                AtomicInteger atomicInteger = sendStruct(map, topic);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            //对象转体
            String json = ObjectToJson(topic, filePath);
            //kafka传入消息
            send(json, topic);
        }
    }

    /**
     * 发送到广通kafka
     *
     * @param filePath           ：文件的绝对路径
     * @param topic：四级编码
     * @param startTime：文件监控的时间
     * @param sendTime：文件剪切完成的时间
     * @throws ParseException
     */
    public void sendGTKafka(String filePath, String topic, String startTime, String sendTime) throws ParseException {
        //取文件名
        String[] splitFilePath = filePath.split("/");
        String fileName = splitFilePath[splitFilePath.length - 1];
        File file = new File(filePath);
        //配置kafka参数
        Properties properties = properties("10.225.6.202:9192,10.225.6.203:9192,10.225.6.204:9192,10.225.6.205:9192", "StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        SimpleDateFormat fileNameFormat = null;
        String FileDate = "";
        //提取文件名中时间，例如：Z_RADA_I_54304_20210714224659_O_WPRD_LC_FFT.BIN中的20210714224659==>转为yyyy-MM-dd HH:mm
        Pattern pattern = Pattern.compile("\\d{10,}");
        Matcher matcher = pattern.matcher(fileName);
        if (matcher.find()) {
            String group = matcher.group();
            switch (group.length()) {
                case 10:
                    fileNameFormat = new SimpleDateFormat("yyyy-MM-dd HH");
                    break;
                case 12:
                    fileNameFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
                    break;
                case 17:
                    fileNameFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
                    break;
                default:
                    fileNameFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            }
            //如果是世界时间就转为北京时
            Calendar calendar = Calendar.getInstance();
            if ((System.currentTimeMillis() / 1000 - fileNameFormat.parse(group).getTime() / 1000) / (60 * 60) > 6) {
                calendar.setTime(fileNameFormat.parse(group));
                calendar.set(Calendar.HOUR, calendar.get(Calendar.HOUR) + 8);
                //calendar.getTime() 返回的是Date类型，也可以使用calendar.getTimeInMillis()获取时间戳
                FileDate = sdf.format(calendar.getTime());
                System.out.println("最终值" + FileDate);
            } else {
                FileDate = sdf.format(fileNameFormat.parse(group));
            }

        }

        Map<String, Object> fileInfo = new HashMap<>();
        if (fileName.contains("_I_")) {
            fileInfo.put("IIiii", "");
        }
        fileInfo.put("DATA_TYPE", topic);//资料所属的四级编码
        fileInfo.put("DATA_TIME", FileDate);//资料文件名的时间
        fileInfo.put("SYSTEM", "BSS.CTS");
        fileInfo.put("PROCESS_START_TIME", sdf.format(startTime));
        fileInfo.put("PROCESS_END_TIME", sdf.format(sendTime));
        fileInfo.put("FILE_NAME_O", fileName);
        fileInfo.put("FILE_NAME_N", fileName);
        fileInfo.put("FILE_SIZE", file.length());
        fileInfo.put("PROCESS_STATE", 0);
        fileInfo.put("BUSINESS_STATE", 2);
        fileInfo.put("RECORD_TIME", getDate(new Date(), sdf));
        ProducerRecord record = new ProducerRecord<>("BSS.DATA.CTS", topic, JSONObject.toJSONString(fileInfo));
        //发送记录
        producer.send(record);
    }

    /**
     * 日志
     */
    private org.slf4j.Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * 生成连接kafka的配置对象
     *
     * @param bootstrapServers：集群地址，例如：10.225.6.202:9192,10.225.6.203:9192,10.225.6.204:9192,10.225.6.205:9192
     * @param Serializer：格式化类，例如：Serializer
     * @return
     */
    public static Properties properties(String bootstrapServers, String Serializer) {
        Properties properties = new Properties();
        //broker的地址清单，建议至少填写两个，避免宕机
        properties.put("bootstrap.servers", bootstrapServers);
        //acks指定必须有多少个分区副本接收消息，生产者才认为消息写入成功，用户检测数据丢失的可能性
        //acks=0：生产者在成功写入消息之前不会等待任何来自服务器的响应。无法监控数据是否发送成功，但可以以网络能够支持的最大速度发送消息，达到很高的吞吐量。
        //acks=1：只要集群的首领节点收到消息，生产者就会收到来自服务器的成功响应。
        //acks=all：只有所有参与复制的节点全部收到消息时，生产者才会收到来自服务器的成功响应。这种模式是最安全的，
        properties.put("acks", "all");
        //retries：生产者从服务器收到的错误有可能是临时性的错误的次数
        properties.put("retries", 3);
        //batch.size：该参数指定了一个批次可以使用的内存大小，按照字节数计算（而不是消息个数)。
        properties.put("batch.size", 16384);
        //每次发送信息最大size
        //properties.put("message.max.bytes",100000000);
        properties.put("max.request.size", 104857600);
        //连接超时时间
        properties.put("message.timeout.ms", 3000);
        //消费者所允许的读取大小
        //properties.put("fetch.message.max.bytes",104857600);
        //linger.ms：该参数指定了生产者在发送批次之前等待更多消息加入批次的时间，增加延迟，提高吞吐量
        properties.put("linger.ms", 1);
        //buffer.memory该参数用来设置生产者内存缓冲区的大小，生产者用它缓冲要发送到服务器的消息。
        properties.put("buffer.memory", 33554432);
        //compression.type:数据压缩格式，有snappy、gzip和lz4，snappy算法比较均衡，gzip会消耗更高的cpu，但压缩比更高
        //key和value的序列化
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization." + Serializer);
        //client.id：该参数可以是任意的字符串，服务器会用它来识别消息的来源。
        //max.in.flight.requests.per.connection：生产者在收到服务器晌应之前可以发送多少个消息。越大越占用内存，但会提高吞吐量
        //timeout.ms：指定了broker等待同步副本返回消息确认的时间
        //request.timeout.ms：生产者在发送数据后等待服务器返回响应的时间
        //metadata.fetch.timeout.ms：生产者在获取元数据（比如目标分区的首领是谁）时等待服务器返回响应的时间。
        // max.block.ms：该参数指定了在调用 send（）方法或使用 partitionsFor（）方法获取元数据时生产者阻塞时间
        // max.request.size：该参数用于控制生产者发送的请求大小。
        //receive.buffer.bytes和send.buffer.bytes：指定了 TCP socket 接收和发送数据包的缓冲区大小，默认值为-1
        return properties;
    }

    public static HashMap<String, Value> ObjectToJsonStruct(String topic, byte[] readFile, String filePath) {
        //设置要转换的时间类型
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        //new 一个要返回的集合
        HashMap<String, Value> hashMap = new HashMap<>();
        //目录数组
        String[] pathSplit = null;
        //文件名
        String[] fileName = null;
        HashMap<String, String> data = new HashMap<>();
        Value value = new Value();
        //以   / 作为分隔目录
        pathSplit = filePath.split("/");
        //以  _  作为分隔文件名
        fileName = pathSplit[pathSplit.length - 1].split("_");
        String s = pathSplit[pathSplit.length - 1];
        if (s.toUpperCase().contains("-CC")) {
            String gzb = s.substring(s.indexOf("-CC") + 1, s.indexOf("-CC") + 4);
            data.put("BBB", gzb);
            data.put("TypeTag", "0");
        } else {
            data.put("BBB", "");
            data.put("TypeTag", "1");
            data.put("CCX", "");
        }
        data.put("TYPE", topic);
        data.put("IIIII", fileName.length >= 3 ? fileName[3] : "");
        data.put("CCCC", "");
        data.put("PQC", "0");
        data.put("OTime", fileName.length >= 4 ? updateDataType(fileName[4]) : "");
        data.put("InTime", fileName.length >= 4 ? updateDataType(fileName[4]) : "");
        data.put("STime", getDate(new Date(), sdf));
        data.put("FileType", "O");
        data.put("MD5", "");
        data.put("DataType", "AWS");
        data.put("FileName", pathSplit[pathSplit.length - 1]);
        data.put("NasPath", filePath.replace("/" + pathSplit[pathSplit.length - 1], ""));
        data.put("Format", "txt");
        data.put("FileSize", readFile.length / 1024 + "." + readFile.length % 1024);
        data.put("Lenth", "" + readFile.length);
        //内容
        value.setDate(new Bytes(readFile));
        hashMap.put(JSONObject.toJSONString(data), value);

        return hashMap;
    }

    private static String updateDataType(String data) {
        return data.substring(0, 4) + "-" + data.substring(4, 6) + "-" + data.substring(6, 8) + " " + data.substring(8, 10) + ":" + data.substring(10, 12) + ":" + data.substring(12, 14);
    }

    public static byte[] readFile2(String FilePath) throws Exception {
        //创建文件对象
        File file = new File(FilePath);
        if (!file.exists()) {
            return null;
        }
        //new 输入流对象
        InputStream inputStream = new FileInputStream(file);
        //获取文件内容的长度
        long length = file.length();
        //创建数组缓冲区，读取
        byte[] b = new byte[(int) length];
        //读取
        inputStream.read(b);
        //文件大小
        file.length();
        //输出
        // 关流
        inputStream.close();

        return b;
    }

    public static AtomicInteger sendStruct(HashMap<String, Value> map, String topic) {
        Properties properties = properties("10.225.5.219:9092,10.225.5.220:9092,10.225.5.221:9092,10.225.5.222:9092,10.225.5.223:9092", "BytesSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        AtomicInteger sum = new AtomicInteger();
        try {
            for (Map.Entry<String, Value> stringValueEntry : map.entrySet()) {
                Value value = stringValueEntry.getValue();
                String key = stringValueEntry.getKey();
                //kafka进行发布主题
                //kafka进行发布主题89u
                Map maps = (Map) JSON.parse(key);
                Object fileName = maps.get("FileName");
                ProducerRecord producerRecord = null;
                //更正报处理
                if (fileName.toString().toUpperCase().contains("-CC")) {
                    producerRecord = new ProducerRecord<>("UAC.VBBB", key, value.getDate());
                } else {
                    producerRecord = new ProducerRecord<>(topic, key, value.getDate());
                }
                producer.send(producerRecord, (recordMetadata, e) -> {
                    if (e != null) {
                        //  logger.error("消息发送失败，具体错误为:", e);
                    }
                });
                sum.getAndIncrement();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
        return sum;
    }

    public static String ObjectToJson(String topic, String filePath) {
        File file = new File(filePath);
        //设置要转换的时间类型
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        //目录数组
        String[] pathSplit = null;
        //文件名
        String fileName = null;
        String NasPath = "";
        //创建个map存放消息
        HashMap<String, String> data = new HashMap<>();
        pathSplit = filePath.split("/");
        NasPath = filePath.replace("/" + pathSplit[pathSplit.length - 1], "");
        fileName = pathSplit[pathSplit.length - 1];
        if (fileName.toUpperCase().contains("-CC")) {
            String gzb = fileName.substring(fileName.indexOf("-CC") + 1, fileName.indexOf("-CC") + 4);
            data.put("BBB", gzb);
            data.put("TypeTag", "0");
        } else {
            data.put("BBB", "");
            data.put("TypeTag", "1");
            data.put("CCX", "");
        }
        data.put("CCCC", "");
        data.put("PQC", "0");
        data.put("IIIII", "");
        data.put("TYPE", topic);
        data.put("OTime", "");
        data.put("InTime", "");
        data.put("STime", getDate(new Date(), sdf));
        data.put("FileType", "O");
        data.put("DataType", "AWS");
        data.put("MD5", "");
        data.put("FileName", fileName);
        data.put("NasPath", NasPath);
        data.put("Format", fileName.substring(fileName.lastIndexOf(".")));
        data.put("FileSize", String.valueOf(file.length()));
        data.put("Lenth", String.valueOf(file.length()));
        //map 转   json
        return JSONObject.toJSONString(data);
    }

    public static String getDate(Date date, SimpleDateFormat dateFormat) {
        return dateFormat.format(date);
    }

    public static void send(String value, String topic) {
        KafkaProducer<String, String> producer = null;
        try {
            //kafka进行发布主题
            Map maps = (Map) JSON.parse(value);
            Object fileName = maps.get("FileName");
            Object NasPath = maps.get("NasPath");
            ProducerRecord producerRecord = null;
            Value value1 = new Value();
            if (fileName.toString().toUpperCase().contains("-CC")) {
                Properties properties = properties("10.225.1.29:9092,10.225.1.30:9092,10.225.1.31:9092", "BytesSerializer");
                producer = new KafkaProducer<>(properties);
                File file = new File(NasPath + File.separator + fileName);
                if (!file.exists()) {
                    return;
                }
                //new 输入流对象
                InputStream inputStream = new FileInputStream(file);
                //获取文件内容的长度
                long length = file.length();
                //创建数组缓冲区，读取
                byte[] b = new byte[(int) length];
                //读取
                inputStream.read(b);
                //文件大小
                file.length();
                inputStream.close();
                value1.setDate(new Bytes(b));
                //更正报处理
                producerRecord = new ProducerRecord<>("UAC.VBBB", value, value1.getDate());
            } else {
                Properties properties = properties("10.225.5.219:9092,10.225.5.220:9092,10.225.5.221:9092,10.225.5.222:9092,10.225.5.223:9092", "BytesSerializer");
                producer = new KafkaProducer<>(properties);
                producerRecord = new ProducerRecord<>(topic, value, value1.getDate());
            }
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        //  logger.error("消息发送失败，具体错误为:", e);
                    }
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
            // logger.error("运行出错，关闭kafka连接:{},topic:{}", e, topic);
        } finally {
            producer.close();
        }

    }

    static class Value {
        private Bytes date;

        public Bytes getDate() {
            return date;
        }

        public void setDate(Bytes date) {
            this.date = date;
        }

    }


}
