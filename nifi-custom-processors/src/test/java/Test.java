//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONObject;
//import org.apache.kafka.clients.producer.Callback;
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.clients.producer.RecordMetadata;
//import org.apache.kafka.common.utils.Bytes;
//
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.InputStream;
//import java.text.SimpleDateFormat;
//import java.util.Date;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Properties;
//import java.util.concurrent.atomic.AtomicInteger;
//
//public class Test {
//    @org.junit.Test
//    public void test() {
//        String str = new File("").getAbsolutePath() + "/Z_SURF_I_A1135-REG_20210709061700_O_AWS-MM_FTM_PQC.BIN";
//        File file = new File(str);
//        System.out.println(file.getPath());
//        sendKafka(file.getPath(), "nifitest");
//
//    }
//
//    public Properties properties(String bootstrapServers, String Serializer) {
//        Properties properties = new Properties();
//        //broker的地址清单，建议至少填写两个，避免宕机
//        properties.put("bootstrap.servers", bootstrapServers);
//        //acks指定必须有多少个分区副本接收消息，生产者才认为消息写入成功，用户检测数据丢失的可能性
//        //acks=0：生产者在成功写入消息之前不会等待任何来自服务器的响应。无法监控数据是否发送成功，但可以以网络能够支持的最大速度发送消息，达到很高的吞吐量。
//        //acks=1：只要集群的首领节点收到消息，生产者就会收到来自服务器的成功响应。
//        //acks=all：只有所有参与复制的节点全部收到消息时，生产者才会收到来自服务器的成功响应。这种模式是最安全的，
//        properties.put("acks", "all");
//        //retries：生产者从服务器收到的错误有可能是临时性的错误的次数
//        properties.put("retries", 3);
//        //batch.size：该参数指定了一个批次可以使用的内存大小，按照字节数计算（而不是消息个数)。
//        properties.put("batch.size", 16384);
//        //每次发送信息最大size
//        //properties.put("message.max.bytes",100000000);
//        properties.put("max.request.size", 104857600);
//        //连接超时时间
//        properties.put("message.timeout.ms", 3000);
//        //消费者所允许的读取大小
//        //properties.put("fetch.message.max.bytes",104857600);
//        //linger.ms：该参数指定了生产者在发送批次之前等待更多消息加入批次的时间，增加延迟，提高吞吐量
//        properties.put("linger.ms", 1);
//        //buffer.memory该参数用来设置生产者内存缓冲区的大小，生产者用它缓冲要发送到服务器的消息。
//        properties.put("buffer.memory", 33554432);
//        //compression.type:数据压缩格式，有snappy、gzip和lz4，snappy算法比较均衡，gzip会消耗更高的cpu，但压缩比更高
//        //key和value的序列化
//        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        properties.put("value.serializer", "org.apache.kafka.common.serialization." + Serializer);
//        //client.id：该参数可以是任意的字符串，服务器会用它来识别消息的来源。
//        //max.in.flight.requests.per.connection：生产者在收到服务器晌应之前可以发送多少个消息。越大越占用内存，但会提高吞吐量
//        //timeout.ms：指定了broker等待同步副本返回消息确认的时间
//        //request.timeout.ms：生产者在发送数据后等待服务器返回响应的时间
//        //metadata.fetch.timeout.ms：生产者在获取元数据（比如目标分区的首领是谁）时等待服务器返回响应的时间。
//        // max.block.ms：该参数指定了在调用 send（）方法或使用 partitionsFor（）方法获取元数据时生产者阻塞时间
//        // max.request.size：该参数用于控制生产者发送的请求大小。
//        //receive.buffer.bytes和send.buffer.bytes：指定了 TCP socket 接收和发送数据包的缓冲区大小，默认值为-1
//        return properties;
//    }
//
//    public void sendKafka(String monitoringPath,String topic) {
//        //判断结构化发送kafka和非结构化发送
//        //读取文件内容
//        try {
//            byte[] readFile = readFile2(monitoringPath);
//            //对象转体
//            HashMap<String, Value> map = ObjectToJsonStruct(topic, readFile, monitoringPath);
//            AtomicInteger atomicInteger = sendStruct(map, topic);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//    }
//
//    public AtomicInteger sendStruct(HashMap<String, Value> map, String topic) {
//        Properties properties = this.properties("10.225.5.219:9092,10.225.5.220:9092,10.225.5.221:9092,10.225.5.222:9092,10.225.5.223:9092", "BytesSerializer");
//        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
//        AtomicInteger sum = new AtomicInteger();
//        try {
//            Date date = new Date();
//            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
//            String format = simpleDateFormat.format(date);
//            for (Map.Entry<String, Value> stringValueEntry : map.entrySet()) {
//                Value value = stringValueEntry.getValue();
//                String key = stringValueEntry.getKey();
//                //kafka进行发布主题
//                //kafka进行发布主题89u
//                Map maps = (Map) JSON.parse(key);
//                Object fileName = maps.get("FileName");
//
//                ProducerRecord producerRecord = null;
//                //更正报处理
//                if (fileName.toString().toUpperCase().contains("-CC")) {
//                    producerRecord = new ProducerRecord<>("UAC.VBBB", key, value.getDate());
//                } else {
//                    producerRecord = new ProducerRecord<>(topic, key, value.getDate());
//                }
//                //producer.send(producerRecord);
//                producer.send(producerRecord, new Callback() {
//                    @Override
//                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
////						System.out.println("partition : " + recordMetadata.partition() + " , offset : " + recordMetadata.offset());
//                    }
//                });
//                //logger.error("消息完成发送：{}", key);
//                sum.getAndIncrement();
//
////                try {
////                    Date date = new Date();
////                    String s = String.valueOf(date);
////                    Random random = new Random();
////                    int i = random.nextInt();
////                    String s1 = String.valueOf(i);
////                    jdbcTemplate.update("INSERT INTO kafka_log VALUES(?,?,?)", new Object[]{s1, s, key});
////                    sum.getAndIncrement();
////                } catch (DataAccessException e) {
////                   logger.error("kafka日志消息入库失败：{}",e.getMessage());
////                }
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            producer.close();
//        }
//        return sum;
//    }
//
////    public void insertLog() throws IOException {
////        String time = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
////        String date = time.substring(0, 8);
////        String logPath = "/Users/xurui/" + date + ".log";
////        File logFile = new File(logPath);
////        if (!logFile.exists()) {
////            logFile.createNewFile();
////        }
////        BufferedWriter writer = new BufferedWriter(new FileWriter(logFile, true));
////        while (!linkedQueue.isEmpty()) {
////            writer.write( time + "," + linkedQueue.poll() + "\r\n");
////        }
////        writer.close();
////    }
//
//    public static HashMap<String, Value> ObjectToJsonStruct(String topic, byte[] readFile, String filePath) {
//        //设置要转换的时间类型
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        //new 一个要返回的集合
//        HashMap<String, Value> hashMap = new HashMap<>();
//        //目录数组
//        String[] pathSplit = null;
//        //文件名
//        String[] fileName = null;
//        HashMap<String, String> data = new HashMap<>();
//        Value value = new Value();
//        //以   / 作为分隔目录
//        pathSplit = filePath.split("/");
//        //以  _  作为分隔文件名
//        fileName = pathSplit[pathSplit.length - 1].split("_");
//        String s = pathSplit[pathSplit.length - 1];
//        if (s.toUpperCase().contains("-CC")){
//            String gzb = s.substring(s.indexOf("-CC") + 1, s.indexOf("-CC") + 4);
//            data.put("BBB", gzb);
//            data.put("TypeTag", "0");
//        }else{
//            data.put("BBB", "");
//            data.put("TypeTag", "1");
//            data.put("CCX", "");
//        }
//        data.put("TYPE", topic);
//        data.put("IIIII", fileName.length >= 3 ? fileName[3] : "");
//        data.put("CCCC", "");
//        data.put("PQC", "0");
//        data.put("OTime", fileName.length >= 4 ? updateDataType(fileName[4]) : "");
//        data.put("InTime", fileName.length >= 4 ? updateDataType(fileName[4]) : "");
//        data.put("STime", getDate(new Date(), sdf));
//        data.put("FileType", "O");
//        data.put("MD5", "");
//        data.put("DataType", "AWS");
//        data.put("FileName", pathSplit[pathSplit.length - 1]);
//        data.put("NasPath", filePath.replace("/"+pathSplit[pathSplit.length-1],""));
//        data.put("Format", "txt");
//        data.put("FileSize", readFile.length / 1024 + "." + readFile.length % 1024);
//        data.put("Lenth", "" + readFile.length);
//        //内容
//        value.setDate(new Bytes(readFile));
//        hashMap.put(JSONObject.toJSONString(data), value);
//
//        return hashMap;
//    }
//
//    public static String getDate(Date date, SimpleDateFormat dateFormat) {
//        return dateFormat.format(date);
//    }
//
//    public byte[] readFile2(String FilePath) throws Exception {
//        //创建文件对象
//        File file = new File(FilePath);
//        if (!file.exists()) {
//            return null;
//        }
//        //new 输入流对象
//        InputStream inputStream = new FileInputStream(file);
//        //获取文件内容的长度
//        long length = file.length();
//        //创建数组缓冲区，读取
//        byte[] b = new byte[(int) length];
//        //读取
//        inputStream.read(b);
//        //文件大小
//        file.length();
//        //输出
//        // 关流
//        inputStream.close();
//
//        return b;
//    }
//
//    private static String updateDataType(String data) {
//        return data.substring(0, 4) + "-" + data.substring(4, 6) + "-" + data.substring(6, 8) + " " + data.substring(8, 10) + ":" + data.substring(10, 12) + ":" + data.substring(12, 14);
//    }
//
//
//    static class Value {
//        private Bytes date;
//
//        public Bytes getDate() {
//            return date;
//        }
//
//        public void setDate(Bytes date) {
//            this.date = date;
//        }
//
//
//    }
//
////    class ThreadXXX extends Thread {
////
////        @Override
////        public void run() {
////            while (true) {
////                try {
////                    String time = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
////                    String date = time.substring(0, 8);
////                    String logPath = "/Users/xurui/" + date + ".log";
////                    File logFile = new File(logPath);
////                    if (!logFile.exists()) {
////                        logFile.createNewFile();
////                    }
////                    BufferedWriter writer = new BufferedWriter(new FileWriter(logFile, true));
////                    while (!linkedQueue.isEmpty()) {
////                        writer.write( time + "," + linkedQueue.poll() + "\r\n");
////                    }
////                    writer.close();
////                    Thread.sleep(1000);
////                } catch (Exception e) {
////                    e.printStackTrace();
////                }
////            }
////        }
////    }
//
//
//}
