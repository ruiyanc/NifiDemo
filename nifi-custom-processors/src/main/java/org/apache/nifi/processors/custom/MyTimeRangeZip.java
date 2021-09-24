//package org.apache.nifi.processors.custom;
//
////import org.apache.http.NameValuePair;
////import org.apache.http.client.config.RequestConfig;
////import org.apache.http.client.methods.CloseableHttpResponse;
////import org.apache.http.client.methods.HttpGet;
////import org.apache.http.client.methods.HttpPost;
////import org.apache.http.client.methods.HttpPut;
////import org.apache.http.client.utils.HttpClientUtils;
////import org.apache.http.client.utils.URIBuilder;
////import org.apache.http.entity.StringEntity;
////import org.apache.http.impl.client.CloseableHttpClient;
////import org.apache.http.impl.client.HttpClients;
////import org.apache.http.message.BasicNameValuePair;
////import org.apache.http.util.EntityUtils;
//
//import org.apache.nifi.annotation.behavior.ReadsAttribute;
//import org.apache.nifi.annotation.behavior.ReadsAttributes;
//import org.apache.nifi.annotation.behavior.WritesAttribute;
//import org.apache.nifi.annotation.behavior.WritesAttributes;
//import org.apache.nifi.annotation.documentation.CapabilityDescription;
//import org.apache.nifi.annotation.documentation.SeeAlso;
//import org.apache.nifi.annotation.documentation.Tags;
//import org.apache.nifi.annotation.lifecycle.OnScheduled;
//import org.apache.nifi.components.PropertyDescriptor;
//import org.apache.nifi.expression.ExpressionLanguageScope;
//import org.apache.nifi.flowfile.FlowFile;
//import org.apache.nifi.flowfile.attributes.CoreAttributes;
//import org.apache.nifi.logging.ComponentLog;
//import org.apache.nifi.processor.*;
//import org.apache.nifi.processor.exception.ProcessException;
//import org.apache.nifi.processor.util.StandardValidators;
//
//import java.io.*;
//import java.nio.file.FileStore;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.nio.file.attribute.*;
//import java.text.DateFormat;
//import java.text.SimpleDateFormat;
//import java.util.*;
//import java.util.concurrent.TimeUnit;
//import java.util.logging.Logger;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;
//import java.util.zip.ZipEntry;
//import java.util.zip.ZipOutputStream;
//
//
//@Tags({"example"})
//@CapabilityDescription("Provide a description")
//@SeeAlso({})
//@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
//@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
//public class MyTimeRangeZip extends AbstractProcessor {
//
//    static Logger log = Logger.getLogger(String.valueOf((MyTimeRangeZip.class)));
//
//    private static final int BUFFER_SIZE = 2 * 1024;
//
//    public static final PropertyDescriptor ENCODE = new PropertyDescriptor.Builder()
//            .name("encode")
//            .description("encode")
//            .required(true)
//            .addValidator(StandardValidators.createDirectoryExistsValidator(true, false))
//            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
//            .build();
//
//    public static final PropertyDescriptor TIMETYPE = new PropertyDescriptor.Builder()
//            .name("time type")
//            .description("time type")
//            .required(true)
//            .addValidator(StandardValidators.createDirectoryExistsValidator(true, false))
//            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
//            .build();
//
//    public static final Relationship REL_SUCCESS = new Relationship.Builder()
//            .name("success")
//            .description("Files that have been successfully written to the output directory are transferred to this relationship")
//            .build();
//    public static final Relationship REL_FAILURE = new Relationship.Builder()
//            .name("failure")
//            .description("Files that could not be written to the output directory for some reason are transferred to this relationship")
//            .build();
//
//    private List<PropertyDescriptor> properties;
//    private Set<Relationship> relationships;
//
//    public static final String FILE_CREATION_TIME_ATTRIBUTE = "file.creationTime";
//    public static final String FILE_LAST_MODIFY_TIME_ATTRIBUTE = "file.lastModifiedTime";
//    public static final String FILE_LAST_ACCESS_TIME_ATTRIBUTE = "file.lastAccessTime";
//    public static final String FILE_OWNER_ATTRIBUTE = "file.owner";
//    public static final String FILE_GROUP_ATTRIBUTE = "file.group";
//    public static final String FILE_PERMISSIONS_ATTRIBUTE = "file.permissions";
//    public static final String FILE_MODIFY_DATE_ATTR_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";
//    public static final String KEEP_SOURCE_FILE = "yyyy-MM-dd'T'HH:mm:ssZ";
//    public static final int BATCH_SIZE = 10;
//
//    @Override
//    protected void init(final ProcessorInitializationContext context) {
//        // relationships
//        final Set<Relationship> procRels = new HashSet<>();
//        procRels.add(REL_SUCCESS);
//        procRels.add(REL_FAILURE);
//        relationships = Collections.unmodifiableSet(procRels);
//
//        // descriptors
//        final List<PropertyDescriptor> supDescriptors = new ArrayList<>();
//        supDescriptors.add(ENCODE);
//        supDescriptors.add(TIMETYPE);
//        properties = Collections.unmodifiableList(supDescriptors);
//    }
//
//    @Override
//    public Set<Relationship> getRelationships() {
//        return relationships;
//    }
//
//    @Override
//    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
//        return properties;
//    }
//
//    @OnScheduled
//    public void onScheduled(final ProcessContext context) {
//        String encode = context.getProperty(ENCODE).evaluateAttributeExpressions().getValue();
//        String timeType = context.getProperty(TIMETYPE).evaluateAttributeExpressions().getValue();
//        log.info("encode:" + encode + ",timeType:" + timeType);
//        try {
//            toZip(encode, timeType);
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }
//    }
//
//
//    @Override
//    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
////        FlowFile flowFile = session.get();
////        if (flowFile == null) {
////            return;
////        }
////        flowFile = session.create();
////        final long importStart = System.nanoTime();
////        flowFile = session.importFrom(filePath, false, flowFile);
////        String encode = context.getProperty(ENCODE).evaluateAttributeExpressions().getValue();
////        String timeType = context.getProperty(TIMETYPE).evaluateAttributeExpressions().getValue();
////        log.info("encode:" + encode + ",timeType:" + timeType);
////        try {
////            toZip(encode, timeType);
////        } catch (FileNotFoundException e) {
////            e.printStackTrace();
////        }
////        session.transfer(flowFile, REL_SUCCESS);
////        session.commit();
//
//        final File directory = new File(context.getProperty(DIRECTORY).evaluateAttributeExpressions().getValue());
//        final boolean keepingSourceFile = context.getProperty(KEEP_SOURCE_FILE).asBoolean();
//        final ComponentLog logger = getLogger();
//
//        if (fileQueue.size() < 100) {
//            final long pollingMillis = context.getProperty(POLLING_INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS);
//            if ((queueLastUpdated.get() < System.currentTimeMillis() - pollingMillis) && listingLock.tryLock()) {
//                try {
//                    final Set<File> listing = performListing(directory, fileFilterRef.get(), context.getProperty(RECURSE).asBoolean().booleanValue());
//
//                    queueLock.lock();
//                    try {
//                        listing.removeAll(inProcess);
//                        if (!keepingSourceFile) {
//                            listing.removeAll(recentlyProcessed);
//                        }
//
//                        fileQueue.clear();
//                        fileQueue.addAll(listing);
//
//                        queueLastUpdated.set(System.currentTimeMillis());
//                        recentlyProcessed.clear();
//
//                        if (listing.isEmpty()) {
//                            context.yield();
//                        }
//                    } finally {
//                        queueLock.unlock();
//                    }
//                } finally {
//                    listingLock.unlock();
//                }
//            }
//        }
//
//        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
//        final List<File> files = new ArrayList<>(batchSize);
//
//        final ListIterator<File> itr = files.listIterator();
//        FlowFile flowFile = null;
//        try {
//            final Path directoryPath = directory.toPath();
//            while (itr.hasNext()) {
//                final File file = itr.next();
//                final Path filePath = file.toPath();
//                final Path relativePath = directoryPath.relativize(filePath.getParent());
//                String relativePathString = relativePath.toString() + "/";
//                if (relativePathString.isEmpty()) {
//                    relativePathString = "./";
//                }
//                final Path absPath = filePath.toAbsolutePath();
//                final String absPathString = absPath.getParent().toString() + "/";
//
//                flowFile = session.create();
//                final long importStart = System.nanoTime();
//                flowFile = session.importFrom(filePath, keepingSourceFile, flowFile);
//                final long importNanos = System.nanoTime() - importStart;
//                final long importMillis = TimeUnit.MILLISECONDS.convert(importNanos, TimeUnit.NANOSECONDS);
//
//                flowFile = session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), file.getName());
//                flowFile = session.putAttribute(flowFile, CoreAttributes.PATH.key(), relativePathString);
//                flowFile = session.putAttribute(flowFile, CoreAttributes.ABSOLUTE_PATH.key(), absPathString);
//                Map<String, String> attributes = getAttributesFromFile(filePath);
//                if (attributes.size() > 0) {
//                    flowFile = session.putAllAttributes(flowFile, attributes);
//                }
//
//                session.getProvenanceReporter().receive(flowFile, file.toURI().toString(), importMillis);
//                session.transfer(flowFile, REL_SUCCESS);
//                logger.info("added {} to flow", new Object[]{flowFile});
//            }
//            session.commit();
//        } catch (final Exception e) {
//            logger.error("Failed to retrieve files due to {}", e);
//            // anything that we've not already processed needs to be put back on the queue
//            if (flowFile != null) {
//                session.remove(flowFile);
//            }
//        }
//
//    }
//
//    protected Map<String, String> getAttributesFromFile(final Path file) {
//        Map<String, String> attributes = new HashMap<>();
//        try {
//            FileStore store = Files.getFileStore(file);
//            if (store.supportsFileAttributeView("basic")) {
//                try {
//                    final DateFormat formatter = new SimpleDateFormat(FILE_MODIFY_DATE_ATTR_FORMAT, Locale.US);
//                    BasicFileAttributeView view = Files.getFileAttributeView(file, BasicFileAttributeView.class);
//                    BasicFileAttributes attrs = view.readAttributes();
//                    attributes.put(FILE_LAST_MODIFY_TIME_ATTRIBUTE, formatter.format(new Date(attrs.lastModifiedTime().toMillis())));
//                    attributes.put(FILE_CREATION_TIME_ATTRIBUTE, formatter.format(new Date(attrs.creationTime().toMillis())));
//                    attributes.put(FILE_LAST_ACCESS_TIME_ATTRIBUTE, formatter.format(new Date(attrs.lastAccessTime().toMillis())));
//                } catch (Exception ignore) {
//                } // allow other attributes if these fail
//            }
//            if (store.supportsFileAttributeView("owner")) {
//                try {
//                    FileOwnerAttributeView view = Files.getFileAttributeView(file, FileOwnerAttributeView.class);
//                    attributes.put(FILE_OWNER_ATTRIBUTE, view.getOwner().getName());
//                } catch (Exception ignore) {
//                } // allow other attributes if these fail
//            }
//            if (store.supportsFileAttributeView("posix")) {
//                try {
//                    PosixFileAttributeView view = Files.getFileAttributeView(file, PosixFileAttributeView.class);
//                    attributes.put(FILE_PERMISSIONS_ATTRIBUTE, PosixFilePermissions.toString(view.readAttributes().permissions()));
//                    attributes.put(FILE_GROUP_ATTRIBUTE, view.readAttributes().group().getName());
//                } catch (Exception ignore) {
//                } // allow other attributes if these fail
//            }
//        } catch (IOException ioe) {
//            // well then this FlowFile gets none of these attributes
//        }
//
//        return attributes;
//    }
//
//    public void toZip(String encode, String timeType) throws RuntimeException, FileNotFoundException {
//        Long l = System.currentTimeMillis() - System.currentTimeMillis() % 3600000;
//        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
//        String zipName = simpleDateFormat.format(new Date(l - 3600000)) + ".zip";
////        /HTData/NewFTP/ZipBak/+encode+/srcDir
//        //需要从全局变量拿的压缩包路径
//        FileOutputStream out = new FileOutputStream(new File(
//                "/HTData/NewFTP/ZipBak/" + encode + "/zipDir" + File.separator + zipName));
//        //需要从全局变量拿的源文件夹路径
//        String srcDir = "/HTData/NewFTP/ZipBak/" + encode + "/srcDir";
////        String timeType = "time";//需要从全局变量拿的压缩类型：1.时间段压缩，2.时次压缩
//        log.info("=========={srcDir:} " + srcDir + "==============");
//        boolean KeepDirStructure = false;
//        long start = System.currentTimeMillis();
//        ZipOutputStream zos = null;
//        try {
//            zos = new ZipOutputStream(out);
//            File sourceFile = new File(srcDir);
//            compress(sourceFile, zos, sourceFile.getName(), KeepDirStructure, l, timeType);
//            long end = System.currentTimeMillis();
//            System.out.println("压缩完成，耗时：" + (end - start) + " ms");
//        } catch (Exception e) {
//            throw new RuntimeException("zip error from ZipUtils", e);
//        } finally {
//            if (zos != null) {
//                try {
//                    zos.close();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//    }
//
//    private static void compress(File sourceFile, ZipOutputStream zos, String name,
//                                 boolean KeepDirStructure, Long l, String timeType) throws Exception {
//        byte[] buf = new byte[BUFFER_SIZE];
//        if (sourceFile.isFile()) {
//            // 向zip输出流中添加一个zip实体，构造器中name为zip实体的文件的名字
//            zos.putNextEntry(new ZipEntry(name));
//            // copy文件到zip输出流中
//            int len;
//            FileInputStream in = new FileInputStream(sourceFile);
//            while ((len = in.read(buf)) != -1) {
//                zos.write(buf, 0, len);
//            }
//            // Complete the entry
//            zos.closeEntry();
//            in.close();
//            boolean delete = sourceFile.delete();
//            System.out.println("文件删除" + delete);
//        } else {
//            File[] listFiles = sourceFile.listFiles();
//            if (listFiles == null || listFiles.length == 0) {
//                // 需要保留原来的文件结构时,需要对空文件夹进行处理
//                if (KeepDirStructure) {
//                    // 空文件夹的处理
//                    zos.putNextEntry(new ZipEntry(name + "/"));
//                    // 没有文件，不需要文件的copy
//                    zos.closeEntry();
//                }
//
//            } else {
//                for (File file : listFiles) {
//                    //时间段压缩
//                    if ("times".equals(timeType)) {
//                        if (file.lastModified() < l) {
//                            // 判断是否需要保留原来的文件结构
//                            if (KeepDirStructure) {
//                                // 注意：file.getName()前面需要带上父文件夹的名字加一斜杠,
//                                // 不然最后压缩包中就不能保留原来的文件结构,即：所有文件都跑到压缩包根目录下了
//                                compress(file, zos, name + "/" + file.getName(), KeepDirStructure, l, timeType);
//                            } else {
//                                compress(file, zos, file.getName(), KeepDirStructure, l, timeType);
//                            }
//                        }
//                    }
//                    //时次压缩
//                    else {
//                        //正则表达式匹配14位的日期字符串
//                        Pattern pattern14 = Pattern.compile("\\d{14}");
//                        Matcher matcher14 = pattern14.matcher(file.getName());
//                        if (matcher14.find()) {
//                            String group = matcher14.group();
//                            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
//                            String FileNameDate = simpleDateFormat.format(new Date(l - 3600000));
//                            if (group.equals(FileNameDate)) {
//                                if (KeepDirStructure) {
//                                    // 注意：file.getName()前面需要带上父文件夹的名字加一斜杠,
//                                    // 不然最后压缩包中就不能保留原来的文件结构,即：所有文件都跑到压缩包根目录下了
//                                    compress(file, zos, name + "/" + file.getName(), KeepDirStructure, l, timeType);
//                                } else {
//                                    compress(file, zos, file.getName(), KeepDirStructure, l, timeType);
//                                }
//                            }
//                        }
//                    }
//                }
//            }
//        }
//    }
//
//}
//
