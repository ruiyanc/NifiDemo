package org.apache.nifi.processors.custom;

/**
 * @author xurui
 * @description TODO
 * @date 2022/1/14 14:51
 */

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"content", "compress", "decompress", "gzip", "bzip2", "lzma", "xz-lzma2", "snappy", "snappy-hadoop", "snappy framed", "lz4-framed", "deflate"})
@CapabilityDescription("Compresses or decompresses the contents of FlowFiles using a user-specified compression algorithm and updates the mime.type "
        + "attribute as appropriate. This processor operates in a very memory efficient way so very large objects well beyond the heap size "
        + "are generally fine to process")
@ReadsAttribute(attribute = "mime.type", description = "If the Compression Format is set to use mime.type attribute, this attribute is used to "
        + "determine the compression type. Otherwise, this attribute is ignored.")
@WritesAttribute(attribute = "mime.type", description = "If the Mode property is set to compress, the appropriate MIME Type is set. If the Mode "
        + "property is set to decompress and the file is successfully decompressed, this attribute is removed, as the MIME Type is no longer known.")
@SystemResourceConsideration(resource = SystemResource.CPU)
@SystemResourceConsideration(resource = SystemResource.MEMORY)
public class MyCompressContent extends AbstractProcessor {

    static Logger log = Logger.getLogger(String.valueOf((MyCompressContent.class)));

    public static final PropertyDescriptor DESTPATH = new PropertyDescriptor.Builder()
            .name("destpath")
            .description("destpath")
            .required(true)
            .addValidator(StandardValidators.createDirectoryExistsValidator(true, false))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles will be transferred to the success relationship after successfully being compressed or decompressed")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles will be transferred to the failure relationship if they fail to compress/decompress")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DESTPATH);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {

        return new ArrayList<>(super.customValidate(context));
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final String destPath = context.getProperty(DESTPATH).evaluateAttributeExpressions().getValue();
        final StopWatch stopWatch = new StopWatch(true);

        log.info("flowFile：" + flowFile);
        Map<String, String> attributes = flowFile.getAttributes();
        String filename = attributes.get("filename");
        log.info("文件名：" + filename);
        String filepath = attributes.get("absolute.path");
        log.info("文件路径：" + filepath);
        log.info("destpath：" + destPath);
        AtomicReference<String> dirName = new AtomicReference<>("");

        try {
            flowFile = session.write(flowFile, (rawIn, rawOut) -> {

                final OutputStream bufferedOut = new BufferedOutputStream(rawOut, 65536);
                final InputStream bufferedIn = new BufferedInputStream(rawIn, 65536);

                TarArchiveInputStream compressionIn = new TarArchiveInputStream(bufferedIn, 2048);
//                TarArchiveInputStream tais = new TarArchiveInputStream(new FileInputStream(file));
                OutputStream fos = null;
//                TarArchiveInputStream tarInputStream = null;
                try {
//                    fis = new FileInputStream(file);
//                    tarInputStream = new TarArchiveInputStream(fis, 1024 * 2);
                    // 创建输出目录
                    createDirectory(destPath, null);

                    ArchiveEntry entry;
                    for(int i =0;;i++){
                        entry = compressionIn.getNextEntry();
                        if( entry == null){
                            break;
                        }
                        if(entry.isDirectory()){
                            createDirectory(destPath, entry.getName()); // 创建子目录
                            if (i == 0) {
                                dirName.set(entry.getName());
                            }
                        }else{
                            fos = new FileOutputStream(destPath + File.separator + entry.getName());
                            int count;
                            byte[] data = new byte[2048];
                            while ((count = compressionIn.read(data)) != -1) {
                                fos.write(data, 0, count);
                            }
                            fos.flush();
                        }
                    }
                    //删除所有._文件
//                    File targetFile = new File(destPath);
//                    File[] files = targetFile.listFiles();
//                    for (File file1 : files) {
//                        if (file1.getName().contains("._")) {
//                            file1.delete();
//                        }
//                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }finally {
                    try {
                        if(fos != null){
                            fos.close();
                        }
                        compressionIn.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
            stopWatch.stop();

            flowFile = session.removeAttribute(flowFile, CoreAttributes.MIME_TYPE.key());
            session.getProvenanceReporter().modifyContent(flowFile, stopWatch.getDuration(TimeUnit.MILLISECONDS));

            session.putAttribute(flowFile, "unTarDirName", dirName.get());
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final ProcessException e) {
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private void closeQuietly(final Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (final Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 解压缩tar文件
     * @param file 压缩包文件
     * @param targetPath 目标文件夹
     * @param delete 解压后是否删除原压缩包文件
     */
    private static void decompressTar(File file, String targetPath, boolean delete){
        if (targetPath == null) {
            targetPath = file.getParent();
        }
        FileInputStream fis = null;
        OutputStream fos = null;
        TarArchiveInputStream tarInputStream = null;
        try {
            fis = new FileInputStream(file);
            tarInputStream = new TarArchiveInputStream(fis, 1024 * 2);
            // 创建输出目录
            createDirectory(targetPath, null);

            ArchiveEntry entry;
            while(true){
                entry = tarInputStream.getNextEntry();
                if( entry == null){
                    break;
                }
                if(entry.isDirectory()){
                    createDirectory(targetPath, entry.getName()); // 创建子目录
                }else{
                    fos = new FileOutputStream(targetPath + File.separator + entry.getName());
                    int count;
                    byte[] data = new byte[2048];
                    while ((count = tarInputStream.read(data)) != -1) {
                        fos.write(data, 0, count);
                    }
                    fos.flush();
                }
            }
            //删除所有._文件
            File targetFile = new File(targetPath);
            File[] files = targetFile.listFiles();
            for (File file1 : files) {
                if (file1.getName().contains("._")) {
                    file1.delete();
                }
            }
            if (delete) {
                file.delete();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                if(fis != null){
                    fis.close();
                }
                if(fos != null){
                    fos.close();
                }
                if(tarInputStream != null){
                    tarInputStream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void createDirectory(String targetPath, String name) {
        if (name != null) {
            targetPath = targetPath + File.separator + name;
        }
        File file = new File(targetPath);
        if (!file.exists()) {
            file.mkdirs();
        }
    }

}
