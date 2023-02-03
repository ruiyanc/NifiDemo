package org.apache.nifi.processors.custom;

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class MyProcessor extends AbstractProcessor {

    static Logger log = Logger.getLogger(String.valueOf((MyProcessor.class)));

    public static final PropertyDescriptor TIME = new PropertyDescriptor.Builder()
            .name("time")
            .description("time is in file")
            .required(false)
            .addValidator(Validator.VALID)
            .defaultValue("")
            .build();

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
        supDescriptors.add(TIME);
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
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        log.info("flowFile：" + flowFile);
        Map<String, String> attributes = flowFile.getAttributes();
        String filename = attributes.get("filename");
        SimpleDateFormat fileNameFormat = null;
        SimpleDateFormat fileNameFormat1 = null;
        Pattern pattern = Pattern.compile("\\d{10,}");
        Matcher matcher = pattern.matcher(filename);
        String format = null;
        if (matcher.find()) {
            String group = matcher.group();
            switch (group.length()) {
                case 8:
                    fileNameFormat1 = new SimpleDateFormat("yyyyMMdd");
                    fileNameFormat = new SimpleDateFormat("yyyy-MM-dd");
                case 10:
                    fileNameFormat1 = new SimpleDateFormat("yyyyMMddHH");
                    fileNameFormat = new SimpleDateFormat("yyyy-MM-dd HH");
                    break;
                case 12:
                    fileNameFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
                    fileNameFormat1 = new SimpleDateFormat("yyyyMMddHHmm");
                    break;
                case 17:
                    fileNameFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
                    fileNameFormat1 = new SimpleDateFormat("yyyyMMddHHmmssSSS");
                    break;
                default:
                    fileNameFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    fileNameFormat1 = new SimpleDateFormat("yyyyMMddHHmmss");
            }
            try {
                Date parse = fileNameFormat1.parse(group);
                format = fileNameFormat.format(parse);
                String currentTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").format(new Date());
                log.info("文件名称：" + filename + " ,到达时间：" +  currentTime);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
//        updateProcessorById(processorId, format);

        session.putAttribute(flowFile, "time", format);

        session.transfer(flowFile, REL_SUCCESS);
    }

}

