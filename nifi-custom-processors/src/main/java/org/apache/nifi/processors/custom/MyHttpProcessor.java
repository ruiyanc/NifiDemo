package org.apache.nifi.processors.custom;

//import org.apache.http.NameValuePair;
//import org.apache.http.client.config.RequestConfig;
//import org.apache.http.client.methods.CloseableHttpResponse;
//import org.apache.http.client.methods.HttpGet;
//import org.apache.http.client.methods.HttpPost;
//import org.apache.http.client.methods.HttpPut;
//import org.apache.http.client.utils.HttpClientUtils;
//import org.apache.http.client.utils.URIBuilder;
//import org.apache.http.entity.StringEntity;
//import org.apache.http.impl.client.CloseableHttpClient;
//import org.apache.http.impl.client.HttpClients;
//import org.apache.http.message.BasicNameValuePair;
//import org.apache.http.util.EntityUtils;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
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

import java.io.File;
import java.util.*;
import java.util.logging.Logger;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class MyHttpProcessor extends AbstractProcessor {

    static Logger log = Logger.getLogger(String.valueOf((MyHttpProcessor.class)));

    public static final PropertyDescriptor URL = new PropertyDescriptor.Builder()
            .name("Remote URL")
            .description("Remote URL")
            .required(false)
            .addValidator(Validator.VALID)
            .defaultValue("")
            .build();

    public static final PropertyDescriptor METHOD = new PropertyDescriptor.Builder()
            .name("HTTP Method")
            .description("HTTP Method")
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
        supDescriptors.add(URL);
        supDescriptors.add(METHOD);
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
        log.info("文件名：" + filename);
        String filepath = attributes.get("absolute.path") + attributes.get("filename");
        log.info("文件路径：" + filepath);
        Map<String, Object> paramMap = new HashMap<>();
        File file = new File(filepath);
        String url = context.getProperty(URL).getValue();
        String method = context.getProperty(METHOD).getValue();
        log.info("url:" + url);
        log.info("method:" + method);
        if ("POST".equalsIgnoreCase(method)) {
            paramMap.put("files", file);
//            HttpClientUtil.postFormData(url, paramMap);
            CloseableHttpClient httpClient = HttpClients.createDefault();
            HttpPost httpPost = new HttpPost(url);
            httpPost.setHeader("Content-Type", "multipart/form-data");

        }

        session.transfer(flowFile, REL_SUCCESS);
    }

}

