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
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.custom.file.FileUploadServer;

import java.io.File;
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
public class MySocketServer extends AbstractProcessor {

    static Logger log = Logger.getLogger(String.valueOf((MySocketServer.class)));

    public static final PropertyDescriptor PATH = new PropertyDescriptor.Builder()
            .name("path")
            .description("path")
            .required(true)
            .addValidator(StandardValidators.createDirectoryExistsValidator(true, false))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("Port")
            .description("port")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .required(true)
            .defaultValue("7676")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
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
        supDescriptors.add(PORT);
        supDescriptors.add(PATH);
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
        String path = context.getProperty(PATH).evaluateAttributeExpressions().getValue();
        int port = Integer.parseInt(context.getProperty(PORT).evaluateAttributeExpressions().getValue());
//        int port = Integer.parseInt(context.getProperty(PORT).getValue());
        log.info("socketServer监听路径：" + path + "，端口：" + port);
        try {
            new FileUploadServer(path).bind(port);
//            Map<String, Object> paramMap = new HashMap<>();
//            paramMap.put("filepath", path);
//            paramMap.put("port", port);
//            HttpClientUtil.getMap("http://127.0.0.1:9100/socket/socketServer", paramMap);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
//        log.info("flowFile：" + flowFile);
//        String path = context.getProperty(PATH).getValue();
//        int port = Integer.parseInt(context.getProperty(PORT).getValue());
//        log.info("socketServer监听路径：" + path + "，端口：" + port);
//        try {
////            new FileUploadServer(path).bind(port);
//            Map<String, Object> paramMap = new HashMap<>();
//            paramMap.put("filepath", path);
//            paramMap.put("port", port);
//            HttpClientUtil.getMap("http://127.0.0.1:9100/socket/socketServer", paramMap);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
        session.transfer(flowFile, REL_SUCCESS);
    }

}

