package com.ktung.jmeter.aws.sqs;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import java.io.Serializable;
import org.apache.commons.lang3.StringUtils;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqsSampler extends AbstractJavaSamplerClient implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(SqsSampler.class);

  // set up default arguments for the JMeter GUI
  @Override
  public Arguments getDefaultParameters() {
    Arguments defaultParameters = new Arguments();
    defaultParameters.addArgument("REGION", "");
    defaultParameters.addArgument("QUEUE_NAME", "");
    defaultParameters.addArgument("MSG", "");
    defaultParameters.addArgument("MessageDeduplicationId", "");
    defaultParameters.addArgument("MessageGroupId", "");
    return defaultParameters;
  }

  @Override
  public SampleResult runTest(JavaSamplerContext context) {
    LOG.info("Start SQS sampler test");
    // pull parameters
    String region = context.getParameter("REGION");
    String queueName = context.getParameter("QUEUE_NAME");
    String msg = context.getParameter("MSG");
    String messageDeduplicationId = context.getParameter("MessageDeduplicationId");
    String messageGroupId = context.getParameter("MessageGroupId");
    messageDeduplicationId = StringUtils.isEmpty(messageDeduplicationId) ? String.valueOf((Math.random() * 10000) + 1) : messageDeduplicationId;
    messageGroupId = StringUtils.isEmpty(messageGroupId) ? String.valueOf((Math.random() * 10000) + 1) : messageGroupId;

    SampleResult result = new SampleResult();
    SendMessageRequest request = null;
    result.sampleStart(); // start stopwatch

    try {
      AmazonSQSAsync sqs = AmazonSQSAsyncClientBuilder.standard()
          .withRegion(Regions.fromName(region))
          .withCredentials(new ProfileCredentialsProvider())
          .withClientSideMonitoringConfigurationProvider(null)
          .build();

      request = new SendMessageRequest(queueName, msg);
      request.setMessageGroupId(messageGroupId);
      request.setMessageDeduplicationId(messageDeduplicationId);
      SendMessageResult sqsResult = sqs.sendMessage(request);

      result.sampleEnd(); // stop stopwatch
      result.setSuccessful(true);
      result.setResponseMessage("Successfully performed action");
      result.setResponseCodeOK();

      if (null != request) {
        LOG.info(request.getMessageBody());
      }
    } catch (Exception e) {
      result.sampleEnd(); // stop stopwatch
      result.setSuccessful(false);
      result.setResponseMessage("Exception: " + e);
      result.setDataType(org.apache.jmeter.samplers.SampleResult.TEXT);
      result.setResponseCode("500");

      if (null != request) {
        LOG.error(request.getMessageBody());
      }
    }

    return result;
  }
}
