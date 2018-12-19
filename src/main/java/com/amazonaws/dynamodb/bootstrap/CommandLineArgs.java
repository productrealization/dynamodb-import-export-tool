/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.dynamodb.bootstrap;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.dynamodb.bootstrap.constants.BootstrapConstants;
import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

/**
 * This class contains the parameters to input when executing the program from
 * command line.
 */
public class CommandLineArgs {
    public static final String HELP = "--help";
    @Parameter(names = HELP, description = "Display usage information", help = true)
    private boolean help;

    public boolean getHelp() {
        return help;
    }

    public static final String SOURCE_ENDPOINT = "--sourceEndpoint";
    @Parameter(names = SOURCE_ENDPOINT, description = "Endpoint of the source table", required = true)
    private String sourceEndpoint;

    public String getSourceEndpoint() {
        return sourceEndpoint;
    }

    public static final String SOURCE_TABLE = "--sourceTable";
    @Parameter(names = SOURCE_TABLE, description = "Name of the source table", required = true)
    private String sourceTable;

    public String getSourceTable() {
        return sourceTable;
    }

    public AWSCredentialsProvider getDestinationCredentialsProvider() {
        return AWSCredentialsProviderType.valueOf(destinationCredentialsProviderType).getProvider();
    }

    public AWSCredentialsProvider getSourceCredentialsProvider() {
        return AWSCredentialsProviderType.valueOf(sourceCredentialsProviderType).getProvider();
    }

    public static final String DESTINATION_ENDPOINT = "--destinationEndpoint";
    @Parameter(names = DESTINATION_ENDPOINT, description = "Endpoint of the destination table", required = true)
    private String destinationEndpoint;

    public String getDestinationEndpoint() {
        return destinationEndpoint;
    }

    public static final String DESTINATION_TABLE = "--destinationTable";
    @Parameter(names = DESTINATION_TABLE, description = "Name of the destination table", required = true)
    private String destinationTable;

    public String getDestinationTable() {
        return destinationTable;
    }

    public static final String READ_THROUGHPUT_RATIO = "--readThroughputRatio";
    @Parameter(names = READ_THROUGHPUT_RATIO, description = "Percentage of total read throughput to scan the source table", required = true)
    private double readThroughputRatio;

    public double getReadThroughputRatio() {
        return readThroughputRatio;
    }

    public static final String WRITE_THROUGHPUT_RATIO = "--writeThroughputRatio";
    @Parameter(names = WRITE_THROUGHPUT_RATIO, description = "Percentage of total write throughput to write the destination table", required = true)
    private double writeThroughputRatio;

    public double getWriteThroughputRatio() {
        return writeThroughputRatio;
    }

    public static final String MAX_WRITE_THREADS = "--maxWriteThreads";
    @Parameter(names = MAX_WRITE_THREADS, description = "Number of max threads to write to destination table", required = false)
    private int maxWriteThreads = BootstrapConstants.DYNAMODB_CLIENT_EXECUTOR_MAX_POOL_SIZE;

    public int getMaxWriteThreads() {
        return maxWriteThreads;
    }

    public static final String TOTAL_SECTIONS = "--totalSections";
    @Parameter(names = TOTAL_SECTIONS, description = "Total number of sections to divide the scan into", required = false)
    private int totalSections = 1;

    public int getTotalSections() {
        return totalSections;
    }

    public static final String SECTION = "--section";
    @Parameter(names = SECTION, description = "Section number to scan when running multiple programs concurrently [0, 1... totalSections-1]", required = false)
    private int section = 0;

    public int getSection() {
        return section;
    }
    
    public static final String CONSISTENT_SCAN = "--consistentScan";
    @Parameter(names = CONSISTENT_SCAN, description = "Use this flag to use strongly consistent scan. If the flag is not used it will default to eventually consistent scan")
    private boolean consistentScan = false;

    public boolean getConsistentScan() {
        return consistentScan;
    }

    public static final String SOURCE_CREDENTIAL_PROVIDER_TYPE = "--sourceCredentialsProvider";
    @Parameter(names = SOURCE_CREDENTIAL_PROVIDER_TYPE, description = "AWS credentials provider to use for source DynamoDb table. Available providers:  DEFAULT, INSTANCE, PROFILE", required = false, validateValueWith = AWSCredentialsProviderTypeValidator.class)
    private String sourceCredentialsProviderType = AWSCredentialsProviderType.DEFAULT.name();

    public static final String DESTINATION_CREDENTIAL_PROVIDER_TYPE = "--destinationCredentialsProvider";
    @Parameter(names = DESTINATION_CREDENTIAL_PROVIDER_TYPE, description = "AWS credential provider to use for destination DynamoDb table. Available providers: DEFAULT, INSTANCE, PROFILE", required = false, validateValueWith = AWSCredentialsProviderTypeValidator.class)
    private String destinationCredentialsProviderType = AWSCredentialsProviderType.DEFAULT.name();

    public static final String SOURCE_PROFILE = "--sourceProfile";
    @Parameter(names = SOURCE_PROFILE, description = "AWS profile to use when reading", required = false, validateValueWith = AWSCredentialsProviderTypeValidator.class)
    private String sourceProfile = "default";

    public String getSourceProfile() {
        return sourceProfile;
    }

    public static final String DESTINATION_PROFILE = "--destinationProfile";
    @Parameter(names = DESTINATION_PROFILE, description = "AWS profile to use when writing", required = false, validateValueWith = AWSCredentialsProviderTypeValidator.class)
    private String destinationProfile = "default";

    public String getDestinationProfile() {
        return destinationProfile;
    }

    public static class AWSCredentialsProviderTypeValidator implements IValueValidator
    {
        @Override
        public void validate(final String name, final Object value) throws ParameterException
        {
            try
            {
                AWSCredentialsProviderType.valueOf((String) value);
            }
            catch (Exception ex)
            {
                throw new ParameterException("Invalid AWS credential provider type: " + value, ex);
            }
        }
    }
}
