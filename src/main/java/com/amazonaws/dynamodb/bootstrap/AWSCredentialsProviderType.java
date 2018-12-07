package com.amazonaws.dynamodb.bootstrap;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;

/**
 *	Available credentials providers.
 */
public enum AWSCredentialsProviderType
{
	DEFAULT(new DefaultAWSCredentialsProviderChain()),
	PROFILE(new ProfileCredentialsProvider()),
	INSTANCE(new InstanceProfileCredentialsProvider());

	private final AWSCredentialsProvider provider;

	AWSCredentialsProviderType(final AWSCredentialsProvider provider)
	{
		this.provider = provider;
	}

	public AWSCredentialsProvider getProvider()
	{
		return provider;
	}


}
