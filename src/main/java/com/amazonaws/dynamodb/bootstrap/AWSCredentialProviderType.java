package com.amazonaws.dynamodb.bootstrap;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;

/**
 *	Available credentials providers.
 */
public enum AWSCredentialProviderType
{
	DEFAULT("default", new DefaultAWSCredentialsProviderChain()),
	PROFILE("profile", new ProfileCredentialsProvider()),
	INSTANCE("instance", new InstanceProfileCredentialsProvider());

	private final AWSCredentialsProvider provider;
	private final String name;

	AWSCredentialProviderType(final String name, final AWSCredentialsProvider provider)
	{
		this.name = name;
		this.provider = provider;
	}

	public AWSCredentialsProvider getProvider()
	{
		return provider;
	}

	public AWSCredentialProviderType fromString(final String str)
	{
		for (final AWSCredentialProviderType providerType : AWSCredentialProviderType.values())
		{
			if (providerType.name.equals(str))
			{
				return providerType;
			}
		}

		throw new IllegalArgumentException("Unknown provider: " + str);
	}

}
