// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;
use std::time::UNIX_EPOCH;

use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_s3tables::config::{Credentials, ProvideCredentials, SharedCredentialsProvider};
use iceberg_storage_opendal::{AwsCredential, CustomAwsCredentialLoader, ProvideCredential};
use reqsign_core::time::Timestamp;
use reqsign_core::{Context, Error, ErrorKind};

/// Property aws profile name
pub const AWS_PROFILE_NAME: &str = "profile_name";
/// Property aws region
pub const AWS_REGION_NAME: &str = "region_name";
/// Property aws access key
pub const AWS_ACCESS_KEY_ID: &str = "aws_access_key_id";
/// Property aws secret access key
pub const AWS_SECRET_ACCESS_KEY: &str = "aws_secret_access_key";
/// Property aws session token
pub const AWS_SESSION_TOKEN: &str = "aws_session_token";

/// Creates an aws sdk configuration based on
/// provided properties and an optional endpoint URL.
pub(crate) async fn create_sdk_config(
    properties: &HashMap<String, String>,
    endpoint_url: Option<String>,
) -> SdkConfig {
    let mut config = aws_config::defaults(BehaviorVersion::latest());

    if let Some(endpoint_url) = endpoint_url {
        config = config.endpoint_url(endpoint_url);
    }

    if let (Some(access_key), Some(secret_key)) = (
        properties.get(AWS_ACCESS_KEY_ID),
        properties.get(AWS_SECRET_ACCESS_KEY),
    ) {
        let session_token = properties.get(AWS_SESSION_TOKEN).cloned();
        let credentials_provider =
            Credentials::new(access_key, secret_key, session_token, None, "properties");

        config = config.credentials_provider(credentials_provider)
    };

    if let Some(profile_name) = properties.get(AWS_PROFILE_NAME) {
        config = config.profile_name(profile_name);
    }

    if let Some(region_name) = properties.get(AWS_REGION_NAME) {
        let region = Region::new(region_name.clone());
        config = config.region(region);
    }

    config.load().await
}

#[derive(Debug, Clone)]
struct AwsSdkCredentialProvider {
    provider: SharedCredentialsProvider,
}

impl ProvideCredential for AwsSdkCredentialProvider {
    type Credential = AwsCredential;

    async fn provide_credential(
        &self,
        _context: &Context,
    ) -> reqsign_core::Result<Option<Self::Credential>> {
        let credentials = self.provider.provide_credentials().await.map_err(|error| {
            Error::new(
                ErrorKind::CredentialInvalid,
                "AWS SDK credential provider failed",
            )
            .with_source(error)
        })?;
        let expires_in = credentials
            .expiry()
            .map(|expiry| {
                expiry
                    .duration_since(UNIX_EPOCH)
                    .map_err(|error| {
                        Error::new(
                            ErrorKind::CredentialInvalid,
                            "AWS credential expiry predates the Unix epoch",
                        )
                        .with_source(error)
                    })
                    .and_then(|duration| {
                        i64::try_from(duration.as_millis())
                            .map_err(|error| {
                                Error::new(
                                    ErrorKind::CredentialInvalid,
                                    "AWS credential expiry is out of range",
                                )
                                .with_source(error)
                            })
                            .and_then(Timestamp::from_millisecond)
                    })
            })
            .transpose()?;

        Ok(Some(AwsCredential {
            access_key_id: credentials.access_key_id().to_string(),
            secret_access_key: credentials.secret_access_key().to_string(),
            session_token: credentials.session_token().map(ToOwned::to_owned),
            expires_in,
        }))
    }
}

pub(crate) fn sdk_credential_loader(config: &SdkConfig) -> Option<CustomAwsCredentialLoader> {
    config
        .credentials_provider()
        .map(|provider| CustomAwsCredentialLoader::new(AwsSdkCredentialProvider { provider }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_config_with_custom_endpoint() {
        let properties = HashMap::new();
        let endpoint_url = "http://localhost:5001";

        let sdk_config = create_sdk_config(&properties, Some(endpoint_url.to_string())).await;

        let result = sdk_config.endpoint_url().unwrap();

        assert_eq!(result, endpoint_url);
    }
}
