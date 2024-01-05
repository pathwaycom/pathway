// Copyright © 2024 Pathway

use s3::bucket::Bucket as S3Bucket;

pub trait DeepCopy {
    #[must_use]
    fn deep_copy(&self) -> Self;
}

impl DeepCopy for S3Bucket {
    fn deep_copy(&self) -> Self {
        let mut result = self.clone();

        let credentials = self.credentials.read().unwrap().clone();
        result.set_credentials(credentials);

        result
    }
}
