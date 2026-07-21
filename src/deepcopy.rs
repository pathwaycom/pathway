// Copyright © 2026 Pathway

use s3::bucket::Bucket as S3Bucket;

pub trait DeepCopy {
    #[must_use]
    fn deep_copy(&self) -> Self;
}

impl DeepCopy for S3Bucket {
    fn deep_copy(&self) -> Self {
        let mut result = self.clone();

        // `credentials()` is async on rust-s3's tokio backend, but it only reads
        // the in-memory credential store — no I/O. Drive it on a throwaway
        // current-thread runtime that spawns no worker threads, so this never
        // leaves live threads behind in a process that may later `fork()`.
        let credentials = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("failed to build a runtime for reading S3 credentials")
            .block_on(self.credentials())
            .unwrap()
            .clone();
        result.set_credentials(credentials);

        result
    }
}
