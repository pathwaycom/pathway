// Copyright © 2026 Pathway

use s3::bucket::Bucket as S3Bucket;

pub trait DeepCopy {
    #[must_use]
    fn deep_copy(&self) -> Self;
}

impl DeepCopy for S3Bucket {
    fn deep_copy(&self) -> Self {
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

        // Built anew rather than cloned: a cloned `Bucket` shares its
        // `hyper::Client` through an `Arc`, and a pooled connection can only be
        // driven by the runtime whose task owns it. Every S3-touching struct
        // here builds a runtime and drops it with itself, so a shared pool
        // outlives the runtime behind some of its connections - and the
        // requests that reuse them then fail with "runtime dropped the dispatch
        // task" or, worse, return a body that ends early under a 200 status.
        let mut result = *S3Bucket::new(&self.name, self.region.clone(), credentials)
            .expect("bucket recreation from its own parameters should succeed");
        if self.is_path_style() {
            result.set_path_style();
        }
        result.extra_headers.clone_from(&self.extra_headers);
        result.extra_query.clone_from(&self.extra_query);
        result
    }
}
