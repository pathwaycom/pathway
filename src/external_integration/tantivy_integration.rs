// Copyright © 2024 Pathway

use crate::engine::error::DynResult;
use crate::engine::{Error, Key};
use tantivy::collector::TopDocs;
use tantivy::query::{Query, QueryParser};
use tantivy::schema::{Field, Schema, Term, Value, INDEXED, STORED, TEXT};
use tantivy::{doc, Index, IndexReader, IndexWriter, ReloadPolicy, Searcher, TantivyDocument};

use super::{
    DerivedFilteredSearchIndex, ExternalIndex, ExternalIndexFactory, KeyScoreMatch,
    KeyToU64IdMapper, NonFilteringExternalIndex,
};

pub struct TantivyIndex {
    // non configurable parameters
    reader: IndexReader,
    writer: IndexWriter,
    id_field: Field,
    data_field: Field,
    query_parser: QueryParser,
    key_to_id_mapper: KeyToU64IdMapper,
}
impl TantivyIndex {
    pub fn new(ram_budget: usize, in_memory_index: bool) -> DynResult<TantivyIndex> {
        let mut schema_builder = Schema::builder();
        schema_builder.add_u64_field("id", INDEXED | STORED);
        schema_builder.add_text_field("data", TEXT);
        let schema = schema_builder.build();

        let index = if in_memory_index {
            Index::create_in_ram(schema.clone())
        } else {
            // TODO use some pathway storage, if defined
            Index::create_from_tempdir(schema.clone())?
        };

        let index_writer: IndexWriter = index.writer(ram_budget)?;
        let index_reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;

        let data_field = schema.get_field("data").unwrap();
        let id_field = schema.get_field("id").unwrap();
        let query_parser = QueryParser::for_index(&index, vec![data_field]);

        Ok(TantivyIndex {
            reader: index_reader,
            writer: index_writer,
            id_field,
            data_field,
            query_parser,
            key_to_id_mapper: KeyToU64IdMapper::new(),
        })
    }

    fn search_one(
        &self,
        data: &str,
        limit: usize,
        searcher: &Searcher,
    ) -> DynResult<Vec<KeyScoreMatch>> {
        let query: Box<dyn Query> = self.query_parser.parse_query(data)?;

        let top_docs = searcher.search(&query, &TopDocs::with_limit(limit))?;

        let mut ret_vec = Vec::with_capacity(top_docs.len());
        for (score, doc_address) in top_docs {
            let retrieved_doc: TantivyDocument = searcher.doc(doc_address)?;
            let match_proxy_id = retrieved_doc
                .get_first(self.id_field)
                .unwrap()
                .as_u64()
                .unwrap();

            ret_vec.push(KeyScoreMatch {
                key: self.key_to_id_mapper.get_key_for_id(match_proxy_id),
                score: f64::from(score),
            });
        }
        Ok(ret_vec)
    }

    fn add_one(&mut self, key: Key, data: String) -> DynResult<()> {
        let key_id = self.key_to_id_mapper.get_noncolliding_u64_id(key);
        self.writer.add_document(doc!(
            self.id_field => key_id,
            self.data_field => data,
        ))?;
        Ok(())
    }

    fn remove_one(&mut self, key: Key) -> DynResult<()> {
        let key_id = self.key_to_id_mapper.remove_key(key)?;
        let proxy_id_term = Term::from_field_u64(self.id_field, key_id);
        self.writer.delete_term(proxy_id_term);
        Ok(())
    }
}

// index methods
// maybe todo -> make search generic wrt ResultType
impl NonFilteringExternalIndex<String, String> for TantivyIndex {
    fn add(&mut self, add_data: Vec<(Key, String)>) -> Vec<(Key, DynResult<()>)> {
        let ret = add_data
            .into_iter()
            .map(|(key, data)| (key, self.add_one(key, data)))
            .collect();

        self.writer.commit().unwrap(); //TODO fix when clear how to report batch errors
        ret
    }

    fn remove(&mut self, keys: Vec<Key>) -> Vec<(Key, DynResult<()>)> {
        let ret = keys
            .into_iter()
            .map(|key| (key, self.remove_one(key)))
            .collect();
        self.writer.commit().unwrap(); //TODO fix when clear how to report batch errors
        ret
    }

    fn search(
        &self,
        queries: &[(Key, String, usize)],
    ) -> Vec<(Key, DynResult<Vec<KeyScoreMatch>>)> {
        self.reader.reload().unwrap(); //maybe handle fail in some better way? not clear how to report fail on a
                                       // batch of updates, due to fail of index reload
        let searcher: Searcher = self.reader.searcher();
        queries
            .iter()
            .map(|(key, data, limit)| (*key, self.search_one(data, *limit, &searcher)))
            .collect()
    }
}

// index factory structure
pub struct TantivyIndexFactory {
    // it seems to be some upper bound on the in-ram size of the index used by tantivy;
    // when reached, a segment is serialized to a storage (my guess is that it won't really
    // matter much for IN_RAM index, should impact performance of directory based index;
    // need to read more on those parameters)
    ram_budget: usize,
    // indicates whether the index is maintained in ram (feasible only when index is small)
    // if set to true, the index is created in ram, otherwise it should be created in some default
    // storage place
    in_memory_index: bool,
}

impl TantivyIndexFactory {
    pub fn new(ram_budget: usize, in_memory_index: bool) -> TantivyIndexFactory {
        TantivyIndexFactory {
            ram_budget,
            in_memory_index,
        }
    }
}

impl ExternalIndexFactory for TantivyIndexFactory {
    fn make_instance(&self) -> Result<Box<dyn ExternalIndex>, Error> {
        let t_index = TantivyIndex::new(self.ram_budget, self.in_memory_index)?;
        Ok(Box::new(DerivedFilteredSearchIndex::new(Box::new(t_index))))
    }
}
