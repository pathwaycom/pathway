//! Multi-way equijoin expression plan.
//!
//! This plan provides us the opportunity to map out a non-trivial differential
//! implementation for a complex join query. In particular, we are able to invoke
//! delta-query and worst-case optimal join plans, which avoid any intermediate
//! materialization.
//!
//! Each `MultiwayJoin` indicates several source collections, equality constraints
//! among their attributes, and then the set of attributes to produce as results.
//!
//! One naive implementation would take each input collection in order, and develop
//! the join restricted to the prefix of relations so far. Ideally the order would
//! be such that joined collections have equality constraints and prevent Cartesian
//! explosion. At each step, a new collection picks out some of the attributes and
//! instantiates a primitive binary join between the accumulated collection and the
//! next collection.
//!
//! A more sophisticated implementation establishes delta queries for each input
//! collection, which responds to changes in that input collection against the
//! current other input collections. For each input collection we may choose very
//! different join orders, as the order must follow equality constraints.
//!
//! A further implementation could develop the results attribute-by-attribute, as
//! opposed to collection-by-collection, which gives us the ability to use column
//! indices rather than whole-collection indices.

use std::hash::Hash;

use timely::dataflow::Scope;

use differential_dataflow::operators::Consolidate;
use differential_dataflow::operators::arrange::{ArrangeBySelf, ArrangeByKey};

use differential_dataflow::{Collection, ExchangeData};
use plan::{Plan, Render};
use {TraceManager, Time, Diff, Datum};

/// A multiway join of muliple relations.
///
/// By expressing multiple relations and required equivalances between their attributes,
/// we can more efficiently design incremental update strategies without materializing
/// and indexing intermediate relations.
#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct MultiwayJoin<V: Datum> {
    /// A list of (attribute index, input) pairs to extract.
    pub results: Vec<(usize, usize)>,
    /// A list of source collections.
    pub sources: Vec<Plan<V>>,
    /// Equality constraints.
    ///
    /// Equality constraints are presented as lists of `(attr, input)` equivalence classes.
    /// This means that each `(attr, input)` pair can exist in at most one list; if it would
    /// appear in more than one list, those two lists should be merged.
    pub equalities: Vec<Vec<(usize, usize)>>,
}

// TODO: This logic fails to perform restrictions in cases where a join does not
//       occur. One example could be:
//
//              result(a,b,c) := R1(a,b), R2(b,c,c)
//
//       In this case, the requirement that the 2nd and 3rd columns of R2 be equal
//       is not surfaced in any join, and is instead a filter that should be applied
//       directly to R2 (before or after the join with R1; either could be best).

impl<V: ExchangeData+Hash+Datum> Render for MultiwayJoin<V> {

    type Value = V;

    fn render<S: Scope<Timestamp = Time>>(
        &self,
        scope: &mut S,
        collections: &mut std::collections::HashMap<Plan<Self::Value>, Collection<S, Vec<Self::Value>, Diff>>,
        arrangements: &mut TraceManager<Self::Value>,
    ) -> Collection<S, Vec<Self::Value>, Diff>
    {
        // The idea here is the following:
        //
        // For each stream, we will determine a streaming delta query, in which changes
        // are joined against indexed forms of the other relations using `dogsdogsdogs`
        // stateless `propose` operators.
        //
        // For a query Q(x,y,z) := A(x,y), B(y,z), C(x,z) we might write dataflows like:
        //
        //   dQdA := dA(x,y), B(y,z), C(x,z)
        //   dQdB := dB(y,z), A(x,y), C(x,z)
        //   dQdC := dC(x,y), A(x,y), B(y,z)
        //
        // where each line is read from left to right as a sequence of `propose` joins,
        // which respond to timestamped delta changes by joining with the maintained
        // relation at that corresponding time.
        //
        // We take some care to make sure that when these joins are performed, a delta
        // interacts with relations as if we updated A, then B, then C, as if in sequence.
        // That is, when a dB delta joins A it observes all updates to A at times less or
        // equal to the delta's timestamp, but when a dB delta joins C it observes only
        // updates to C at times strictly less than the delta's timestamp.
        //
        // This is done to avoid double counting updates; any concurrent changes will be
        // accounted for by the last relation for which there is a concurrent update.

        // println!("{:?}", self);

        // Attributes we may need from any and all relations.
        let mut relevant_attributes = Vec::new();
        relevant_attributes.extend(self.results.iter().cloned());
        relevant_attributes.extend(self.equalities.iter().flat_map(|list| list.iter().cloned()));
        relevant_attributes.sort();
        relevant_attributes.dedup();

        // println!("Relevant attributes: {:?}", relevant_attributes);

        // Into which we accumulate change streams.
        let mut accumulated_changes = Vec::new();

        // For each participating relation, we build a delta query dataflow.
        for (index, plan) in self.sources.iter().enumerate() {

            // println!("building dataflow for relation {}", index);

            // Restrict down to relevant attributes.
            let mut attributes: Vec<(usize, usize)> =
            relevant_attributes
                .iter()
                .filter(|(_attr, input)| input == &index)
                .cloned()
                .collect::<Vec<_>>();

            let attributes_init = attributes.clone();
            // println!("\tinitial attributes: {:?}", attributes);

            // Ensure the plan is rendered and cached.
            if arrangements.get_unkeyed(&plan).is_none() {
                // println!("\tbuilding/caching source plan");
                let collection = plan.render(scope, collections, arrangements);
                arrangements.set_unkeyed(plan, &collection.arrange_by_self().trace);
            }
            else {
                // println!("\tsource plan found");
            }
            let changes =
            arrangements
                .get_unkeyed(&plan)
                .expect("Surely we just ensured this")
                .import(scope)
                .as_collection(|val,&()| val.clone())
                .map(move |tuple| attributes_init.iter().map(|&(attr,_)|
                    tuple[attr].clone()).collect::<Vec<_>>()
                );

            // Before constructing the dataflow, which takes a borrow on `scope`,
            // we'll want to ensure that we have all of the necessary data assets
            // in place. This requires a bit of planning first, then the building.

            // Acquire a sane sequence in which to join the relations:
            //
            // This is a sequence of relation identifiers, starting with `index`,
            // such that each has at least one attribute in common with a prior
            // relation, and so can be effectively joined.
            let join_order = plan_join_order(index, &self.equalities);
            let mut join_plan = Vec::new();

            // println!("\tjoin order: {:?}", join_order);

            // Skipping `index`, join in each relation in sequence.
            for join_idx in join_order.into_iter().skip(1) {

                // To join a relation, we need to determine any constraints on
                // attributes in common with prior relations. Any other values
                // should be appended to tuples in `changes` with care taken to
                // update `attributes`.
                let (keys, priors) = determine_keys_priors(join_idx, &self.equalities, &attributes[..]);

                // The fields in `sources[join_idx]` that should be values are those
                // that are required output or participate in an equality constraint,
                // but *WHICH ARE NOT* in `keys`.
                let vals =
                relevant_attributes
                    .iter()
                    .filter(|&(attr,index)| index == &join_idx && !keys.contains(&attr))
                    .cloned()
                    .collect::<Vec<_>>();

                // println!("\tkeys: {:?}, priors: {:?}, vals: {:?}", keys, priors, vals);

                let mut projection = Vec::new();
                for &attr in keys.iter() {
                    projection.push(attr);
                }
                for &(attr, _index) in vals.iter() {
                    projection.push(attr);
                }
                // TODO: Sort, to improve chances of re-use opportunities.
                //       Requires understanding how attributes move to get the right
                //       key selectors out though.
                // projection.sort();
                // projection.dedup(); // Should already be deduplicated, probably?

                // Get a plan for the projection on to these few attributes.
                let plan = self.sources[join_idx].clone().project(projection);

                if arrangements.get_keyed(&plan, &keys[..]).is_none() {
                    // println!("\tbuilding key: {:?}, plan: {:?}", keys, plan);
                    let keys_clone = keys.clone();
                    let arrangement =
                    plan.render(scope, collections, arrangements)
                        .map(move |tuple| (keys_clone.iter().map(|&i| tuple[i].clone()).collect::<Vec<_>>(), tuple))
                        .arrange_by_key();

                    arrangements.set_keyed(&plan, &keys[..], &arrangement.trace);
                }
                else {
                    // println!("\tplan found: {:?}, {:?}", keys, plan);
                }

                let arrangement =
                arrangements
                    .get_keyed(&plan, &keys[..])
                    .expect("Surely we just ensured this");

                let key_selector = move |change: &Vec<V>|
                    priors.iter().map(|&p| change[p].clone()).collect::<Vec<_>>()
                ;

                join_plan.push((join_idx, key_selector, arrangement));

                attributes.extend(keys.into_iter().map(|x| (x, join_idx)));
                attributes.extend(vals.into_iter());
                // println!("\tattributes: {:?}", attributes);
            }

            // Build the dataflow.
            use dogsdogsdogs::altneu::AltNeu;

            let scope_name = format!("DeltaRule: {}/{}", index, self.sources.len());
            let changes = scope.clone().scoped::<AltNeu<_>,_,_>(&scope_name, |inner| {

                // This should default to an `AltNeu::Alt` timestamp.
                let mut changes =
                changes
                    .enter(inner)
                    ;

                for (join_idx, key_selector, mut trace) in join_plan.into_iter() {

                    // Use alt or neu timestamps based on relative indices.
                    // Must have an `if` statement here as the two arrangement have different
                    // types, and we would to determine `alt` v `neu` once, rather than per
                    // tuple in the cursor.
                    changes =
                    if join_idx < index {
                        let arrangement = trace.import(scope).enter_at(inner, |_,_,t| AltNeu::alt(t.clone()), unimplemented!());
                        dogsdogsdogs::operators::propose(&changes, arrangement, key_selector)
                    }
                    else {
                        let arrangement = trace.import(scope).enter_at(inner, |_,_,t| AltNeu::neu(t.clone()), unimplemented!());
                        dogsdogsdogs::operators::propose(&changes, arrangement, key_selector)
                    }
                    .map(|(mut prefix, extensions)| { prefix.extend(extensions.into_iter()); prefix })
                    ;

                    // TODO: Equality constraints strictly within a relation have the effect
                    //       of "filtering" data, but they are ignored at the moment. We should
                    //       check for these and do something about it.
                }

                // Extract `self.results` in order, using `attributes`.
                //
                // The specific attribute requested in `self.results` may not be present in
                // `attributes` when it is equal to another present attribute. So, we should
                // look around in `self.equalities` also.
                let mut extract_map = Vec::new();
                for result in self.results.iter() {
                    if let Some(position) = attributes.iter().position(|i| i == result) {
                        extract_map.push(position);
                    }
                    else {
                        for constraint in self.equalities.iter() {
                            if constraint.contains(result) {
                                if let Some(position) = constraint.iter().flat_map(|x| attributes.iter().position(|i| i == x)).next() {
                                    extract_map.push(position);
                                }
                                else {
                                    println!("WTF NOTHING FOUND NOOOOO!!!");
                                }
                            }
                        }
                    }
                }

                changes
                    .map(move |tuple| extract_map.iter().map(|&i| tuple[i].clone()).collect::<Vec<_>>())
                    .leave()
            });

            accumulated_changes.push(changes);
        }

        differential_dataflow::collection::concatenate(scope, accumulated_changes.into_iter())
            .consolidate()
    }
}

/// Sequences relations in `constraints`.
///
/// Relations become available for sequencing as soon as they share a constraint with
/// either `source` or another sequenced relation.
fn plan_join_order(source: usize, constraints: &[Vec<(usize, usize)>]) -> Vec<usize> {

    let mut result = vec![source];
    let mut active = true;
    while active {
        active = false;
        for constraint in constraints.iter() {
            // Check to see if the constraint contains a sequenced relation.
            if constraint.iter().any(|(_,index)| result.contains(index)) {
                // If so, sequence any unsequenced relations.
                for (_, index) in constraint.iter() {
                    if !result.contains(index) {
                        result.push(*index);
                        active = true;
                    }
                }
            }
        }
    }

    result
}

/// Identifies keys and values for a join.
///
/// The result is a sequence, for each
fn determine_keys_priors(
    relation: usize,
    constraints: &[Vec<(usize, usize)>],
    current_attributes: &[(usize, usize)],
)
-> (Vec<usize>, Vec<usize>)
{
    // The fields in `sources[join_idx]` that should be keys are those
    // that share an equality constraint with an element of `attributes`.
    // For each key, we should capture the associated `attributes` entry
    // so that we can easily prepare the keys of the `delta` stream.
    let mut keys = Vec::new();
    let mut priors = Vec::new();
    for constraint in constraints.iter() {

        // If there is an intersection between `constraint` and `current_attributes`,
        // we should capture the position in `current_attributes` and emit all of the
        // attributes for `relation`.
        if let Some(prior) = current_attributes.iter().position(|x| constraint.contains(x)) {
            for &(attr, index) in constraint.iter() {
                if index == relation {
                    keys.push(attr);
                    priors.push(prior);
                }
            }
        }
    }

    (keys, priors)
}
