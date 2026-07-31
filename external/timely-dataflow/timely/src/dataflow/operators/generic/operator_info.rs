/// Information about the operator being constructed
#[derive(Clone)]
pub struct OperatorInfo {
    /// Scope-local index assigned to the operator being constructed.
    pub local_id: usize,
    /// Worker-unique identifier.
    pub global_id: usize,
    /// Operator address.
    pub address: Vec<usize>,
}

impl OperatorInfo {
    /// Construct a new `OperatorInfo`.
    pub fn new(local_id: usize, global_id: usize, address: &[usize]) -> OperatorInfo {
        OperatorInfo {
            local_id,
            global_id,
            address: address.to_vec(),
        }
    }
}
