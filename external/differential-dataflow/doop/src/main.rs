#![allow(non_snake_case)]

extern crate indexmap;
extern crate timely;
extern crate differential_dataflow;


use std::collections::HashMap;
use std::rc::Rc;
use std::cell::RefCell;

use timely::dataflow::{Scope, ProbeHandle};
use timely::dataflow::scopes::child::Iterative as Child;

use differential_dataflow::{AsCollection, Collection, Hashable};
use differential_dataflow::ExchangeData as Data;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::input::Input;
use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::operators::{Threshold, Join, JoinCore, Consolidate};
use differential_dataflow::operators::arrange::ArrangeByKey;

// Type aliases for differential execution.
type Time = u32;
type Iter = u32;
type Diff = isize;

type Number = u32;
type Symbol = u32;
type Type = Symbol;
type Modifier = Symbol;
// type PrimitiveType = Type;
type ReferenceType = Type;
type ArrayType = ReferenceType;
type ClassType = ReferenceType;
type InterfaceType = ReferenceType;
type Field = Symbol;
type MethodDescriptor = Symbol;
type Method = Symbol;
type Var = Symbol;
type HeapAllocation = Symbol;
type Instruction = Symbol;
type FieldInstruction = Instruction;
// type LoadInstanceField_Insn = FieldInstruction;
// type StoreInstanceField_Insn = FieldInstruction;
// type LoadStaticField_Insn = FieldInstruction;
// type StoreStaticField_Insn = FieldInstruction;
// type ArrayInstruction = Instruction;
// type LoadArrayIndex_Insn = ArrayInstruction;
// type StoreArrayIndex_Insn = ArrayInstruction;
type AssignInstruction = Instruction;
// type AssignLocal_Insn = AssignInstruction;
// type AssignCast_Insn = AssignInstruction;
// type AssignHeapAllocation_Insn = AssignInstruction;
// type ReturnInstruction = Instruction;
// type ReturnNonvoid_Insn = ReturnInstruction;
type MethodInvocation = Instruction;
// type VirtualMethodInvocation_Insn = MethodInvocation;
// type StaticMethodInvocation_Insn = MethodInvocation;


/// Set-valued collection.
pub struct Relation<'a, G: Scope, D: Data+Hashable> where G::Timestamp : Lattice {
    variable: Variable<Child<'a, G, Iter>, D, Diff>,
    current: Collection<Child<'a, G, Iter>, D, Diff>,
}

impl<'a, G: Scope, D: Data+Hashable> Relation<'a, G, D> where G::Timestamp : Lattice {
    /// Creates a new variable initialized with `source`.
    pub fn new(scope: &mut Child<'a, G, Iter>) -> Self {
        Self::new_from(&::timely::dataflow::operators::generic::operator::empty(scope).as_collection())
    }
    /// Creates a new variable initialized with `source`.
    pub fn new_from(source: &Collection<Child<'a, G, Iter>, D, Diff>) -> Self {
        use ::timely::order::Product;
        let variable = Variable::new_from(source.clone(), Product::new(Default::default(), 1));
        Relation {
            variable: variable,
            current: source.clone(),
        }
    }
    /// Concatenates `production` into the definition of the variable.
    pub fn add_production(&mut self, production: &Collection<Child<'a, G, Iter>, D, Diff>) {
        self.current = self.current.concat(production);
    }
    /// Finalizes the variable, connecting its recursive definition.
    ///
    /// Failure to call `complete` on a variable results in a non-recursively defined
    /// collection, whose contents are just its initial `source` data.
    pub fn complete(self) {
        self.variable.set(&self.current.threshold(|_,_| 1));
    }
}

impl<'a, G: Scope, D: Data+Hashable> ::std::ops::Deref for Relation<'a, G, D>
where G::Timestamp : Lattice {
    type Target = Collection<Child<'a, G, Iter>, D, Diff>;
    fn deref(&self) -> &Collection<Child<'a, G, Iter>, D, Diff> {
        &self.variable
    }
}

struct StringInterner {
    vec: Vec<String>,
    map: HashMap<String, Symbol>
}

impl StringInterner {
    pub fn new() -> Self { StringInterner { vec: Vec::new(), map: HashMap::new() } }
    // pub fn intern(&mut self, string: &str) -> Symbol {
    //     string.to_owned()
    // }
    pub fn intern(&mut self, string: &str) -> Symbol {
        if !self.map.contains_key(string) {
            let len = self.map.len() as Symbol;
            self.vec.push(string.to_owned());
            self.map.insert(string.to_owned(), len);
            len
        }
        else {
            *self.map.get(string).unwrap()
        }
    }
    pub fn concat(&mut self, id1: Symbol, id2: Symbol) -> Symbol {
        let string = self.vec[id1 as usize].to_owned() + &self.vec[id2 as usize];
        self.intern(&string)
    }
}

fn read_file(filename: &str) -> impl Iterator<Item=String> {
    use ::std::io::{BufReader, BufRead};
    use ::std::fs::File;
    let file = BufReader::new(File::open(filename).unwrap());
    file.lines()
        .filter_map(|line| line.ok())
}


fn load<'a>(filename: &str, interner: Rc<RefCell<StringInterner>>) -> impl Iterator<Item=Vec<Symbol>>+'a {
    use ::std::io::{BufReader, BufRead};
    use ::std::fs::File;
    let file = BufReader::new(File::open(filename).unwrap());
    file.lines()
        .filter_map(|line| line.ok())
        .map(move |line| {
            let mut interner = interner.borrow_mut();
            line.split('\t')
                .map(move |string| interner.intern(string))
                .collect()
        })
}

fn load1<'a>(index: usize, prefix: &str, filename: &str, interner: Rc<RefCell<StringInterner>>) -> impl Iterator<Item=((Symbol), Time, Diff)>+'a {
    read_file(&format!("{}{}", prefix, filename))
        .filter(move |_| index == 0)
        .map(move |line| {
            let mut interner = interner.borrow_mut();
            let mut elts = line.split('\t');
            ((
                interner.intern(elts.next().unwrap())
            ), 0, 1)
        })
}

fn load2<'a>(index: usize, prefix: &str, filename: &str, interner: Rc<RefCell<StringInterner>>) -> impl Iterator<Item=((Symbol, Symbol), Time, Diff)>+'a {
    read_file(&format!("{}{}", prefix, filename))
        .filter(move |_| index == 0)
        .map(move |line| {
            let mut interner = interner.borrow_mut();
            let mut elts = line.split('\t');
            ((
                interner.intern(elts.next().unwrap()),
                interner.intern(elts.next().unwrap()),
            ), 0, 1)
        })
}

fn load3<'a>(index: usize, prefix: &str, filename: &str, interner: Rc<RefCell<StringInterner>>) -> impl Iterator<Item=((Symbol, Symbol, Symbol), Time, Diff)>+'a {
    read_file(&format!("{}{}", prefix, filename))
        .filter(move |_| index == 0)
        .map(move |line| {
            let mut interner = interner.borrow_mut();
            let mut elts = line.split('\t');
            ((
                interner.intern(elts.next().unwrap()),
                interner.intern(elts.next().unwrap()),
                interner.intern(elts.next().unwrap()),
            ), 0, 1)
        })
}

fn load4<'a>(index: usize, prefix: &str, filename: &str, interner: Rc<RefCell<StringInterner>>) -> impl Iterator<Item=((Symbol, Symbol, Symbol, Symbol), Time, Diff)>+'a {
    read_file(&format!("{}{}", prefix, filename))
        .filter(move |_| index == 0)
        .map(move |line| {
            let mut interner = interner.borrow_mut();
            let mut elts = line.split('\t');
            ((
                interner.intern(elts.next().unwrap()),
                interner.intern(elts.next().unwrap()),
                interner.intern(elts.next().unwrap()),
                interner.intern(elts.next().unwrap()),
            ), 0, 1)
        })
}

fn load5<'a>(index: usize, prefix: &str, filename: &str, interner: Rc<RefCell<StringInterner>>) -> impl Iterator<Item=((Symbol, Symbol, Symbol, Symbol, Symbol), Time, Diff)>+'a {
    read_file(&format!("{}{}", prefix, filename))
        .filter(move |_| index == 0)
        .map(move |line| {
            let mut interner = interner.borrow_mut();
            let mut elts = line.split('\t');
            ((
                interner.intern(elts.next().unwrap()),
                interner.intern(elts.next().unwrap()),
                interner.intern(elts.next().unwrap()),
                interner.intern(elts.next().unwrap()),
                interner.intern(elts.next().unwrap()),
            ), 0, 1)
        })
}

fn load6<'a>(index: usize, prefix: &str, filename: &str, interner: Rc<RefCell<StringInterner>>) -> impl Iterator<Item=((Symbol, Symbol, Symbol, Symbol, Symbol, Symbol), Time, Diff)>+'a {
    read_file(&format!("{}{}", prefix, filename))
        .filter(move |_| index == 0)
        .map(move |line| {
            let mut interner = interner.borrow_mut();
            let mut elts = line.split('\t');
            ((
                interner.intern(elts.next().unwrap()),
                interner.intern(elts.next().unwrap()),
                interner.intern(elts.next().unwrap()),
                interner.intern(elts.next().unwrap()),
                interner.intern(elts.next().unwrap()),
                interner.intern(elts.next().unwrap()),
            ), 0, 1)
        })
}

fn load7<'a>(index: usize, prefix: &str, filename: &str, interner: Rc<RefCell<StringInterner>>) -> impl Iterator<Item=((Symbol, Symbol, Symbol, Symbol, Symbol, Symbol, Symbol), Time, Diff)>+'a {
    read_file(&format!("{}{}", prefix, filename))
        .filter(move |_| index == 0)
        .map(move |line| {
            let mut interner = interner.borrow_mut();
            let mut elts = line.split('\t');
            ((
                interner.intern(elts.next().unwrap()),
                interner.intern(elts.next().unwrap()),
                interner.intern(elts.next().unwrap()),
                interner.intern(elts.next().unwrap()),
                interner.intern(elts.next().unwrap()),
                interner.intern(elts.next().unwrap()),
                interner.intern(elts.next().unwrap()),
            ), 0, 1)
        })
}

fn main() {

    let mut args = std::env::args().skip(1);
    let prefix = args.next().expect("must supply path to facts");
    let batch: Time = args.next().unwrap_or("1".to_string()).parse().expect("batch must be an integer");

    timely::execute_from_args(std::env::args(), move |worker| {

        let timer = ::std::time::Instant::now();
        let index = worker.index();

        let mut probe = ProbeHandle::new();

        // For interning strings.
        let interner = Rc::new(RefCell::new(StringInterner::new()));

        let mut inputs = (
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );

        // let inputs =
        worker.dataflow::<Time,_,_>(|scope| {

            // For loading inputs
            let (input1, ClassType) = scope.new_collection_from_raw(load1(index, &prefix, "ClassType.facts", interner.clone())); inputs.0.push(input1);
            let (input2, ArrayType) = scope.new_collection_from_raw(load1(index, &prefix, "ArrayType.facts", interner.clone())); inputs.0.push(input2);
            let (input3, InterfaceType) = scope.new_collection_from_raw(load1(index, &prefix, "InterfaceType.facts", interner.clone())); inputs.0.push(input3);
            // let Var_DeclaringMethod: Collection<_,(Symbol, Symbol)> = scope.new_collection_from_raw(load2(index, &prefix, "Var-DeclaringMethod.facts", interner.clone())).1;
            let (input4, ApplicationClass) = scope.new_collection_from_raw(load1(index, &prefix, "ApplicationClass.facts", interner.clone())); inputs.0.push(input4);
            let (input5, ThisVar) = scope.new_collection_from_raw(load2(index, &prefix, "ThisVar.facts", interner.clone())); inputs.1.push(input5);

            let (input6, _NormalHeap) = scope.new_collection_from_raw(load2(index, &prefix, "NormalHeap.facts", interner.clone())); inputs.1.push(input6);
            let (input7, _StringConstant) = scope.new_collection_from_raw(load1(index, &prefix, "StringConstant.facts", interner.clone())); inputs.0.push(input7);
            let (input8, _AssignHeapAllocation) = scope.new_collection_from_raw(load6(index, &prefix, "AssignHeapAllocation.facts", interner.clone())); inputs.5.push(input8);
            let (input9, _AssignLocal) = scope.new_collection_from_raw(load5(index, &prefix, "AssignLocal.facts", interner.clone())); inputs.4.push(input9);
            let (input10, _AssignCast) = scope.new_collection_from_raw(load6(index, &prefix, "AssignCast.facts", interner.clone())); inputs.5.push(input10);
            let (input11, _Field) = scope.new_collection_from_raw(load4(index, &prefix, "Field.facts", interner.clone())); inputs.3.push(input11);
            let (input12, _StaticMethodInvocation) = scope.new_collection_from_raw(load4(index, &prefix, "StaticMethodInvocation.facts", interner.clone())); inputs.3.push(input12);
            let (input13, _SpecialMethodInvocation) = scope.new_collection_from_raw(load5(index, &prefix, "SpecialMethodInvocation.facts", interner.clone())); inputs.4.push(input13);
            let (input14, _VirtualMethodInvocation) = scope.new_collection_from_raw(load5(index, &prefix, "VirtualMethodInvocation.facts", interner.clone())); inputs.4.push(input14);
            let (input15, _Method) = scope.new_collection_from_raw(load7(index, &prefix, "Method.facts", interner.clone())); inputs.6.push(input15);
            let (input16, _StoreInstanceField) = scope.new_collection_from_raw(load6(index, &prefix, "StoreInstanceField.facts", interner.clone())); inputs.5.push(input16);
            let (input17, _LoadInstanceField) = scope.new_collection_from_raw(load6(index, &prefix, "LoadInstanceField.facts", interner.clone())); inputs.5.push(input17);
            let (input18, _StoreStaticField) = scope.new_collection_from_raw(load5(index, &prefix, "StoreStaticField.facts", interner.clone())); inputs.4.push(input18);
            let (input19, _LoadStaticField) = scope.new_collection_from_raw(load5(index, &prefix, "LoadStaticField.facts", interner.clone())); inputs.4.push(input19);
            let (input20, _StoreArrayIndex) = scope.new_collection_from_raw(load5(index, &prefix, "StoreArrayIndex.facts", interner.clone())); inputs.4.push(input20);
            let (input21, _LoadArrayIndex) = scope.new_collection_from_raw(load5(index, &prefix, "LoadArrayIndex.facts", interner.clone())); inputs.4.push(input21);
            let (input22, _Return) = scope.new_collection_from_raw(load4(index, &prefix, "Return.facts", interner.clone())); inputs.3.push(input22);

            // TODO: Loaded as an input.
            let (input23, DirectSuperclass) = scope.new_collection_from_raw(load2(index, &prefix, "DirectSuperclass.facts", interner.clone())); inputs.1.push(input23);
            let (input24, DirectSuperinterface) = scope.new_collection_from_raw(load2(index, &prefix, "DirectSuperinterface.facts", interner.clone())); inputs.1.push(input24);
            let (input25, MainClass) = scope.new_collection_from_raw(load1(index, &prefix, "MainClass.facts", interner.clone())); inputs.0.push(input25);
            let (input26, Method_Modifier) = scope.new_collection_from_raw(load2(index, &prefix, "Method-Modifier.facts", interner.clone())); inputs.1.push(input26);
            let (input27, FormalParam) = scope.new_collection_from_raw(load3(index, &prefix, "FormalParam.facts", interner.clone())); inputs.2.push(input27);
            let (input28, Var_Type) = scope.new_collection_from_raw(load2(index, &prefix, "Var-Type.facts", interner.clone())); inputs.1.push(input28);
            let (input29, ComponentType) = scope.new_collection_from_raw(load2(index, &prefix, "ComponentType.facts", interner.clone())); inputs.1.push(input29);
            let (input30, AssignReturnValue) = scope.new_collection_from_raw(load2(index, &prefix, "AssignReturnValue.facts", interner.clone())); inputs.1.push(input30);
            let (input31, ActualParam) = scope.new_collection_from_raw(load3(index, &prefix, "ActualParam.facts", interner.clone())); inputs.2.push(input31);

            // Main schema
            let isType: Collection<_,Type> =
            ClassType
                .concat(&ArrayType)
                .concat(&InterfaceType)
                .concat(&ApplicationClass)
                .concat(&_NormalHeap.map(|(_id,ty)| ty));

            let isReferenceType: Collection<_,ReferenceType> =
            ClassType
                .concat(&ArrayType)
                .concat(&InterfaceType)
                .concat(&ApplicationClass);

            let Field_DeclaringType = _Field.map(|(sig,dec,_,_)| (sig,dec));
            let Method_DeclaringType = _Method.map(|(meth,_,_,dec,_,_,_)| (meth,dec));
            let _Method_ReturnType = _Method.map(|(meth,_,_,_,ret,_,_)| (meth,ret));
            let Method_SimpleName = _Method.map(|(meth,name,_,_,_,_,_)| (meth,name));
            let _Method_Params = _Method.map(|(meth,_,params,_,_,_,_)| (meth,params));
            let interner2 = interner.clone();
            let Method_Descriptor = _Method.map(move |(meth,_,params,_,ret,_,_)| (meth, interner2.borrow_mut().concat(ret, params)));

            let temp1 = interner.borrow_mut().intern("java.lang.String");
            let temp1b = interner.borrow_mut().intern("java.lang.String");
            let temp2 = interner.borrow_mut().intern("<<main method array content>>");
            let temp3 = interner.borrow_mut().intern("<<main method array>>");
            let temp4 = interner.borrow_mut().intern("java.lang.String[]");
            let HeapAllocation_Type =
            _NormalHeap
                .concat(&_StringConstant.map(move |s| (s, temp1.clone())))
                .concat(&scope.new_collection_from_raw(Some(((temp3.clone(), temp4.clone()), 0, 1))).1)
                .concat(&scope.new_collection_from_raw(Some(((temp2.clone(), temp1b.clone()), 0, 1))).1);

            // NOTE: Unused
            // let MainMethodArgArray: Collection<_,HeapAllocation> = scope.new_collection_from_raw(Some(temp3.clone())).1;
            // NOTE: Unused
            // let MainMethodArgArrayContent: Collection<_,HeapAllocation> = scope.new_collection_from_raw(Some(temp2.clone())).1;

            let Instruction_Method = //: Collection<_,(Instruction, Method)> =
            _AssignHeapAllocation.map(|x| (x.0, x.4))
                .concat(&_AssignLocal.map(|x| (x.0, x.4)))
                .concat(&_AssignCast.map(|x| (x.0, x.5)))
                .concat(&_StaticMethodInvocation.map(|x| (x.0, x.3)))
                .concat(&_SpecialMethodInvocation.map(|x| (x.0, x.4)))
                .concat(&_VirtualMethodInvocation.map(|x| (x.0, x.4)))
                .concat(&_StoreInstanceField.map(|x| (x.0, x.5)))
                .concat(&_LoadInstanceField.map(|x| (x.0, x.5)))
                .concat(&_StoreStaticField.map(|x| (x.0, x.4)))
                .concat(&_LoadStaticField.map(|x| (x.0, x.4)))
                .concat(&_StoreArrayIndex.map(|x| (x.0, x.4)))
                .concat(&_LoadArrayIndex.map(|x| (x.0, x.4)))
                .concat(&_Return.map(|x| (x.0, x.3)))
                .distinct()
                .arrange_by_key();

            let isVirtualMethodInvocation_Insn = _VirtualMethodInvocation.map(|x| x.0);
            let isStaticMethodInvocation_Insn = _StaticMethodInvocation.map(|x| x.0);

            let FieldInstruction_Signature: Collection<_,(FieldInstruction, Field)> =
            _StoreInstanceField.map(|x| (x.0, x.4))
                .concat(&_LoadInstanceField.map(|x| (x.0, x.4)))
                .concat(&_StoreStaticField.map(|x| (x.0, x.3)))
                .concat(&_LoadStaticField.map(|x| (x.0, x.3)));

            let LoadInstanceField_Base = _LoadInstanceField.map(|x| (x.0, x.3));
            let LoadInstanceField_To = _LoadInstanceField.map(|x| (x.0, x.2));
            let StoreInstanceField_From = _StoreInstanceField.map(|x| (x.0, x.2));
            let StoreInstanceField_Base = _StoreInstanceField.map(|x| (x.0, x.3));
            let LoadStaticField_To = _LoadStaticField.map(|x| (x.0, x.2));
            let StoreStaticField_From = _StoreStaticField.map(|x| (x.0, x.2));

            let LoadArrayIndex_Base = _LoadArrayIndex.map(|x| (x.0, x.3));
            let LoadArrayIndex_To = _LoadArrayIndex.map(|x| (x.0, x.2));
            let StoreArrayIndex_From = _StoreArrayIndex.map(|x| (x.0, x.2));
            let StoreArrayIndex_Base = _StoreArrayIndex.map(|x| (x.0, x.3));

            let AssignInstruction_To: Collection<_,(AssignInstruction, Var)> =
            _AssignHeapAllocation.map(|x| (x.0, x.3))
                .concat(&_AssignLocal.map(|x| (x.0, x.3)))
                .concat(&_AssignCast.map(|x| (x.0, x.3)));

            let AssignCast_From = _AssignCast.map(|x| (x.0, x.2));
            let AssignCast_Type = _AssignCast.map(|x| (x.0, x.4));
            let AssignLocal_From = _AssignLocal.map(|x| (x.0, x.2));
            let AssignHeapAllocation_Heap = _AssignHeapAllocation.map(|x| (x.0, x.2));

            let ReturnNonvoid_Var = _Return.map(|x| (x.0, x.2));
            let MethodInvocation_Method: Collection<_,(MethodInvocation, Method)> =
            _StaticMethodInvocation.map(|x| (x.0, x.2))
                .concat(&_SpecialMethodInvocation.map(|x| (x.0, x.2)))
                .concat(&_VirtualMethodInvocation.map(|x| (x.0, x.2)));

            let VirtualMethodInvocation_Base = _VirtualMethodInvocation.map(|x| (x.0, x.3));
            let SpecialMethodInvocation_Base = _SpecialMethodInvocation.map(|x| (x.0, x.3));
            // NOTE: Unused
            // let MethodInvocation_Base = VirtualMethodInvocation_Base.concat(&SpecialMethodInvocation_Base);

            // Fat schema

            // LoadInstanceField(?base, ?sig, ?to, ?inmethod) :-
            //  Instruction_Method(?insn, ?inmethod),
            //  LoadInstanceField_Base(?insn, ?base),
            //  FieldInstruction_Signature(?insn, ?sig),
            //  LoadInstanceField_To(?insn, ?to).
            let LoadInstanceField: Collection<_,(Var, Field, Var, Method)> =
            Instruction_Method
                .join(&LoadInstanceField_Base)
                .join(&FieldInstruction_Signature)
                .join(&LoadInstanceField_To)
                .map(|(_insn, (((inmethod, base), sig), to))| (base, sig, to, inmethod));

            // StoreInstanceField(?from, ?base, ?sig, ?inmethod) :-
            //  Instruction_Method(?insn, ?inmethod),
            //  StoreInstanceField_From(?insn, ?from),
            //  StoreInstanceField_Base(?insn, ?base),
            //  FieldInstruction_Signature(?insn, ?sig).
            let StoreInstanceField: Collection<_,(Var, Var, Field, Method)> =
            Instruction_Method
                .join(&StoreInstanceField_From)
                .join(&StoreInstanceField_Base)
                .join(&FieldInstruction_Signature)
                .map(|(_insn, (((inmethod, from), base), sig))| (from, base, sig, inmethod));

            // LoadStaticField(?sig, ?to, ?inmethod) :-
            //  Instruction_Method(?insn, ?inmethod),
            //  FieldInstruction_Signature(?insn, ?sig),
            //  LoadStaticField_To(?insn, ?to).
            let LoadStaticField: Collection<_,(Field, Var, Method)> =
            Instruction_Method
                .join(&FieldInstruction_Signature)
                .join(&LoadStaticField_To)
                .map(|(_insn, ((inmethod, sig), to))| (sig, to, inmethod));

            // StoreStaticField(?from, ?sig, ?inmethod) :-
            //  Instruction_Method(?insn, ?inmethod),
            //  StoreStaticField_From(?insn, ?from),
            //  FieldInstruction_Signature(?insn, ?sig).
            let StoreStaticField: Collection<_,(Var, Field, Method)> =
            Instruction_Method
                .join(&StoreStaticField_From)
                .join(&FieldInstruction_Signature)
                .map(|(_insn, ((inmethod, from), sig))| (from, sig, inmethod));

            // LoadArrayIndex(?base, ?to, ?inmethod) :-
            //  Instruction_Method(?insn, ?inmethod),
            //  LoadArrayIndex_Base(?insn, ?base),
            //  LoadArrayIndex_To(?insn, ?to).
            let LoadArrayIndex: Collection<_,(Var, Var, Method)> =
            Instruction_Method
                .join(&LoadArrayIndex_Base)
                .join(&LoadArrayIndex_To)
                .map(|(_insn, ((inmethod, base), to))| (base, to, inmethod));

            // StoreArrayIndex(?from, ?base, ?inmethod) :-
            //  Instruction_Method(?insn, ?inmethod),
            //  StoreArrayIndex_From(?insn, ?from),
            //  StoreArrayIndex_Base(?insn, ?base).
            let StoreArrayIndex: Collection<_,(Var, Var, Method)> =
            Instruction_Method
                .join(&StoreArrayIndex_From)
                .join(&StoreArrayIndex_Base)
                .map(|(_insn, ((inmethod, from), base))| (from, base, inmethod));

            // AssignCast(?type, ?from, ?to, ?inmethod) :-
            //  Instruction_Method(?insn, ?inmethod),
            //  AssignCast_From(?insn, ?from),
            //  AssignInstruction_To(?insn, ?to),
            //  AssignCast_Type(?insn, ?type).
            let AssignCast: Collection<_,(Type, Var, Var, Method)> =
            Instruction_Method
                .join(&AssignCast_From)
                .join(&AssignInstruction_To)
                .join(&AssignCast_Type)
                .map(|(_insn, (((inmethod, from), to), ty))| (ty, from, to, inmethod));

            // AssignLocal(?from, ?to, ?inmethod) :-
            //  AssignInstruction_To(?insn, ?to),
            //  Instruction_Method(?insn, ?inmethod),
            //  AssignLocal_From(?insn, ?from).
            let AssignLocal: Collection<_,(Var, Var, Method)> =
            Instruction_Method
                .join(&AssignInstruction_To)
                .join(&AssignLocal_From)
                .map(|(_insn, ((inmethod, to), from))| (from, to, inmethod));

            // AssignHeapAllocation(?heap, ?to, ?inmethod) :-
            //  Instruction_Method(?insn, ?inmethod),
            //  AssignHeapAllocation_Heap(?insn, ?heap),
            //  AssignInstruction_To(?insn, ?to).
            let AssignHeapAllocation: Collection<_,(HeapAllocation, Var, Method)> =
            Instruction_Method
                .join(&AssignHeapAllocation_Heap)
                .join(&AssignInstruction_To)
                .map(|(_insn, ((inmethod, heap), to))| (heap, to, inmethod));

            // ReturnVar(?var, ?method) :-
            //  Instruction_Method(?insn, ?method),
            //  ReturnNonvoid_Var(?insn, ?var).
            let ReturnVar: Collection<_,(Var, Method)> =
            Instruction_Method
                .join(&ReturnNonvoid_Var)
                .map(|(_insn, (inmethod, var))| (var, inmethod));

            // StaticMethodInvocation(?invocation, ?signature, ?inmethod) :-
            //  isStaticMethodInvocation_Insn(?invocation),
            //  Instruction_Method(?invocation, ?inmethod),
            //  MethodInvocation_Method(?invocation, ?signature).
            let StaticMethodInvocation: Collection<_,(MethodInvocation, Method, Method)> =
            Instruction_Method
                .semijoin(&isStaticMethodInvocation_Insn)
                .join(&MethodInvocation_Method)
                .map(|(invocation, (inmethod, sig))| (invocation, sig, inmethod));

            // VirtualMethodInvocation_SimpleName(?invocation, ?simplename),
            // VirtualMethodInvocation_Descriptor(?invocation, ?descriptor) :-
            //  isVirtualMethodInvocation_Insn(?invocation),
            //  MethodInvocation_Method(?invocation, ?signature),
            //  Method_SimpleName(?signature, ?simplename),
            //  Method_Descriptor(?signature, ?descriptor).
            let VirtualTemp =
            MethodInvocation_Method
                .semijoin(&isVirtualMethodInvocation_Insn)
                .map(|(invocation, signature)| (signature, invocation));
            let VirtualMethodInvocation_SimpleName =
            VirtualTemp
                .join_map(&Method_SimpleName, |_sig, inv, name| (inv.clone(), name.clone()));
            let VirtualMethodInvocation_Descriptor =
            VirtualTemp
                .join_map(&Method_Descriptor, |_sig, inv, desc| (inv.clone(), desc.clone()));

            let (MethodImplemented, MainMethodDeclaration, MethodLookup, SupertypeOf) = scope.scoped("Basic", |scope| {

                // Import some things
                let isType = isType.enter(scope);
                let isReferenceType = isReferenceType.enter(scope);
                let ClassType = ClassType.enter(scope);
                let ArrayType = ArrayType.enter(scope);
                let InterfaceType = InterfaceType.enter(scope);
                let ComponentType = ComponentType.enter(scope);
                let DirectSuperclass = DirectSuperclass.enter(scope);
                let DirectSuperinterface = DirectSuperinterface.enter(scope);
                let Method_SimpleName = Method_SimpleName.enter(scope);
                let Method_Descriptor = Method_Descriptor.enter(scope);
                let Method_DeclaringType = Method_DeclaringType.enter(scope);
                let Method_Modifier = Method_Modifier.enter(scope);
                let MainClass = MainClass.enter(scope);
                // let LoadStaticField = LoadStaticField.enter(scope);
                // let HeapAllocation_Type = HeapAllocation_Type.enter(scope);
                // let AssignHeapAllocation = AssignHeapAllocation.enter(scope);
                // let Instruction_Method = Instruction_Method.enter(scope);
                // let isStaticMethodInvocation_Insn = isStaticMethodInvocation_Insn.enter(scope);
                // let MethodInvocation_Method = MethodInvocation_Method.enter(scope);
                // let StoreStaticField = StoreStaticField.enter(scope);
                // let Field_DeclaringType = Field_DeclaringType.enter(scope);
                // let StoreArrayIndex = StoreArrayIndex.enter(scope);
                // let FormalParam = FormalParam.enter(scope);
                // let ActualParam = ActualParam.enter(scope);
                // let AssignReturnValue = AssignReturnValue.enter(scope);
                // let ReturnVar = ReturnVar.enter(scope);
                // let StoreInstanceField = StoreInstanceField.enter(scope);
                // let StaticMethodInvocation = StaticMethodInvocation.enter(scope);
                // let VirtualMethodInvocation_Base = VirtualMethodInvocation_Base.enter(scope);
                // let VirtualMethodInvocation_SimpleName = VirtualMethodInvocation_SimpleName.enter(scope);
                // let VirtualMethodInvocation_Descriptor = VirtualMethodInvocation_Descriptor.enter(scope);
                // let SpecialMethodInvocation_Base = SpecialMethodInvocation_Base.enter(scope);
                // let ThisVar = ThisVar.enter(scope);
                // let AssignLocal = AssignLocal.enter(scope);
                // let AssignCast = AssignCast.enter(scope);
                // let LoadArrayIndex = LoadArrayIndex.enter(scope);
                // let Var_Type = Var_Type.enter(scope);
                // let LoadInstanceField = LoadInstanceField.enter(scope);

                // Basic component
                let mut MethodLookup = Relation::<_,(Symbol, MethodDescriptor, Type, Method)>::new(scope);
                let mut Subclass = Relation::<_,(Type, Type)>::new(scope);
                let mut Superinterface = Relation::<_,(Type, Type)>::new(scope);
                let mut SubtypeOf = Relation::<_,(Type, Type)>::new(scope);

                // MethodImplemented(?simplename, ?descriptor, ?type, ?method) :-
                //  Method_SimpleName(?method, ?simplename),
                //  Method_Descriptor(?method, ?descriptor),
                //  Method_DeclaringType(?method, ?type),
                //  ! Method_Modifier("abstract", ?method).
                let temp = interner.borrow_mut().intern("abstract");
                let MethodImplemented =
                Method_SimpleName
                    .antijoin(&Method_Modifier.filter(move |x| x.0 == temp).map(|x| x.1).distinct())
                    .join(&Method_Descriptor)
                    .join(&Method_DeclaringType)
                    .map(|(method, ((simple, desc), ty))| (simple, desc, ty, method))
                    .threshold(|_,_| 1 as Diff);
                    // .distinct();

                // MethodLookup(?simplename, ?descriptor, ?type, ?method) :-
                //  MethodImplemented(?simplename, ?descriptor, ?type, ?method).
                // MethodLookup(?simplename, ?descriptor, ?type, ?method) :-
                //  (DirectSuperclass(?type, ?supertype) ; DirectSuperinterface(?type, ?supertype)),
                //  MethodLookup(?simplename, ?descriptor, ?supertype, ?method),
                //  ! MethodImplemented(?simplename, ?descriptor, ?type, _).
                let MethodLookupClone = MethodLookup.clone();
                MethodLookup.add_production(&MethodImplemented);
                MethodLookup.add_production(
                    &DirectSuperclass.concat(&DirectSuperinterface)
                        .map(|x| (x.1, x.0))
                        .join(&MethodLookupClone.map(|x| (x.2, (x.0, x.1, x.3))))
                        .map(|(_superty, (ty, (name, desc, method)))| ((name, desc, ty), method))
                        .antijoin(&MethodImplemented.map(|x| (x.0, x.1, x.2)).threshold(|_,_| 1))
                        .map(|((name, desc, ty), method)| (name, desc, ty, method))
                );

                // MethodImplemented.map(|_| ()).consolidate().inspect(|x| println!("MI: {:?}", x));
                // MethodLookup.map(|_| ()).consolidate().inspect(|x| println!("ML: {:?}", x));

                // DirectSubclass(?a, ?c) :- DirectSuperclass(?a, ?c).
                let DirectSubclass = DirectSuperclass.clone();  // TODO: This seems badly named.

                // Subclass(?c, ?a) :- DirectSubclass(?a, ?c).
                // Subclass(?c, ?a) :- Subclass(?b, ?a),DirectSubclass(?b, ?c).
                let SubclassClone = Subclass.clone();
                Subclass.add_production(&DirectSubclass.map(|x| (x.1, x.0)));
                Subclass.add_production(&SubclassClone.join_map(&DirectSubclass, |_b, a, c| (c.clone(), a.clone())));

                // Superclass(?c, ?a) :- Subclass(?a, ?c).
                let _Superclass = Subclass.map(|x| (x.1, x.0));

                // Superinterface(?k, ?c) :- DirectSuperinterface(?c, ?k).
                // Superinterface(?k, ?c) :- DirectSuperinterface(?c, ?j),Superinterface(?k, ?j).
                // Superinterface(?k, ?c) :- DirectSuperclass(?c, ?super),Superinterface(?k, ?super).
                let SuperinterfaceClone = Superinterface.clone();
                Superinterface.add_production(&DirectSuperinterface.map(|x| (x.1, x.0)));
                Superinterface.add_production(
                    &DirectSuperinterface
                        .map(|x| (x.1, x.0))
                        .join(&SuperinterfaceClone.map(|x| (x.1, x.0)))
                        .map(|(_j, (c, k))| (k, c))
                );
                Superinterface.add_production(
                    &DirectSuperclass
                        .map(|x| (x.1, x.0))
                        .join(&SuperinterfaceClone.map(|x| (x.1, x.0)))
                        .map(|(_j, (c, k))| (k, c))
                );

                // SupertypeOf(?s, ?t) :- SubtypeOf(?t, ?s).
                let SupertypeOf = SubtypeOf.map(|x| (x.1, x.0));

                let SubtypeOfClone = SubtypeOf.clone();
                // SubtypeOf(?s, ?s) :- isClassType(?s).
                SubtypeOf.add_production(&ClassType.map(|x| (x.clone(), x)));
                // SubtypeOf(?s, ?t) :- Subclass(?t, ?s).
                SubtypeOf.add_production(&Subclass.map(|x| (x.1, x.0)));
                // SubtypeOf(?s, ?t) :- isClassType(?s),Superinterface(?t, ?s).
                SubtypeOf.add_production(&Superinterface.map(|x| (x.1, x.0)).semijoin(&ClassType));
                // SubtypeOf(?s, ?t) :- isInterfaceType(?s),isType(?t),?t = "java.lang.Object".
                let temp = interner.borrow_mut().intern("java.lang.Object");
                SubtypeOf.add_production(&InterfaceType.map(move |x| (x, temp.clone())));
                // SubtypeOf(?s, ?s) :- isInterfaceType(?s).
                SubtypeOf.add_production(&InterfaceType.map(|x| (x.clone(), x)));
                // SubtypeOf(?s, ?t) :- isInterfaceType(?s),Superinterface(?t, ?s).
                SubtypeOf.add_production(&Superinterface.map(|x| (x.1, x.0)).semijoin(&InterfaceType));
                // SubtypeOf(?s, ?t) :- isArrayType(?s),isType(?t),?t = "java.lang.Object".
                let temp = interner.borrow_mut().intern("java.lang.Object");
                SubtypeOf.add_production(&ArrayType.map(move |x| (x, temp.clone())));
                // SubtypeOf(?s, ?t) :-
                //  ComponentType(?s, ?sc),
                //  ComponentType(?t, ?tc),
                //  isReferenceType(?sc),
                //  isReferenceType(?tc),
                //  SubtypeOf(?sc, ?tc).
                SubtypeOf.add_production(
                    &ComponentType.map(|x| (x.1, x.0))
                        .semijoin(&isReferenceType)
                        .join_map(&SubtypeOfClone, |_sc, s, tc| (tc.clone(), s.clone()))
                        .semijoin(&isReferenceType)
                        .join_map(&ComponentType.map(|x| (x.1, x.0)), |_tc, s, t| (s.clone(), t.clone()))
                );
                // SubtypeOf(?s, ?t) :- isArrayType(?s),isInterfaceType(?t),isType(?t),?t = "java.lang.Cloneable".
                let temp = interner.borrow_mut().intern("java.lang.Cloneable");
                SubtypeOf.add_production(&ArrayType.map(move |x| (x, temp.clone())));
                // SubtypeOf(?s, ?t) :- isArrayType(?s),isInterfaceType(?t),isType(?t),?t = "java.io.Serializable".
                let temp = interner.borrow_mut().intern("java.io.Serializable");
                SubtypeOf.add_production(&ArrayType.map(move |x| (x, temp.clone())));
                // SubtypeOf(?t, ?t) :- isType(?t).
                SubtypeOf.add_production(&isType.map(|x| (x.clone(), x)));

                // SubtypeOfDifferent(?s, ?t) :- SubtypeOf(?s, ?t),?s != ?t.
                let _SubtypeOfDifferent = SubtypeOf.filter(|x| x.0 != x.1);

                // MainMethodDeclaration(?method) :-
                //  MainClass(?type),
                //  Method_DeclaringType(?method, ?type),
                //  ?method != "<java.util.prefs.Base64: void main(java.lang.String[])>",
                //  ?method != "<sun.java2d.loops.GraphicsPrimitiveMgr: void main(java.lang.String[])>",
                //  ?method != "<sun.security.provider.PolicyParser: void main(java.lang.String[])>",
                //  Method_SimpleName(?method, "main"),
                //  Method_Descriptor(?method, "void(java.lang.String[])"),
                //  Method_Modifier("public", ?method),
                //  Method_Modifier("static", ?method).
                let temp1 = interner.borrow_mut().intern("<java.util.prefs.Base64: void main(java.lang.String[])>");
                let temp2 = interner.borrow_mut().intern("<sun.java2d.loops.GraphicsPrimitiveMgr: void main(java.lang.String[])>");
                let temp3 = interner.borrow_mut().intern("<sun.security.provider.PolicyParser: void main(java.lang.String[])>");
                let temp4 = interner.borrow_mut().intern("main");
                let temp5 = interner.borrow_mut().intern("void(java.lang.String[])");
                let temp6 = interner.borrow_mut().intern("public");
                let temp7 = interner.borrow_mut().intern("static");
                let MainMethodDeclaration =
                Method_DeclaringType
                    .map(|x| (x.1, x.0))
                    .semijoin(&MainClass)
                    .map(|x| (x.1, ()))
                    .filter(move |x| x.0 != temp1 && x.0 != temp2 && x.0 != temp3)
                    .semijoin(&Method_SimpleName.filter(move |x| x.1 == temp4).map(|x| x.0))
                    .semijoin(&Method_Descriptor.filter(move |x| x.1 == temp5).map(|x| x.0))
                    .semijoin(&Method_Modifier.filter(move |x| x.0 == temp6).map(|x| x.1))
                    .semijoin(&Method_Modifier.filter(move |x| x.0 == temp7).map(|x| x.1))
                    .map(|x| x.0);

                let result = (MethodImplemented.leave(), MainMethodDeclaration.leave(), MethodLookup.leave(), SupertypeOf.leave());

                MethodLookup.complete();
                Subclass.complete();
                Superinterface.complete();
                SubtypeOf.complete();

                result
            });

            let (_st, _ic, _reachable, _varpoints, _callgraph) = scope.scoped("Iteration", |scope| {

                // Import some things
                // let isType = isType.enter(scope);
                // let isReferenceType = isReferenceType.enter(scope);
                // let ClassType = ClassType.enter(scope);
                // let ArrayType = ArrayType.enter(scope);
                // let InterfaceType = InterfaceType.enter(scope);
                let ComponentType = ComponentType.enter(scope);
                let DirectSuperclass = DirectSuperclass.enter(scope);
                let DirectSuperinterface = DirectSuperinterface.enter(scope);
                // let Method_SimpleName = Method_SimpleName.enter(scope);
                // let Method_Descriptor = Method_Descriptor.enter(scope);
                let Method_DeclaringType = Method_DeclaringType.enter(scope);
                // let Method_Modifier = Method_Modifier.enter(scope);
                // let MainClass = MainClass.enter(scope);
                let LoadStaticField = LoadStaticField.enter(scope);
                let HeapAllocation_Type = HeapAllocation_Type.enter(scope);
                let AssignHeapAllocation = AssignHeapAllocation.enter(scope);
                let Instruction_Method = Instruction_Method.enter(scope);
                let isStaticMethodInvocation_Insn = isStaticMethodInvocation_Insn.enter(scope);
                let MethodInvocation_Method = MethodInvocation_Method.enter(scope);
                let StoreStaticField = StoreStaticField.enter(scope);
                let Field_DeclaringType = Field_DeclaringType.enter(scope);
                let StoreArrayIndex = StoreArrayIndex.enter(scope);
                let FormalParam = FormalParam.enter(scope);
                let ActualParam = ActualParam.enter(scope);
                let AssignReturnValue = AssignReturnValue.enter(scope);
                let ReturnVar = ReturnVar.enter(scope);
                let StoreInstanceField = StoreInstanceField.enter(scope);
                let StaticMethodInvocation = StaticMethodInvocation.enter(scope);
                let VirtualMethodInvocation_Base = VirtualMethodInvocation_Base.enter(scope);
                let VirtualMethodInvocation_SimpleName = VirtualMethodInvocation_SimpleName.enter(scope);
                let VirtualMethodInvocation_Descriptor = VirtualMethodInvocation_Descriptor.enter(scope);
                let SpecialMethodInvocation_Base = SpecialMethodInvocation_Base.enter(scope);
                let ThisVar = ThisVar.enter(scope);
                let AssignLocal = AssignLocal.enter(scope);
                let AssignCast = AssignCast.enter(scope);
                let LoadArrayIndex = LoadArrayIndex.enter(scope);
                let Var_Type = Var_Type.enter(scope);
                let LoadInstanceField = LoadInstanceField.enter(scope);

                let MethodImplemented = MethodImplemented.enter(scope);
                let MainMethodDeclaration = MainMethodDeclaration.enter(scope);
                let MethodLookup = MethodLookup.enter(scope);
                let SupertypeOf = SupertypeOf.enter(scope);

                // Required by all
                let mut Reachable = Relation::<_,(Method)>::new(scope);

                // NOTE: Common subexpression.
                let Reachable_Invocation =
                    Instruction_Method
                        .as_collection(|inv,meth| (meth.clone(), inv.clone()))
                        // .map(|(inv,meth)| (meth,inv))
                        .semijoin(&Reachable)
                        .map(|(_meth,inv)| (inv, ()));

                // // NOTE: Cheating, but to test what is broken.
                // let Reachable = ReachableFinal.clone();

                // Class initialization
                let mut InitializedClass = Relation::<_,(Type)>::new(scope);

                // ClassInitializer(?type, ?method) :- basic.MethodImplemented("<clinit>", "void()", ?type, ?method).
                let temp1 = interner.borrow_mut().intern("<clinit>");
                let temp2 = interner.borrow_mut().intern("void()");
                let ClassInitializer =
                MethodImplemented
                    .filter(move |x| x.0 == temp1 && x.1 == temp2)
                    .map(|x| (x.2, x.3))
                    .distinct();

                let InitializedClassClone = InitializedClass.clone();
                // InitializedClass(?superclass) :- InitializedClass(?class),DirectSuperclass(?class, ?superclass).
                InitializedClass.add_production(&DirectSuperclass.semijoin(&InitializedClassClone).map(|x| x.1));
                // InitializedClass(?superinterface) :- InitializedClass(?classOrInterface),DirectSuperinterface(?classOrInterface, ?superinterface).
                InitializedClass.add_production(&DirectSuperinterface.semijoin(&InitializedClassClone).map(|x| x.1));
                // InitializedClass(?class) :- basic.MainMethodDeclaration(?method),Method_DeclaringType(?method, ?class).
                InitializedClass.add_production(&Method_DeclaringType.semijoin(&MainMethodDeclaration).map(|x| x.1));
                // InitializedClass(?class) :-
                //  Reachable(?inmethod),
                //  AssignHeapAllocation(?heap, _, ?inmethod),
                //  HeapAllocation_Type(?heap, ?class).
                InitializedClass.add_production(
                    &AssignHeapAllocation
                        .map(|(heap,_,inmethod)| (inmethod,heap))
                        .semijoin(&Reachable)
                        .map(|(_inmethod,heap)| (heap, ()))
                        .join_map(&HeapAllocation_Type, |_,(),class| class.clone())
                );
                // InitializedClass(?class) :-
                //  Reachable(?inmethod),
                //  Instruction_Method(?invocation, ?inmethod),
                //  isStaticMethodInvocation_Insn(?invocation),
                //  MethodInvocation_Method(?invocation, ?signature),
                //  Method_DeclaringType(?signature, ?class).
                InitializedClass.add_production(
                    &Reachable_Invocation
                        .semijoin(&isStaticMethodInvocation_Insn)
                        .join_map(&MethodInvocation_Method, |_,(),sig| (sig.clone(), ()))
                        .join_map(&Method_DeclaringType, |_,(),class| class.clone())
                );

                // InitializedClass(?classOrInterface) :-
                //  Reachable(?inmethod),
                //  StoreStaticField(_, ?signature, ?inmethod),
                //  Field_DeclaringType(?signature, ?classOrInterface).
                InitializedClass.add_production(
                    &StoreStaticField
                        .map(|(_,sig,meth)| (meth,sig))
                        .semijoin(&Reachable)
                        .map(|(_meth,sig)| (sig, ()))
                        .join_map(&Field_DeclaringType, |_,(),class| class.clone())
                );
                // InitializedClass(?classOrInterface) :-
                //  Reachable(?inmethod),
                //  LoadStaticField(?signature, _, ?inmethod),
                //  Field_DeclaringType(?signature, ?classOrInterface).
                InitializedClass.add_production(
                    &LoadStaticField
                        .map(|x| (x.2, x.0))
                        .semijoin(&Reachable)
                        .map(|x| (x.1, ()))
                        .join_map(&Field_DeclaringType, |_,_,class| class.clone())
                );

                // Reachable(?clinit) :- InitializedClass(?class),ClassInitializer(?class, ?clinit).
                Reachable.add_production(&ClassInitializer.semijoin(&InitializedClass).map(|x| x.1));

                // Main (value-based) analysis
                let mut Assign = Relation::<_,(Var, Var)>::new(scope);
                let mut VarPointsTo = Relation::<_,(HeapAllocation, Var)>::new(scope);
                let mut CallGraphEdge = Relation::<_,(MethodInvocation, Method)>::new(scope);

                let VarPointsToRev = VarPointsTo.map(|x| (x.1, x.0)).arrange_by_key();

                // ArrayIndexPointsTo(?baseheap, ?heap) :-
                //  Reachable(?inmethod),
                //  StoreArrayIndex(?from, ?base, ?inmethod),
                //  VarPointsTo(?baseheap, ?base),
                //  VarPointsTo(?heap, ?from),
                //  HeapAllocation_Type(?heap, ?heaptype),
                //  HeapAllocation_Type(?baseheap, ?baseheaptype),
                //  ComponentType(?baseheaptype, ?componenttype),
                //  basic.SupertypeOf(?componenttype, ?heaptype).
                let ArrayIndexPointsTo =
                StoreArrayIndex
                    .map(|(f,b,m)| (m,(f,b)))
                    .semijoin(&Reachable)
                    .map(|(_m,(f,b))| (b,f))
                    .join_core(&VarPointsToRev, |_b,f,bh| Some((f.clone(), bh.clone())))
                    .join_core(&VarPointsToRev, |_f,bh,h| Some((h.clone(), bh.clone())))
                    .join(&HeapAllocation_Type)
                    .map(|(h,(bh,ht))| (bh,(h,ht)))
                    .join(&HeapAllocation_Type)
                    .map(|(bh,((h,ht),bht))| (bht,(h,ht,bh)))
                    .join(&ComponentType)
                    .map(|(_,((h,ht,bh),ct))| ((ct,ht),(bh,h)))
                    .semijoin(&SupertypeOf)
                    .map(|(_,(bh,h))| (bh,h));

                // Assign(?actual, ?formal) :-
                //  CallGraphEdge(?invocation, ?method),
                //  FormalParam(?index, ?method, ?formal),
                //  ActualParam(?index, ?invocation, ?actual).
                Assign.add_production(
                    &CallGraphEdge
                        .map(|x| (x.1, x.0))
                        .join(&FormalParam.map(|x| (x.1, (x.0, x.2))))
                        .map(|(_method, (inv, (index, formal)))| ((index, inv), formal))
                        .join(&ActualParam.map(|x| ((x.0, x.1), x.2)))
                        .map(|(_ind_inv, (formal, actual))| (actual, formal))
                );
                // Assign(?return, ?local) :-
                //  CallGraphEdge(?invocation, ?method),
                //  ReturnVar(?return, ?method),
                //  AssignReturnValue(?invocation, ?local).
                Assign.add_production(
                    &CallGraphEdge
                        .join(&AssignReturnValue)
                        .map(|(_inv, (meth, local))| (meth, local))
                        .join(&ReturnVar.map(|x| (x.1, x.0)))
                        .map(|(_meth, (local, ret))| (ret, local))
                );

                // InstanceFieldPointsTo(?heap, ?fld, ?baseheap) :-
                //  Reachable(?inmethod),
                //  StoreInstanceField(?from, ?base, ?fld, ?inmethod),
                //  VarPointsTo(?heap, ?from),
                //  VarPointsTo(?baseheap, ?base).
                let InstanceFieldPointsTo =
                StoreInstanceField
                    .map(|(from, base, fld, meth)| (meth, (from, base, fld)))
                    .semijoin(&Reachable)
                    .map(|(_, (from, base, fld))| (from, (base, fld)))
                    .join_core(&VarPointsToRev, |_from,(base,fld),heap| Some((base.clone(), (fld.clone(), heap.clone()))))
                    .join_core(&VarPointsToRev, |_base,(fld,heap),baseheap| Some((heap.clone(), fld.clone(), baseheap.clone())));

                // SIMPLIFICATION: ALL CGE DERIVATIONS PRODUCE REACHABILITY.
                Reachable.add_production(&CallGraphEdge.map(|x| x.1));

                // CallGraphEdge(?invocation, ?toMethod) :-
                //  Reachable(?inMethod),
                //  Instruction_Method(?invocation, ?inMethod),
                //  VirtualMethodInvocation_Base(?invocation, ?base),
                //  VarPointsTo(?heap, ?base),
                //  HeapAllocation_Type(?heap, ?heaptype),
                //  VirtualMethodInvocation_SimpleName(?invocation, ?simplename),
                //  VirtualMethodInvocation_Descriptor(?invocation, ?descriptor),
                //  basic.MethodLookup(?simplename, ?descriptor, ?heaptype, ?toMethod).
                CallGraphEdge.add_production(
                    &Reachable_Invocation
                        .join(&VirtualMethodInvocation_Base)
                        .map(|(inv, ((), base))| (base, inv))
                        .join_core(&VarPointsToRev, |_base,inv,heap| Some((heap.clone(), inv.clone())))
                        .join(&HeapAllocation_Type)
                        .map(|(_heap, (inv, heaptype))| (inv, heaptype))
                        .join(&VirtualMethodInvocation_SimpleName)
                        .join(&VirtualMethodInvocation_Descriptor)
                        .map(|(inv, ((heaptype, simplename), descriptor))| ((simplename, descriptor, heaptype), inv))
                        .join(&MethodLookup.map(|(s,d,h,t)| ((s,d,h),t)))
                        .map(|(_, (inv, to))| (inv, to))
                );

                // CallGraphEdge(?invocation, ?tomethod) :-
                //  Reachable(?inmethod),
                //  StaticMethodInvocation(?invocation, ?tomethod, ?inmethod).
                CallGraphEdge.add_production(
                    &StaticMethodInvocation
                        .map(|x| (x.2, (x.0, x.1)))
                        .semijoin(&Reachable)
                        .map(|(_inmethod, (inv, to))| (inv, to))
                );

                // CallGraphEdge(?invocation, ?tomethod),
                // VarPointsTo(?heap, ?this) :-
                //  Reachable(?inmethod),
                //  Instruction_Method(?invocation, ?inmethod),
                //  SpecialMethodInvocation_Base(?invocation, ?base),
                //  VarPointsTo(?heap, ?base),
                //  MethodInvocation_Method(?invocation, ?tomethod),
                //  ThisVar(?tomethod, ?this).
                let temp =
                Reachable_Invocation
                    .join(&SpecialMethodInvocation_Base)
                    .join(&MethodInvocation_Method)
                    .map(|(inv, (((), base), tomethod))| (base, (inv,tomethod)))
                    .join_core(&VarPointsToRev, |_base,(inv,tomethod),heap| Some((tomethod.clone(), (inv.clone(), heap.clone()))))
                    .join(&ThisVar)
                    .map(|(tomethod, ((inv,heap),this))| (inv, tomethod, heap, this));

                CallGraphEdge.add_production(&temp.map(|(i,t,_,_)| (i,t)));
                VarPointsTo.add_production(&temp.map(|(_,_,h,t)| (h,t)));

                // Reachable(?method) :- basic.MainMethodDeclaration(?method).
                Reachable.add_production(&MainMethodDeclaration);

                // StaticFieldPointsTo(?heap, ?fld) :-
                //  Reachable(?inmethod),
                //  StoreStaticField(?from, ?fld, ?inmethod),
                //  VarPointsTo(?heap, ?from).
                let StaticFieldPointsTo =
                StoreStaticField
                    .map(|(from, fld, meth)| (meth, (from, fld)))
                    .semijoin(&Reachable)
                    .map(|(_meth, (from, fld))| (from, fld))
                    .join_core(&VarPointsToRev, |_from,fld,heap| Some((heap.clone(), fld.clone())));

                let VarPointsToClone = VarPointsTo.clone();
                // VarPointsTo(?heap, ?var) :-
                //  AssignHeapAllocation(?heap, ?var, ?inMethod),
                //  Reachable(?inMethod).
                VarPointsTo.add_production(
                    &AssignHeapAllocation
                        .map(|x| (x.2, (x.0, x.1)))
                        .semijoin(&Reachable)
                        .map(|x| x.1)
                );

                // VarPointsTo(?heap, ?to) :- Assign(?from, ?to),VarPointsTo(?heap, ?from).
                VarPointsTo.add_production(
                    &VarPointsToClone
                        .map(|x| (x.1, x.0))
                        .join(&Assign)
                        .map(|(_from, (heap, to))| (heap, to))
                );

                // VarPointsTo(?heap, ?to) :-
                //  Reachable(?inmethod),
                //  AssignLocal(?from, ?to, ?inmethod),
                //  VarPointsTo(?heap, ?from).
                VarPointsTo.add_production(
                    &AssignLocal
                        .map(|(from, to, meth)| (meth, (from, to)))
                        .semijoin(&Reachable)
                        .map(|(_, (from, to))| (from, to))
                        .join_core(&VarPointsToRev, |_from,to,heap| Some((heap.clone(), to.clone())))
                );

                // VarPointsTo(?heap, ?to) :-
                //  Reachable(?method),
                //  AssignCast(?type, ?from, ?to, ?method),
                //  basic.SupertypeOf(?type, ?heaptype),
                //  HeapAllocation_Type(?heap, ?heaptype),
                //  VarPointsTo(?heap, ?from).
                VarPointsTo.add_production(
                    &AssignCast
                        .map(|(ty,f,to,m)| (m, (ty,f,to)))
                        .semijoin(&Reachable)
                        .map(|(_m, (ty,f,to))| (ty, (f,to)))
                        .join(&SupertypeOf)
                        .map(|(_ty, ((f,to), heaptype))| (heaptype, (f,to)))
                        .join(&HeapAllocation_Type.map(|x| (x.1, x.0)))
                        .map(|(_heaptype, ((f,to),heap))| ((heap, f), to))
                        .semijoin(&VarPointsToClone)
                        .map(|((heap, _f), to)| (heap, to))
                );

                // VarPointsTo(?heap, ?to) :-
                //  Reachable(?inmethod),
                //  LoadArrayIndex(?base, ?to, ?inmethod),
                //  VarPointsTo(?baseheap, ?base),
                //  ArrayIndexPointsTo(?baseheap, ?heap),
                //  Var_Type(?to, ?type),
                //  HeapAllocation_Type(?baseheap, ?baseheaptype),
                //  ComponentType(?baseheaptype, ?basecomponenttype),
                //  basic.SupertypeOf(?type, ?basecomponenttype).
                VarPointsTo.add_production(
                    &LoadArrayIndex
                        .map(|(b,t,m)| (m, (b,t)))
                        .semijoin(&Reachable)
                        .map(|(_m, (b,t))| (b,t))
                        .join_core(&VarPointsToRev, |_base,to,baseheap| Some((baseheap.clone(), to.clone())))
                        .join(&ArrayIndexPointsTo)
                        .join(&HeapAllocation_Type)
                        .map(|(_baseheap, ((to, heap), baseheaptype))| (to, (heap, baseheaptype)))
                        .join(&Var_Type)
                        .map(|(to, ((h,bht),ty))| (bht,(to,h,ty)))
                        .join(&ComponentType)
                        .map(|(_bht, ((to,h,ty),bct))| ((ty,bct), (to,h)))
                        .semijoin(&SupertypeOf)
                        .map(|(_, (to,h))| (h,to))
                );

                // VarPointsTo(?heap, ?to) :-
                //  Reachable(?inmethod),
                //  LoadInstanceField(?base, ?signature, ?to, ?inmethod),
                //  VarPointsTo(?baseheap, ?base),
                //  InstanceFieldPointsTo(?heap, ?signature, ?baseheap).
                VarPointsTo.add_production(
                    &LoadInstanceField
                        .map(|(b,s,t,m)| (m, (b,s,t)))
                        .semijoin(&Reachable)
                        .map(|(_m, (b,s,t))| (b,(s,t)))
                        .join_core(&VarPointsToRev, |_b,(s,t),bh| Some(((s.clone(),bh.clone()), t.clone())))
                        .join(&InstanceFieldPointsTo.map(|(h,s,bh)| ((s,bh),h)))
                        .map(|(_, (t,h))| (h,t))
                );

                // VarPointsTo(?heap, ?to) :-
                //  Reachable(?inmethod),
                //  LoadStaticField(?fld, ?to, ?inmethod),
                //  StaticFieldPointsTo(?heap, ?fld).
                VarPointsTo.add_production(
                    &LoadStaticField
                        .map(|(f,t,m)| (m, (f,t)))
                        .semijoin(&Reachable)
                        .map(|(_m,(f,t))| (f,t))
                        .join(&StaticFieldPointsTo.map(|x| (x.1, x.0)))
                        .map(|(_f,(t,h))| (h,t))
                );

                // VarPointsTo(?heap, ?this) :-
                //  Reachable(?inMethod),
                //  Instruction_Method(?invocation, ?inMethod),
                //  VirtualMethodInvocation_Base(?invocation, ?base),
                //  VarPointsTo(?heap, ?base),
                //  HeapAllocation_Type(?heap, ?heaptype),
                //  VirtualMethodInvocation_SimpleName(?invocation, ?simplename),
                //  VirtualMethodInvocation_Descriptor(?invocation, ?descriptor),
                //  basic.MethodLookup(?simplename, ?descriptor, ?heaptype, ?toMethod),
                //  ThisVar(?toMethod, ?this).
                VarPointsTo.add_production(
                    &Reachable_Invocation
                        .join(&VirtualMethodInvocation_Base)
                        .join(&VirtualMethodInvocation_SimpleName)
                        .join(&VirtualMethodInvocation_Descriptor)
                        .map(|(_inv, ((((), base), simplename), descriptor))| (base, (simplename, descriptor)))
                        .join_core(&VarPointsToRev, |_base,(name,desc),heap| Some((heap.clone(), (name.clone(), desc.clone()))))
                        .join(&HeapAllocation_Type)
                        .map(|(heap,((name,desc),heaptype))| ((name,desc,heaptype),heap))
                        .join(&MethodLookup.map(|(n,d,h,t)| ((n,d,h),t)))
                        .map(|((_n,_d,_t),(heap,to_method))| (to_method, heap))
                        .join(&ThisVar)
                        .map(|(_to_method, (heap, this))| (heap,this))
                );

                let result = (SupertypeOf.leave(), InitializedClass.leave(), Reachable.leave(), VarPointsTo.leave(), CallGraphEdge.leave());

                Reachable.complete();
                InitializedClass.complete();
                Assign.complete();
                VarPointsTo.complete();
                CallGraphEdge.complete();

                result
            });

            // st  .distinct()
            //     .map(|_| ())
            //     .consolidate()
            //     .inspect(|x| println!("ST: {:?}", x));

            // ic  .distinct()
            //     .map(|_| ())
            //     .consolidate()
            //     .inspect(|x| println!("IC: {:?}", x));

            _reachable
                .distinct()
                .map(|_| ())
                .consolidate()
                .inspect(|x| println!("Reach: {:?}", x))
                .probe_with(&mut probe);

            _varpoints
                .distinct()
                .map(|_| ())
                .consolidate()
                .inspect(|x| println!("VarPT: {:?}", x))
                .probe_with(&mut probe);

            _callgraph
                .distinct()
                .map(|_| ())
                .consolidate()
                .inspect(|x| println!("Graph: {:?}", x))
                .probe_with(&mut probe);

            // (input1, input2, input3, input4, input5, input6, input7, input8, input9, input10, input11, input12, input13, input14, input15, input16, input17, input18, input19, input20, input21, input22, input23, input24, input25, input26, input27, input28, input29, input30, input31)

        });

        if worker.index() == 0 {

            for input in inputs.0.iter_mut() { input.advance_to(1); input.flush(); }
            for input in inputs.1.iter_mut() { input.advance_to(1); input.flush(); }
            for input in inputs.2.iter_mut() { input.advance_to(1); input.flush(); }
            for input in inputs.3.iter_mut() { input.advance_to(1); input.flush(); }
            for input in inputs.4.iter_mut() { input.advance_to(1); input.flush(); }
            for input in inputs.5.iter_mut() { input.advance_to(1); input.flush(); }
            for input in inputs.6.iter_mut() { input.advance_to(1); input.flush(); }

            while probe.less_than(inputs.0[0].time()) { worker.step(); }

            println!("{:?}\tcomputation initalized", timer.elapsed());

            if batch > 0 {

                // Load methods from disk
                let methods_stuff = load7(index, &prefix, "Method.facts", interner.clone()).collect::<Vec<_>>();
                for (round, methods_thing) in methods_stuff.into_iter().enumerate() {

                    let round = round as Time;

                    // remove, advance, re-insert.
                    inputs.6[0].remove(methods_thing.0.clone());
                    for input in inputs.0.iter_mut() { input.advance_to(2 + round); input.flush(); }
                    for input in inputs.1.iter_mut() { input.advance_to(2 + round); input.flush(); }
                    for input in inputs.2.iter_mut() { input.advance_to(2 + round); input.flush(); }
                    for input in inputs.3.iter_mut() { input.advance_to(2 + round); input.flush(); }
                    for input in inputs.4.iter_mut() { input.advance_to(2 + round); input.flush(); }
                    for input in inputs.5.iter_mut() { input.advance_to(2 + round); input.flush(); }
                    for input in inputs.6.iter_mut() { input.advance_to(2 + round); input.flush(); }
                    inputs.6[0].insert(methods_thing.0.clone());

                    // step worker until remove has resolved.
                    if round % batch == batch - 1 {
                        while probe.less_than(inputs.0[0].time()) { worker.step(); }
                        println!("{:?}\tround {} complete", timer.elapsed(), round);
                    }

                }
            }
        }

    }).expect("Timely computation did not complete cleanly");
}
