extern crate graph_map;

use graph_map::GraphMMap;

fn main() {

    let filename = std::env::args().nth(1).expect("Must supply filename");
    let rootnode = std::env::args().nth(2).expect("Must supply root node").parse().expect("Invalid root node");

    let graph = GraphMMap::new(&filename);

    let timer = ::std::time::Instant::now();
    breadth_first(&graph, rootnode);
    println!("{:?}\tbreadth_first", timer.elapsed());

    let timer = ::std::time::Instant::now();
    breadth_first_hash(&graph, rootnode);
    println!("{:?}\tbreadth_first_hash", timer.elapsed());

    let timer = ::std::time::Instant::now();
    union_find(&graph);
    println!("{:?}\tunion_find", timer.elapsed());

    let timer = ::std::time::Instant::now();
    union_find_hash(&graph);
    println!("{:?}\tunion_find_hash", timer.elapsed());

}

fn breadth_first(graph: &GraphMMap, root: u32) {

    let nodes = graph.nodes() as u32;

    let mut reached = vec![false; nodes as usize];
    let mut buff1 = Vec::with_capacity(nodes as usize);
    let mut buff2 = Vec::with_capacity(nodes as usize);

    reached[root as usize] = true;
    buff1.push(root);

    while !buff1.is_empty() {
        buff1.sort_unstable();          // useful here, not for hashset tho.
        for node in buff1.drain(..) {
            for &edge in graph.edges(node as usize) {
                unsafe {
                    if !*reached.get_unchecked(edge as usize) {
                        *reached.get_unchecked_mut(edge as usize) = true;
                        buff2.push(edge);
                    }
                }
            }
        }
        ::std::mem::swap(&mut buff1, &mut buff2);
    }
}

fn breadth_first_hash(graph: &GraphMMap, root: u32) {

    use std::collections::HashSet;

    let nodes = graph.nodes() as u32;

    let mut reached = HashSet::new();
    let mut buff1 = Vec::with_capacity(nodes as usize);
    let mut buff2 = Vec::with_capacity(nodes as usize);

    reached.insert(root);
    buff1.push(root);

    while !buff1.is_empty() {
        for node in buff1.drain(..) {
            for &edge in graph.edges(node as usize) {
                if !reached.contains(&edge) {
                    reached.insert(edge);
                    buff2.push(edge);
                }
            }
        }
        ::std::mem::swap(&mut buff1, &mut buff2);
    }
}

fn union_find(graph: &GraphMMap) {

    let nodes = graph.nodes() as u32;
    let mut roots: Vec<u32> = (0..nodes).collect();      // u32 works, and is smaller than uint/u64
    let mut ranks: Vec<u8> = vec![0u8; nodes as usize];  // u8 should be large enough (n < 2^256)

    for node in 0 .. graph.nodes() {
        for &edge in graph.edges(node) {

            let mut x = node as u32;
            let mut y = edge as u32;

            // x = roots[x as usize];
            // y = roots[y as usize];
            x = unsafe { *roots.get_unchecked(x as usize) };
            y = unsafe { *roots.get_unchecked(y as usize) };

            // while x != roots[x as usize] { x = roots[x as usize]; }
            // while y != roots[y as usize] { y = roots[y as usize]; }
            unsafe { while x != *roots.get_unchecked(x as usize) { x = *roots.get_unchecked(x as usize); } }
            unsafe { while y != *roots.get_unchecked(y as usize) { y = *roots.get_unchecked(y as usize); } }

            if x != y {
                unsafe {
                    match ranks[x as usize].cmp(&ranks[y as usize]) {
                        std::cmp::Ordering::Less    => *roots.get_unchecked_mut(x as usize) = y as u32,
                        std::cmp::Ordering::Greater => *roots.get_unchecked_mut(y as usize) = x as u32,
                        std::cmp::Ordering::Equal   => { *roots.get_unchecked_mut(y as usize) = x as u32;
                                                         *ranks.get_unchecked_mut(x as usize) += 1 },
                    }
                }
            }
        }
    }
}

fn union_find_hash(graph: &GraphMMap) {


    use std::collections::HashMap;
    let nodes = graph.nodes() as u32;
    let mut roots: HashMap<u32,u32> = (0..nodes).map(|x| (x,x)).collect();
    let mut ranks: HashMap<u32,u8> =  (0..nodes).map(|x| (x,0)).collect();

    for node in 0 .. graph.nodes() {
        for &edge in graph.edges(node) {

            let mut x = node as u32;
            let mut y = edge as u32;

            x = roots[&x];
            y = roots[&y];

            while x != roots[&x] { x = roots[&x]; }
            while y != roots[&y] { y = roots[&y]; }

            if x != y {
                match ranks[&x].cmp(&ranks[&y]) {
                    std::cmp::Ordering::Less    => *roots.get_mut(&x).unwrap() = y,
                    std::cmp::Ordering::Greater => *roots.get_mut(&y).unwrap() = x,
                    std::cmp::Ordering::Equal   => { *roots.get_mut(&y).unwrap() = x; *ranks.get_mut(&x).unwrap() += 1; },
                }
            }
        }
    }
}