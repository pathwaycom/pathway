// #![feature(vec_push_all)]
// #![feature(test)]
// #![feature(collections)]
//
// extern crate differential_dataflow;
// extern crate rand;
// extern crate test;
//
// use rand::{Rng, SeedableRng, StdRng, Rand};
// use test::Bencher;
//
// use differential_dataflow::sort::*;
// use differential_dataflow::sort::radix::*;
//
// fn random_vec<T: Rand>(size: usize) -> Vec<T> {
//     let seed: &[_] = &[1, 2, 3, 4];
//     let mut rng: StdRng = SeedableRng::from_seed(seed);
//     // println!("{}", rng.gen::<f64>());
//
//     let mut result = Vec::with_capacity(size);
//     for _ in 0..size {
//         result.push(rng.gen());
//     }
//
//     result
// }
//
// #[bench]
// fn bench_rand_exp8(bencher: &mut Bencher) {
//     let mut data = random_vec::<(u32, u32)>(1 << 16);
//
//     let mut src = Vec::new();
//     for _ in 0..256 { src.push(Vec::with_capacity(data.len())); }
//
//     let mut dst = Vec::new();
//     for _ in 0..256 { dst.push(Vec::with_capacity(data.len())); }
//
//     bencher.bytes = 1 << 16;
//     bencher.iter(|| {
//         for s in &mut src { s.clear(); }
//         for d in &mut dst { d.clear(); }
//         src[0].push_all(&data[..]);
//
//         rsort_experimental8_buf(&mut src, &mut dst, &|x| x.0 as u64);
//         rsort_experimental8_buf(&mut dst, &mut src, &|x| x.0 as u64 >> 8);
//         rsort_experimental8_buf(&mut src, &mut dst, &|x| x.0 as u64 >> 16);
//         rsort_experimental8_buf(&mut dst, &mut src, &|x| x.0 as u64 >> 24);
//         for s in &mut src { isort_by(s, &|x| x); }
//     });
// }
//
//
// fn bench_rand<T: Ord+Rand+Clone, F: FnMut(&mut[(u32, T)])>(bencher: &mut Bencher, mut func: F) {
//     let mut data = random_vec::<(u32, T)>(1 << 16);
//     let mut data2 = Vec::new();
//
//     bencher.bytes = 1 << 16;
//     bencher.iter(|| {
//         data2.clear();
//         data2.push_all(&data[..]);
//         func(&mut data2)
//     });
// }
//
// // fn bench_sort<T: Ord+Rand+Clone, F: FnMut(&mut[(u32, T)])>(bencher: &mut Bencher, mut func: F) {
// //     let mut data = random_vec::<(u32, T)>(1 << 16);
// //     let mut data2 = Vec::new();
// //
// //     func(&mut data);
// //
// //     bencher.bytes = 1 << 16;
// //     bencher.iter(|| {
// //         data2.clear();
// //         data2.push_all(&data[..]);
// //         func(&mut data2)
// //     });
// // }
// // fn bench_same<T: Ord+Rand+Clone, F: FnMut(&mut[(u32, T)])>(bencher: &mut Bencher, mut func: F) {
// //     let mut data = random_vec::<(u32, T)>(1 << 16);
// //     let mut data2 = Vec::new();
// //
// //     for i in 0..data.len()  { data[i].0 = 0; }
// //
// //     bencher.bytes = 1 << 16;
// //     bencher.iter(|| {
// //         data2.clear();
// //         data2.push_all(&data[..]);
// //         func(&mut data2)
// //     });
// // }
//
// // #[bench] fn rand_sort(bencher: &mut Bencher) {
// //     bench_rand::<u32, _>(bencher, |x| x.sort());
// // }
// //
// // #[bench] fn rand_qsort(bencher: &mut Bencher) {
// //     bench_rand::<u32, _>(bencher, |x| qsort_by(x, &|y|&y.0));
// // }
//
// // #[bench] fn rand_rsort(bencher: &mut Bencher) {
// //     bench_rand::<u32, _>(bencher, |x| rsort_msb(x, &|y|y.0 as u64, &|z| qsort_by(z, &|w|&w.0)));
// // }
// //
// // #[bench] fn rand_rsort_safe(bencher: &mut Bencher) {
// //     bench_rand::<u32, _>(bencher, |x| rsort_msb_safe(x, &|y|y.0 as u64, &|z| qsort_by(z, &|w|&w.0)));
// // }
//
// #[bench] fn rand_rsort_bsaf(bencher: &mut Bencher) {
//     let mut buffer = vec![(0u32, 0u32); 8 * 256];
//     bench_rand::<u32, _>(bencher, |x| rsort_msb_buf(x, &mut buffer[..], &|y|y.0 as u64, &|z| qsort_by(z, &|q| q)));
// }
//
// #[bench] fn rand_rsort_c(bencher: &mut Bencher) {
//     bench_rand::<u32, _>(bencher, |x| rsort_msb_clv(x, &|y|y.0 as u64, &|z| qsort_by(z, &|q| q)));
// }
//
// // #[bench] fn same_sort(bencher: &mut Bencher) {
// //     bench_same::<(u32, u32), _>(bencher, |x| x.sort());
// // }
// //
// // #[bench] fn same_qsort(bencher: &mut Bencher) {
// //     bench_same::<(u32, u32), _>(bencher, |x| qsort_by(x, &|y|&y.0));
// // }
// //
// // #[bench] fn same_rsort(bencher: &mut Bencher) {
// //     bench_same::<(u32, u32), _>(bencher, |x| rsort_msb(x, &|y|y.0 as u64, &|z| qsort_by(z, &|w|&w.0)));
// // }
// //
// // #[bench] fn sort_sort(bencher: &mut Bencher) {
// //     bench_sort::<(u32, u32), _>(bencher, |x| x.sort());
// // }
// //
// // #[bench] fn sort_qsort(bencher: &mut Bencher) {
// //     bench_sort::<(u32, u32), _>(bencher, |x| qsort_by(x, &|y|&y.0));
// // }
// //
// // #[bench] fn sort_rsort(bencher: &mut Bencher) {
// //     bench_sort::<(u32, u32), _>(bencher, |x| rsort_msb(x, &|y|y.0 as u64, &|z| qsort_by(z, &|w|&w.0)));
// // }
