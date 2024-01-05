// Copyright © 2024 Pathway

use ndarray::{arr0, ArrayD, ArrayViewD, Ix1, Ix2, LinalgScalar};

pub fn mat_mul<T>(a: &ArrayViewD<T>, b: &ArrayViewD<T>) -> Option<ArrayD<T>>
where
    T: LinalgScalar,
{
    if a.ndim() < 1 || 2 < a.ndim() || b.ndim() < 1 || 2 < b.ndim() {
        return None;
    } else if let Ok(a) = a.view().into_dimensionality::<Ix2>() {
        if a.shape()[1] != b.shape()[0] {
            return None;
        } else if let Ok(b) = b.view().into_dimensionality::<Ix2>() {
            return Some(a.dot(&b).into_dyn());
        } else if let Ok(b) = b.view().into_dimensionality::<Ix1>() {
            return Some(a.dot(&b).into_dyn());
        }
    } else if let Ok(a) = a.view().into_dimensionality::<Ix1>() {
        if a.shape()[0] != b.shape()[0] {
            return None;
        } else if let Ok(b) = b.view().into_dimensionality::<Ix2>() {
            return Some(a.dot(&b).into_dyn());
        } else if let Ok(b) = b.view().into_dimensionality::<Ix1>() {
            return Some(arr0(a.dot(&b)).into_dyn());
        }
    }
    None
}
