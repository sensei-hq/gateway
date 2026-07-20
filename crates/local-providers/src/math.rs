//! Small numeric helpers shared by every adapter. Kept outside any feature
//! gate so the unit tests run under the default test profile (no C++ build).

/// In-place L2 normalisation. Replaces the input vector with the unit-length
/// version of itself. If `vec` is empty or has zero magnitude, leaves it
/// unchanged (the all-zero vector cannot be normalised; returning it as-is
/// matches what sentence-transformers does in the same edge case).
pub fn l2_normalize_in_place(vec: &mut [f32]) {
    let norm_sq: f32 = vec.iter().map(|x| x * x).sum();
    if norm_sq == 0.0 {
        return;
    }
    let norm = norm_sq.sqrt();
    for x in vec.iter_mut() {
        *x /= norm;
    }
}

/// Allocating version of [`l2_normalize_in_place`].
pub fn l2_normalize(vec: &[f32]) -> Vec<f32> {
    let mut out = vec.to_vec();
    l2_normalize_in_place(&mut out);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approx(a: f32, b: f32) -> bool {
        (a - b).abs() < 1e-6
    }

    #[test]
    fn normalising_a_unit_vector_returns_an_identical_vector() {
        let v = [1.0f32, 0.0, 0.0];
        let out = l2_normalize(&v);
        assert!(approx(out[0], 1.0));
        assert!(approx(out[1], 0.0));
        assert!(approx(out[2], 0.0));
    }

    #[test]
    fn normalising_produces_unit_magnitude() {
        let v = [3.0f32, 4.0];
        let out = l2_normalize(&v);
        let mag: f32 = out.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!(approx(mag, 1.0));
        // 3/5 and 4/5 exactly
        assert!(approx(out[0], 0.6));
        assert!(approx(out[1], 0.8));
    }

    #[test]
    fn normalising_an_empty_vector_is_a_noop() {
        let v: Vec<f32> = vec![];
        let out = l2_normalize(&v);
        assert!(out.is_empty());
    }

    #[test]
    fn normalising_an_all_zero_vector_leaves_it_unchanged() {
        let v = vec![0.0f32; 10];
        let out = l2_normalize(&v);
        assert!(out.iter().all(|x| *x == 0.0));
    }

    #[test]
    fn in_place_form_mutates_the_argument() {
        let mut v = vec![3.0f32, 4.0];
        l2_normalize_in_place(&mut v);
        assert!(approx(v[0], 0.6));
        assert!(approx(v[1], 0.8));
    }

    #[test]
    fn negative_components_are_preserved_after_normalisation() {
        let mut v = vec![-3.0f32, 4.0];
        l2_normalize_in_place(&mut v);
        assert!(approx(v[0], -0.6));
        assert!(approx(v[1], 0.8));
    }
}
