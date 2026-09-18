//! The reconciliation is a merge, and a merge that does not converge is a
//! cluster whose nodes disagree forever. See `common::converge`; the long
//! run is in the `resilience` target.

mod common;

#[test]
fn random_histories_on_three_nodes_converge_to_the_last_word_on_each_name() {
    // `CELASTRO_RECONCILE_SEEDS=300` for a longer run by hand.
    let seeds: u64 =
        std::env::var("CELASTRO_RECONCILE_SEEDS").ok().and_then(|v| v.parse().ok()).unwrap_or(24);
    common::converge(seeds, 3, 20);
}
