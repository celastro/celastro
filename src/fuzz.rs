//! A seeded mutation fuzzer for the tests: every parser that reads the
//! network or a file is fed thousands of mutants of valid inputs and must
//! answer `Ok` or `Err`, never panic. A panic is an abort in release, so
//! each one a mutant finds is a crash a peer or a corrupt file can cause.
//!
//! Deterministic by seed, so a failure is a repeatable input; bounded by
//! `rounds`, so the suite's time is known. The mutations are the ones that
//! find parser bugs: a bit flipped, a byte set to an edge value, a range
//! cut or doubled, bytes inserted, the input truncated, two samples
//! spliced, and a length-shaped window set to a large or small integer.

use crate::codec::Rng;

/// `rounds` mutants of `samples`, each handed to `f` after the samples
/// themselves. `f` returning is the property; whatever it returns is
/// ignored.
pub(crate) fn sweep<T>(
    seed: u64,
    samples: &[Vec<u8>],
    rounds: usize,
    mut f: impl FnMut(&[u8]) -> T,
) {
    assert!(!samples.is_empty(), "a sweep needs at least one valid sample");
    for s in samples {
        f(s);
    }
    let mut rng = Rng::new(seed);
    for _ in 0..rounds {
        let m = mutate(&mut rng, samples);
        f(&m);
    }
}

/// The same over text: mutants are bytes made into a string lossily, so
/// invalid UTF-8 becomes replacement characters and the parser still sees
/// something a client could send.
pub(crate) fn sweep_text<T>(
    seed: u64,
    samples: &[&str],
    rounds: usize,
    mut f: impl FnMut(&str) -> T,
) {
    let bytes: Vec<Vec<u8>> = samples.iter().map(|s| s.as_bytes().to_vec()).collect();
    sweep(seed, &bytes, rounds, |b| f(&String::from_utf8_lossy(b)));
}

fn below(rng: &mut Rng, n: usize) -> usize {
    if n == 0 {
        0
    } else {
        (rng.next_u64() % n as u64) as usize
    }
}

/// One mutant: a sample with one to four mutations applied.
pub(crate) fn mutate(rng: &mut Rng, samples: &[Vec<u8>]) -> Vec<u8> {
    let mut b = samples[below(rng, samples.len())].clone();
    let steps = 1 + below(rng, 4);
    for _ in 0..steps {
        if b.is_empty() {
            b.push(rng.next_u64() as u8);
            continue;
        }
        let at = below(rng, b.len());
        match below(rng, 10) {
            0 => b[at] ^= 1 << below(rng, 8),
            1 => b[at] = [0x00, 0xff, 0x7f, 0x80, 0x01][below(rng, 5)],
            2 => b[at] = rng.next_u64() as u8,
            3 => {
                let end = (at + 1 + below(rng, 16)).min(b.len());
                b.drain(at..end);
            }
            4 => {
                let end = (at + 1 + below(rng, 16)).min(b.len());
                let chunk: Vec<u8> = b[at..end].to_vec();
                for (i, c) in chunk.into_iter().enumerate() {
                    b.insert(at + i, c);
                }
            }
            5 => {
                for _ in 0..1 + below(rng, 8) {
                    b.insert(at, rng.next_u64() as u8);
                }
            }
            6 => b.truncate(at),
            7 => {
                let other = &samples[below(rng, samples.len())];
                if !other.is_empty() {
                    let from = below(rng, other.len());
                    let take = (from + 1 + below(rng, 32)).min(other.len());
                    b.splice(at..at, other[from..take].iter().copied());
                }
            }
            8 => {
                // A length field made huge or tiny: the classic.
                let width = [1usize, 2, 4, 8][below(rng, 4)];
                let value: u64 =
                    [0, 1, 0x7f, 0xff, 0xffff, 0x7fff_ffff, 0xffff_ffff, u64::MAX][below(rng, 8)];
                for i in 0..width {
                    if at + i < b.len() {
                        b[at + i] = (value >> (8 * (width - 1 - i))) as u8;
                    }
                }
            }
            _ => {
                let end = (at + 1 + below(rng, 64)).min(b.len());
                b[at..end].reverse();
            }
        }
    }
    b
}
