use rand::Rng;
use stract::ranking::bitvec_similarity::BitVec;

fn random_bitvec(max_len: usize, max_id: usize) -> BitVec {
    let mut rng = rand::thread_rng();

    let mut ranks = Vec::with_capacity(max_len);

    for _ in 0..max_len {
        ranks.push(rng.gen_range(0..max_id) as u64);
    }

    ranks.sort_unstable();
    ranks.dedup();

    BitVec::new(ranks)
}

fn main() {
    let start = std::time::Instant::now();
    println!("(100, 100) max_id=1000");
    for _ in 0..100 {
        let a = random_bitvec(100, 1000);
        let b = random_bitvec(100, 1000);
        for _ in 0..10_000 {
            a.sim(&b);
        }
    }
    println!("time: {:?}", start.elapsed());

    let start = std::time::Instant::now();
    println!("(1_000, 1_000) max_id=100_000");

    for _ in 0..100 {
        let a = random_bitvec(1_000, 100_000);
        let b = random_bitvec(1_000, 100_000);
        for _ in 0..10_000 {
            a.sim(&b);
        }
    }
    println!("time: {:?}", start.elapsed());

    let start = std::time::Instant::now();
    println!("(1_000, 1_000) max_id=1_000_000");
    for _ in 0..100 {
        let a = random_bitvec(1_000, 1_000_000);
        let b = random_bitvec(1_000, 1_000_000);
        for _ in 0..10_000 {
            a.sim(&b);
        }
    }
    println!("time: {:?}", start.elapsed());

    let start = std::time::Instant::now();
    println!("(1_000, 1_000) max_id=usize::MAX");
    for _ in 0..100 {
        let a = random_bitvec(1_000, usize::MAX);
        let b = random_bitvec(1_000, usize::MAX);
        for _ in 0..10_000 {
            a.sim(&b);
        }
    }
    println!("time: {:?}", start.elapsed());

    let start = std::time::Instant::now();
    println!("(1_000, 10) max_id=usize::MAX");
    for _ in 0..100 {
        let a = random_bitvec(1_000, usize::MAX);
        let b = random_bitvec(10, usize::MAX);
        for _ in 0..10_000 {
            a.sim(&b);
        }
    }
    println!("time: {:?}", start.elapsed());
}
