extern crate kuchiki;

fn main() {
    let mut depth = 2;
    // 20 M nodes is a few GB of memory.
    while depth <= 20_000_000 {
        let mut node = kuchiki::NodeRef::new_text("");
        for _ in 0..depth {
            let parent = kuchiki::NodeRef::new_text("");
            parent.append(node);
            node = parent;
        }

        println!("Trying to drop {depth} nodes...");
        // Without an explicit `impl Drop for Node`,
        // depth = 20_000 causes "thread '<main>' has overflowed its stack"
        // on my machine (Linux x86_64).
        ::std::mem::drop(node);

        depth *= 10;
    }
}
