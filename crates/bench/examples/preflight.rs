fn main() {
    for c in [200usize, 1_000, 5_000, 10_000, 25_000, 50_000] {
        let pre = lb_bench::harness::Preflight::evaluate(c, 1.0);
        let blocking = pre.blocking();
        println!("  c={:<7} {}", c, if blocking.is_empty() { "OK".to_string() }
            else { format!("BLOCKED by: {}", blocking.iter().map(|b| b.name).collect::<Vec<_>>().join(", ")) });
    }
    println!("\n  max supported right now: {}", lb_bench::harness::Preflight::max_supported(1.0));
}
