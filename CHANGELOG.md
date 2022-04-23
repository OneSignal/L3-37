# 0.10.0

- Meta
    - Update MSRV to 1.49 for tokio's dependency to [std::hint::spin_loop].

- l337-redis
    - Upgrade redis crate to 0.21

[std::hint::spin_loop]: https://doc.rust-lang.org/std/hint/fn.spin_loop.html

# 0.8.0

- Replace `log` events with `tracing` spans

# 0.7.0

- Meta
    - Update MSRV to 1.48

- l337
    - Upgrade to tokio 1.x
- l337-redis
    - Upgrade to tokio 1.x
    - Upgrade to redis 0.20
- l337-postgres
    - Upgrade to tokio 1.x
    - Upgrade to tokio-postgres 0.7
