use futures::future::join_all;
use l337::Pool;
use l337_redis::RedisConnectionManager;
use redis::RedisResult;
use std::{sync::Arc, time::Duration};

async fn blpop_helper(pool: Arc<Pool<RedisConnectionManager>>) -> RedisResult<()> {
    let mut conn = pool.connection().await.unwrap();

    match redis::cmd("blpop")
        .arg("random_key")
        .arg(10)
        .query_async::<_, ()>(&mut *conn)
        .await
    {
        Ok(_) => println!("Successful blpop"),
        Err(e) => println!("Unsuccessful blpop: {:?}", e),
    };

    Ok(())
}

async fn ping_helper(pool: Arc<Pool<RedisConnectionManager>>) -> RedisResult<()> {
    match pool.connection().await {
        Ok(mut conn) => {
            match redis::cmd("PING").query_async::<_, ()>(&mut *conn).await {
                Ok(_) => println!("Successful ping"),
                Err(_) => println!("Unsuccessful ping"),
            };
        }
        Err(_) => println!("could not grab connection from pool"),
    };

    Ok(())
}

#[tokio::main]
async fn main() -> RedisResult<()> {
    let mngr = RedisConnectionManager::new("redis://redis:6379/0").unwrap();
    let config = Default::default();
    let pool = Pool::new(mngr, config).await.unwrap();

    let pool_arc = Arc::new(pool);

    let blpop_pool = Arc::clone(&pool_arc);
    tokio::spawn(async move {
        let mut blpop_futures = vec![];
        println!("Spawning {} blpop commands", blpop_pool.max_conns());
        for _ in std::usize::MIN..blpop_pool.max_conns() {
            blpop_futures.push(blpop_helper(Arc::clone(&blpop_pool)));
        }
        join_all(blpop_futures).await;
        println!("Finished all blpop commands");
    });

    tokio::time::delay_for(Duration::from_millis(100)).await;

    println!("Attempting to start pinging...");
    let ping_pool = Arc::clone(&pool_arc);
    let mut interval = tokio::time::interval(Duration::from_millis(100));
    loop {
        interval.tick().await;
        ping_helper(Arc::clone(&ping_pool)).await;
    }
}
