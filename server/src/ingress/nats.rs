use crate::option::CONFIG;
use async_nats::jetstream::{self, consumer};
use futures::StreamExt;
use std::thread;

fn async_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .thread_name("nats-thread")
        .enable_all()
        .build()
        .unwrap()
}

pub async fn new() -> Result<(), async_nats::Error> {
    // Get Nats addr from config
    let addr = &CONFIG.parseable.nats_addr;
    println!("This is NATS address {:?}", addr);

    let client = async_nats::connect(addr).await?;
    let js = jetstream::new(client.clone());

    let stream = js.get_stream("parseable").await?;
    stream
        .create_consumer(consumer::Config {
            name: Some("parseable_cons".into()),
            deliver_subject: Some("parseable.event".into()),
            deliver_group: Some("parseable_group".into()),
            ack_policy: consumer::AckPolicy::All,
            ..Default::default()
        })
        .await?;

    let cons: consumer::PushConsumer = match stream.get_consumer("parseable_cons").await {
        Ok(mut consumer) => {
            let name = &consumer.info().await?.name;
            println!("Consumer {} is created.", name);

            consumer
        }
        Err(err) => {
            println!("Err: {}", err);
            return Err(err);
        }
    };

    // Subscription
    let mut _sub = client
        .queue_subscribe("parseable.event".into(), "parseable_group".into())
        .await?;

    let rt = async_runtime();

    thread::spawn(move || {
        rt.block_on(async move {
            while let Some(msg) = cons.messages().await.unwrap().next().await {
                // acknowledge
                let _res = msg.as_ref().unwrap().ack().await;
                println!("{:?}", msg.unwrap().payload);
            }
        })
    });

    Ok(())
}
