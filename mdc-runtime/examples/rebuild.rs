use mdc_runtime::{
    Submission, TaskExit,
    demo::{DemoPool, DiskId, DiskRequest},
};

#[tokio::main]
async fn main() {
    let pool = DemoPool::start([DiskId::new("disk-1")])
        .await
        .expect("start demo pool");

    let Submission::Task(ticket) = pool
        .disk
        .client
        .submit(DiskRequest::Offline(DiskId::new("disk-1")))
        .await
        .expect("submit disk offline")
    else {
        panic!("offline is expected to create a task");
    };

    match ticket.wait().await.expect("wait for disk offline") {
        TaskExit::Completed(response) => println!("workflow completed: {response:?}"),
        exit => println!("workflow stopped: {exit:?}"),
    }
}
