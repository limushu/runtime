# pool-control-plane

整个 Pool 管控面业务只有一个 crate。MemberDisk、VirtualDisk、PoolNode 等领域是
这个 crate 内的 Rust module，而不是独立 Cargo package。

模块负责业务知识和元数据所有权；`control-runtime` 负责进程内 Service 容器、
对象准入、Workflow poll、结构化取消和观测。只有出现独立发布或复用需求时，
才把某个领域提升为单独 crate。
