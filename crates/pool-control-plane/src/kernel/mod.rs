use std::fmt;
use std::sync::Arc;

macro_rules! domain_id {
    ($name:ident) => {
        #[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(Arc<str>);

        impl $name {
            pub fn new(value: impl Into<Arc<str>>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_tuple(stringify!($name))
                    .field(&self.0)
                    .finish()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

domain_id!(PoolId);
domain_id!(PhysicalDiskId);
domain_id!(MemberDiskId);
domain_id!(TierId);
domain_id!(VirtualDiskId);
domain_id!(BgId);
domain_id!(BlkId);
domain_id!(NodeId);
domain_id!(FailureDomainId);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ByteCount(u64);

impl ByteCount {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_do_not_leak_domain_state() {
        assert_eq!(MemberDiskId::new("md-1").as_str(), "md-1");
        assert_eq!(ByteCount::new(1 << 30).get(), 1 << 30);
    }
}
