#[derive(Debug)]
pub struct CapacitySnapshot {
    capacity: i64,
    allocated: i64,
    used: i64,
}

unsafe impl Send for CapacitySnapshot {}
unsafe impl Sync for CapacitySnapshot {}

impl From<(i64, i64, i64)> for CapacitySnapshot {
    fn from(value: (i64, i64, i64)) -> Self {
        CapacitySnapshot {
            capacity: value.0,
            allocated: value.1,
            used: value.2,
        }
    }
}

impl CapacitySnapshot {
    pub fn capacity(&self) -> i64 {
        self.capacity
    }
    pub fn allocated(&self) -> i64 {
        self.allocated
    }
    pub fn used(&self) -> i64 {
        self.used
    }

    pub fn available(&self) -> i64 {
        self.capacity - self.allocated - self.used
    }
}
