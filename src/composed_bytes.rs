use bytes::{Bytes, BytesMut};

/// To compose multi Bytes into one for zero copy.
#[derive(Clone, Debug)]
pub struct ComposedBytes {
    composed: Vec<Bytes>,
    total_len: usize,
}

impl ComposedBytes {
    pub fn new() -> ComposedBytes {
        Self {
            composed: vec![],
            total_len: 0,
        }
    }

    pub fn from(all: Vec<Bytes>) -> ComposedBytes {
        let len = all.iter().map(|x| x.len()).sum();
        Self {
            composed: all,
            total_len: len,
        }
    }

    pub fn put(&mut self, bytes: Bytes) {
        self.total_len += bytes.len();
        self.composed.push(bytes);
    }

    /// this is expensive to consume like the Bytes
    pub fn freeze(&self) -> Bytes {
        let mut bytesMut = BytesMut::with_capacity(self.total_len);
        for x in self.composed.iter() {
            bytesMut.extend_from_slice(x);
        }
        bytesMut.freeze()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Bytes> + '_ {
        self.composed.iter()
    }

    pub fn len(&self) -> usize {
        self.total_len
    }
}

#[cfg(test)]
mod test {
    use crate::composed_bytes::ComposedBytes;
    use bytes::Bytes;

    #[test]
    fn test_bytes() {
        let mut composed = ComposedBytes::new();
        composed.put(Bytes::copy_from_slice(b"hello"));
        composed.put(Bytes::copy_from_slice(b"world"));
        assert_eq!(12, composed.len());

        let mut iter = composed.iter();
        assert_eq!(b"hello", iter.next().as_ref());
        assert_eq!(b"world", iter.next().as_ref());

        let data = composed.freeze();
        assert_eq!(b"helloworld", data.as_ref());
    }
}