use std::collections::VecDeque;
use std::sync::{Arc, RwLock};

struct BoundedQueue<T> {
    inner: VecDeque<Vec<T>>,
    capacity: usize,
}

impl<T> BoundedQueue<T> {
    fn new(capacity: usize) -> Self {
        Self { inner: VecDeque::with_capacity(capacity), capacity }
    }

    fn push(&mut self, item: Vec<T>) {
        if self.inner.len() == self.capacity {
            self.inner.pop_front();
        }
        self.inner.push_back(item);
    }

    fn contains(&self, t: &T) -> bool where T: PartialEq,
    {
        self.inner.iter().any(|v| v.contains(t))
    }

    fn push_item(&mut self, item: T) {
        match self.inner.back_mut() {
            Some(v) => v.push(item),
            None => self.push(vec![item]),
        }
    }

    fn push_items(&mut self, item: Vec<T>) {
        match self.inner.back_mut() {
            Some(v) => v.extend(item),
            None => self.push(item),
        }
    }
}

pub struct SharedQueue<T> {
    inner: Arc<RwLock<BoundedQueue<T>>>
}

impl<T> Clone for SharedQueue<T> {
    fn clone(&self) -> Self {
        Self { inner: Arc::clone(&self.inner) }
    }
}

impl<T> SharedQueue<T> {
    pub fn new(capacity: usize) -> SharedQueue<T> {
        Self { inner: Arc::new(RwLock::new(BoundedQueue::new(capacity))) }
    }

    pub fn push(&self, item: Vec<T>) {
        self.inner.write().unwrap().push(item);
    }

    pub fn contains(&self, t: &T) -> bool
    where
        T: PartialEq,
    {
        self.inner.read().unwrap().contains(t)
    }

    pub fn push_item(&self, item: T) {
        self.inner.write().unwrap().push_item(item);
    }

    pub fn push_items(&self, item: Vec<T>) {
        self.inner.write().unwrap().push_items(item);
    }
}
