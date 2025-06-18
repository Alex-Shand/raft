use futures::{StreamExt, stream::FuturesUnordered};
use tokio::sync::{
    RwLock, RwLockMappedWriteGuard, RwLockReadGuard, RwLockWriteGuard,
};

pub(crate) trait Race {
    async fn race(self);
}

impl<I, F> Race for I
where
    I: Iterator<Item = F>,
    F: Future<Output = ()>,
{
    async fn race(self) {
        let mut futures = self.collect::<FuturesUnordered<_>>();
        while let Some(()) = futures.next().await {}
    }
}

pub(crate) struct Locked<T>(RwLock<Option<T>>);

impl<T> Locked<T> {
    pub(crate) fn new(t: T) -> Self {
        Locked(RwLock::new(Some(t)))
    }

    pub(crate) async fn set(&self, new: T) {
        *self.0.write().await = Some(new);
    }

    pub(crate) async fn update(&self, f: impl FnOnce(T) -> T) {
        let mut guard = self.0.write().await;
        let t = guard.take().unwrap();
        let new = f(t);
        *guard = Some(new);
    }

    pub(crate) async fn read(&self) -> RwLockReadGuard<'_, T> {
        RwLockReadGuard::map(self.0.read().await, |opt| opt.as_ref().unwrap())
    }

    pub(crate) async fn write(&self) -> RwLockMappedWriteGuard<'_, T> {
        RwLockWriteGuard::map(self.0.write().await, |opt| opt.as_mut().unwrap())
    }
}

impl<T: Copy> Locked<T> {
    pub(crate) async fn get(&self) -> T {
        self.0.read().await.unwrap()
    }
}

impl Locked<usize> {
    pub(crate) async fn incr(&self) -> usize {
        let mut result = 0;
        self.update(|i| {
            result = i + 1;
            result
        })
        .await;
        result
    }
}
