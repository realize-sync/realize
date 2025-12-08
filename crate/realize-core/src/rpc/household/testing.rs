use crate::rpc::peer_capnp::connected_peer;
use crate::rpc::store_capnp::store::{
    self, ArenasParams, ArenasResults, ReadParams, ReadResults, RsyncParams, RsyncResults,
    SubscriptionsParams, SubscriptionsResults, WithRateLimitParams, WithRateLimitResults,
};
use crate::rpc::testing::HouseholdFixture;
use capnp::capability::Promise;
use capnp_rpc::pry;
use std::cell::RefCell;
use std::rc::Rc;

pub(crate) struct FakeConnectedPeer(pub(crate) Rc<RefCell<Vec<String>>>);
impl connected_peer::Server for FakeConnectedPeer {
    fn store(
        self: Rc<Self>,
        _: connected_peer::StoreParams,
        mut results: connected_peer::StoreResults,
    ) -> Promise<(), capnp::Error> {
        self.0
            .borrow_mut()
            .push("ConnectedPeer.store()".to_string());
        results
            .get()
            .set_store(capnp_rpc::new_client(FakeStore(self.0.clone(), None)));

        Promise::ok(())
    }
}

struct FakeStore(Rc<RefCell<Vec<String>>>, Option<f64>);
impl store::Server for FakeStore {
    fn with_rate_limit(
        self: Rc<Self>,
        params: WithRateLimitParams,
        mut results: WithRateLimitResults,
    ) -> Promise<(), capnp::Error> {
        let rate_limit = pry!(params.get()).get_rate_limit();
        self.0
            .borrow_mut()
            .push(format!("Store.with_rate_limit({rate_limit})"));

        results.get().set_store(capnp_rpc::new_client(FakeStore(
            self.0.clone(),
            Some(rate_limit),
        )));

        Promise::ok(())
    }

    fn arenas(self: Rc<Self>, _: ArenasParams, mut results: ArenasResults) -> Promise<(), capnp::Error> {
        let mut list = results.get().init_arenas(1);
        list.set(0, HouseholdFixture::test_arena().as_str());

        Promise::ok(())
    }

    fn subscriptions(
        self: Rc<Self>,
        _: SubscriptionsParams,
        _: SubscriptionsResults,
    ) -> Promise<(), capnp::Error> {
        self.0
            .borrow_mut()
            .push(format!("Store.subscriptions() rate_limit={:?}", self.1));

        Promise::ok(())
    }

    fn read(self: Rc<Self>, params: ReadParams, _: ReadResults) -> Promise<(), capnp::Error> {
        self.0
            .borrow_mut()
            .push(format!("Store.read() rate_limit={:?}", self.1));

        // send one chunk so the other side knows read has started.
        let cb = pry!(pry!(params.get()).get_cb());
        let request = cb.chunk_request();
        tokio::task::spawn_local(request.send());

        Promise::ok(())
    }

    fn rsync(self: Rc<Self>, _: RsyncParams, _: RsyncResults) -> Promise<(), capnp::Error> {
        self.0
            .borrow_mut()
            .push(format!("Store.rsync() rate_limit={:?}", self.1));

        Promise::ok(())
    }
}
