//! Client-side daemon for Zed's external thread message bridge.
//!
//! The crate is split so the protocol can be exercised without a daemon:
//! [`protocol`] speaks the bridge wire format, [`daemon`] implements one audit
//! cycle, and [`finding`] is the seam where "what to audit" enters from outside.

pub mod config;
pub mod daemon;
pub mod finding;
pub mod ledger;
pub mod logging;
pub mod protocol;
pub mod store;
pub mod time;
