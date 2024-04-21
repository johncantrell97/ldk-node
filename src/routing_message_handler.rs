use std::{ops::Deref, sync::Arc};

use lightning::{
	events::MessageSendEventsProvider, ln::msgs::RoutingMessageHandler, util::logger::Logger,
};

use crate::liquidity::LiquiditySource;

pub struct PeerConnectedNotifier<RM: Deref, L: Deref>
where
	RM::Target: RoutingMessageHandler,
	L::Target: Logger,
{
	route_handler: RM,
	liquidity_source: Option<Arc<LiquiditySource<L>>>,
}

impl<RM: Deref, L: Deref> PeerConnectedNotifier<RM, L>
where
	RM::Target: RoutingMessageHandler,
	L::Target: Logger,
{
	pub fn new(route_handler: RM, liquidity_source: Option<Arc<LiquiditySource<L>>>) -> Self {
		Self { route_handler, liquidity_source }
	}
}

impl<RM: Deref, L: Deref> RoutingMessageHandler for PeerConnectedNotifier<RM, L>
where
	RM::Target: RoutingMessageHandler,
	L::Target: Logger,
{
	fn handle_node_announcement(
		&self, msg: &lightning::ln::msgs::NodeAnnouncement,
	) -> Result<bool, lightning::ln::msgs::LightningError> {
		self.route_handler.handle_node_announcement(msg)
	}

	fn handle_channel_announcement(
		&self, msg: &lightning::ln::msgs::ChannelAnnouncement,
	) -> Result<bool, lightning::ln::msgs::LightningError> {
		self.route_handler.handle_channel_announcement(msg)
	}

	fn handle_channel_update(
		&self, msg: &lightning::ln::msgs::ChannelUpdate,
	) -> Result<bool, lightning::ln::msgs::LightningError> {
		self.route_handler.handle_channel_update(msg)
	}

	fn get_next_channel_announcement(
		&self, starting_point: u64,
	) -> Option<(
		lightning::ln::msgs::ChannelAnnouncement,
		Option<lightning::ln::msgs::ChannelUpdate>,
		Option<lightning::ln::msgs::ChannelUpdate>,
	)> {
		self.route_handler.get_next_channel_announcement(starting_point)
	}

	fn get_next_node_announcement(
		&self, starting_point: Option<&lightning::routing::gossip::NodeId>,
	) -> Option<lightning::ln::msgs::NodeAnnouncement> {
		self.route_handler.get_next_node_announcement(starting_point)
	}

	fn peer_connected(
		&self, their_node_id: &bitcoin::secp256k1::PublicKey, init: &lightning::ln::msgs::Init,
		inbound: bool,
	) -> Result<(), ()> {
		if let Some(liquidity_source) = &self.liquidity_source {
			liquidity_source.peer_connected(their_node_id);
		}
		self.route_handler.peer_connected(their_node_id, init, inbound)
	}

	fn handle_reply_channel_range(
		&self, their_node_id: &bitcoin::secp256k1::PublicKey,
		msg: lightning::ln::msgs::ReplyChannelRange,
	) -> Result<(), lightning::ln::msgs::LightningError> {
		self.route_handler.handle_reply_channel_range(their_node_id, msg)
	}

	fn handle_reply_short_channel_ids_end(
		&self, their_node_id: &bitcoin::secp256k1::PublicKey,
		msg: lightning::ln::msgs::ReplyShortChannelIdsEnd,
	) -> Result<(), lightning::ln::msgs::LightningError> {
		self.route_handler.handle_reply_short_channel_ids_end(their_node_id, msg)
	}

	fn handle_query_channel_range(
		&self, their_node_id: &bitcoin::secp256k1::PublicKey,
		msg: lightning::ln::msgs::QueryChannelRange,
	) -> Result<(), lightning::ln::msgs::LightningError> {
		self.route_handler.handle_query_channel_range(their_node_id, msg)
	}

	fn handle_query_short_channel_ids(
		&self, their_node_id: &bitcoin::secp256k1::PublicKey,
		msg: lightning::ln::msgs::QueryShortChannelIds,
	) -> Result<(), lightning::ln::msgs::LightningError> {
		self.route_handler.handle_query_short_channel_ids(their_node_id, msg)
	}

	fn processing_queue_high(&self) -> bool {
		self.route_handler.processing_queue_high()
	}

	fn provided_node_features(&self) -> lightning::ln::features::NodeFeatures {
		self.route_handler.provided_node_features()
	}

	fn provided_init_features(
		&self, their_node_id: &bitcoin::secp256k1::PublicKey,
	) -> lightning::ln::features::InitFeatures {
		self.route_handler.provided_init_features(their_node_id)
	}
}

impl<RM: Deref, L: Deref> MessageSendEventsProvider for PeerConnectedNotifier<RM, L>
where
	RM::Target: RoutingMessageHandler,
	L::Target: Logger,
{
	fn get_and_clear_pending_msg_events(&self) -> Vec<lightning::events::MessageSendEvent> {
		self.route_handler.get_and_clear_pending_msg_events()
	}
}
