use crate::cmd::{Parse, ParseError, Unknown};
use crate::connection::ConnectionWriter;
use crate::{Command, Connection, Db, Frame, Shutdown};

use bytes::Bytes;
use std::pin::Pin;
use tokio::sync::broadcast;
use tokio_stream::{Stream, StreamMap};

/// Subscribes the client to one or more channels.
///
/// Once the client enters the subscribed state, it is not supposed to issue any
/// other commands, except for additional SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE,
/// PUNSUBSCRIBE, PING and QUIT commands.
#[derive(Debug)]
pub struct Subscribe {
    channels: Vec<String>,
}

/// Unsubscribes the client from one or more channels.
///
/// When no channels are specified, the client is unsubscribed from all the
/// previously subscribed channels.
#[derive(Clone, Debug)]
pub struct Unsubscribe {
    channels: Vec<String>,
}

/// Stream of messages. The stream receives messages from the
/// `broadcast::Receiver`. We use `stream!` to create a `Stream` that consumes
/// messages. Because `stream!` values cannot be named, we box the stream using
/// a trait object.
type Messages = Pin<Box<dyn Stream<Item = Bytes> + Send>>;

impl Subscribe {
    /// Creates a new `Subscribe` command to listen on the specified channels.
    pub(crate) fn new(channels: Vec<String>) -> Subscribe {
        Subscribe { channels }
    }

    /// Parse a `Subscribe` instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `SUBSCRIBE` string has already been consumed.
    ///
    /// # Returns
    ///
    /// On success, the `Subscribe` value is returned. If the frame is
    /// malformed, `Err` is returned.
    ///
    /// # Format
    ///
    /// Expects an array frame containing two or more entries.
    ///
    /// ```text
    /// SUBSCRIBE channel [channel ...]
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Subscribe> {
        use ParseError::EndOfStream;

        // The `SUBSCRIBE` string has already been consumed. At this point,
        // there is one or more strings remaining in `parse`. These represent
        // the channels to subscribe to.
        //
        // Extract the first string. If there is none, the the frame is
        // malformed and the error is bubbled up.
        let mut channels = vec![parse.next_string()?];

        // Now, the remainder of the frame is consumed. Each value must be a
        // string or the frame is malformed. Once all values in the frame have
        // been consumed, the command is fully parsed.
        loop {
            match parse.next_string() {
                // A string has been consumed from the `parse`, push it into the
                // list of channels to subscribe to.
                Ok(s) => channels.push(s),
                // The `EndOfStream` error indicates there is no further data to
                // parse.
                Err(EndOfStream) => break,
                // All other errors are bubbled up, resulting in the connection
                // being terminated.
                Err(err) => return Err(err.into()),
            }
        }

        Ok(Subscribe { channels })
    }

    /// Apply the `Subscribe` command to the specified `Db` instance.
    ///
    /// This function is the entry point and includes the initial list of
    /// channels to subscribe to. Additional `subscribe` and `unsubscribe`
    /// commands may be received from the client and the list of subscriptions
    /// are updated accordingly.
    ///
    /// [here]: https://redis.io/topics/pubsub
    pub(crate) async fn apply(
        mut self,
        db: &Db,
        dst: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> crate::Result<()> {
        // Each individual channel subscription is handled using a
        // `sync::broadcast` channel. Messages are then fanned out to all
        // clients currently subscribed to the channels.
        //
        // An individual client may subscribe to multiple channels and may
        // dynamically add and remove channels from its subscription set. To
        // handle this, a `StreamMap` is used to track active subscriptions. The
        // `StreamMap` merges messages from individual broadcast channels as
        // they are received.
        let mut subscriptions = StreamMap::new();

        let reader = &mut dst.reader;
        let reader_stream = async_stream::stream! {
            while let Some(frame) = reader.read_frame().await.transpose() {
                yield frame;
            }
        };
        let writer = &mut dst.writer;

        for channel_name in self.channels.drain(..) {
            subscribe_to_channel(
                channel_name.clone(),
                |messages| {
                    subscriptions.insert(channel_name, messages);
                    subscriptions.len()
                },
                db,
                writer,
            )
            .await?;
        }

        join_me_maybe::join!(
            // TODO: This isn't entirely correct as-is, because if the `StreamMap` is ever empty,
            // it'll return `Poll::Ready(None)` from `poll_next`, and `join_me_maybe` will
            // interpret that as end-of-stream and drop it immediately. To be robust to that
            // situation (you unsubscribe from all channels but then resubscribe to something
            // later?) we'd need to wrap `StreamMap` in some adapter that never(?) returns
            // `Poll::Ready(None)`. See this section of the docs:
            // https://docs.rs/join_me_maybe/0.4.1/join_me_maybe/#mutable-access-to-futures-and-streams
            subscriptions_canceller: (channel_name, msg) in subscriptions => {
                writer.write_frame(&make_message_frame(channel_name, msg)).await?;
            },
            frame_result in reader_stream => {
                handle_command(
                    frame_result?,
                    &mut self.channels,
                    subscriptions_canceller,
                    writer,
                ).await?;
                // `self.channels` is used to track additional channels to subscribe
                // to. When new `SUBSCRIBE` commands are received during the
                // execution of `apply`, the new channels are pushed onto this vec.
                for channel_name in self.channels.drain(..) {
                    subscribe_to_channel(
                        channel_name.clone(),
                        |messages| {
                            subscriptions_canceller.with_mut(|subscriptions| {
                                let subscriptions = subscriptions.expect("not finished or cancelled");
                                subscriptions.insert(channel_name, messages);
                                subscriptions.len()
                            })
                        },
                        db,
                        writer,
                    ).await?;
                }
            },
            _ = shutdown.recv() => return Ok(()),
        );
        Ok(())
    }

    /// Converts the command into an equivalent `Frame`.
    ///
    /// This is called by the client when encoding a `Subscribe` command to send
    /// to the server.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("subscribe".as_bytes()));
        for channel in self.channels {
            frame.push_bulk(Bytes::from(channel.into_bytes()));
        }
        frame
    }
}

async fn subscribe_to_channel(
    channel_name: String,
    insert_subscription: impl FnOnce(Messages) -> usize,
    db: &Db,
    dst: &mut ConnectionWriter,
) -> crate::Result<()> {
    let mut rx = db.subscribe(channel_name.clone());

    // Subscribe to the channel.
    let rx = Box::pin(async_stream::stream! {
        loop {
            match rx.recv().await {
                Ok(msg) => yield msg,
                // If we lagged in consuming messages, just resume.
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(_) => break,
            }
        }
    });

    // Track subscription in this client's subscription set.
    let len = insert_subscription(rx);

    // Respond with the successful subscription
    let response = make_subscribe_frame(channel_name, len);
    dst.write_frame(&response).await?;

    Ok(())
}

/// Handle a command received while inside `Subscribe::apply`. Only subscribe
/// and unsubscribe commands are permitted in this context.
///
/// Any new subscriptions are appended to `subscribe_to` instead of modifying
/// `subscriptions`.
async fn handle_command(
    frame: Frame,
    subscribe_to: &mut Vec<String>,
    subscriptions_canceller: &join_me_maybe::CancellerMut<'_, StreamMap<String, Messages>>,
    dst: &mut ConnectionWriter,
) -> crate::Result<()> {
    // A command has been received from the client.
    //
    // Only `SUBSCRIBE` and `UNSUBSCRIBE` commands are permitted
    // in this context.
    match Command::from_frame(frame)? {
        Command::Subscribe(subscribe) => {
            // The `apply` method will subscribe to the channels we add to this
            // vector.
            subscribe_to.extend(subscribe.channels.into_iter());
        }
        Command::Unsubscribe(mut unsubscribe) => {
            // If no channels are specified, this requests unsubscribing from
            // **all** channels. To implement this, the `unsubscribe.channels`
            // vec is populated with the list of channels currently subscribed
            // to.
            if unsubscribe.channels.is_empty() {
                subscriptions_canceller.with_mut(|subscriptions| {
                    unsubscribe.channels = subscriptions
                        .expect("not finished or cancelled")
                        .keys()
                        .map(|channel_name| channel_name.to_string())
                        .collect();
                });
            }

            for channel_name in unsubscribe.channels {
                let response = subscriptions_canceller.with_mut(|subscriptions| {
                    let subscriptions = subscriptions.expect("not finished or cancelled");
                    subscriptions.remove(&channel_name);
                    make_unsubscribe_frame(channel_name, subscriptions.len())
                });
                dst.write_frame(&response).await?;
            }
        }
        command => {
            let cmd = Unknown::new(command.get_name());
            cmd.apply(dst).await?;
        }
    }
    Ok(())
}

/// Creates the response to a subscribe request.
///
/// All of these functions take the `channel_name` as a `String` instead of
/// a `&str` since `Bytes::from` can reuse the allocation in the `String`, and
/// taking a `&str` would require copying the data. This allows the caller to
/// decide whether to clone the channel name or not.
fn make_subscribe_frame(channel_name: String, num_subs: usize) -> Frame {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"subscribe"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_int(num_subs as u64);
    response
}

/// Creates the response to an unsubcribe request.
fn make_unsubscribe_frame(channel_name: String, num_subs: usize) -> Frame {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"unsubscribe"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_int(num_subs as u64);
    response
}

/// Creates a message informing the client about a new message on a channel that
/// the client subscribes to.
fn make_message_frame(channel_name: String, msg: Bytes) -> Frame {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"message"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_bulk(msg);
    response
}

impl Unsubscribe {
    /// Create a new `Unsubscribe` command with the given `channels`.
    pub(crate) fn new(channels: &[String]) -> Unsubscribe {
        Unsubscribe {
            channels: channels.to_vec(),
        }
    }

    /// Parse an `Unsubscribe` instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `UNSUBSCRIBE` string has already been consumed.
    ///
    /// # Returns
    ///
    /// On success, the `Unsubscribe` value is returned. If the frame is
    /// malformed, `Err` is returned.
    ///
    /// # Format
    ///
    /// Expects an array frame containing at least one entry.
    ///
    /// ```text
    /// UNSUBSCRIBE [channel [channel ...]]
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> Result<Unsubscribe, ParseError> {
        use ParseError::EndOfStream;

        // There may be no channels listed, so start with an empty vec.
        let mut channels = vec![];

        // Each entry in the frame must be a string or the frame is malformed.
        // Once all values in the frame have been consumed, the command is fully
        // parsed.
        loop {
            match parse.next_string() {
                // A string has been consumed from the `parse`, push it into the
                // list of channels to unsubscribe from.
                Ok(s) => channels.push(s),
                // The `EndOfStream` error indicates there is no further data to
                // parse.
                Err(EndOfStream) => break,
                // All other errors are bubbled up, resulting in the connection
                // being terminated.
                Err(err) => return Err(err),
            }
        }

        Ok(Unsubscribe { channels })
    }

    /// Converts the command into an equivalent `Frame`.
    ///
    /// This is called by the client when encoding an `Unsubscribe` command to
    /// send to the server.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("unsubscribe".as_bytes()));

        for channel in self.channels {
            frame.push_bulk(Bytes::from(channel.into_bytes()));
        }

        frame
    }
}
