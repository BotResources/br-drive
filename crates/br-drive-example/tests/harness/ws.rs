use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

pub struct Subscription {
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl Subscription {
    pub async fn open(ws_url: &str, passport: &str, query: &str) -> Subscription {
        let mut request = ws_url
            .into_client_request()
            .expect("a valid websocket request");
        request.headers_mut().insert(
            "sec-websocket-protocol",
            "graphql-transport-ws".parse().unwrap(),
        );
        request
            .headers_mut()
            .insert("x-passport", passport.parse().unwrap());
        let (mut socket, _response) = tokio_tungstenite::connect_async(request)
            .await
            .expect("the subscription websocket upgrades");

        socket
            .send(Message::text(r#"{"type":"connection_init"}"#))
            .await
            .expect("send connection_init");
        let ack = read_json(&mut socket, Duration::from_secs(5)).await;
        assert_eq!(
            ack["type"], "connection_ack",
            "server acknowledges the init"
        );

        let subscribe = serde_json::json!({
            "id": "1",
            "type": "subscribe",
            "payload": { "query": query },
        });
        socket
            .send(Message::text(subscribe.to_string()))
            .await
            .expect("send subscribe");
        Subscription { socket }
    }

    pub async fn next_payload(&mut self, within: Duration) -> serde_json::Value {
        loop {
            let frame = read_json(&mut self.socket, within).await;
            match frame["type"].as_str() {
                Some("next") => return frame["payload"]["data"].clone(),
                Some("error") => panic!("the subscription returned an error: {frame}"),
                Some("complete") => panic!("the subscription completed before a delta arrived"),
                _ => continue,
            }
        }
    }
}

async fn read_json(
    socket: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    within: Duration,
) -> serde_json::Value {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let next = tokio::time::timeout(remaining, socket.next())
            .await
            .expect("a websocket frame arrives before the deadline");
        match next {
            Some(Ok(Message::Text(text))) => {
                return serde_json::from_str(&text).expect("a json websocket frame");
            }
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
            Some(Ok(Message::Close(_))) | None => panic!("the websocket closed unexpectedly"),
            Some(Ok(_)) => continue,
            Some(Err(error)) => panic!("websocket error: {error}"),
        }
    }
}
