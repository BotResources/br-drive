use std::time::Duration;

/// A subscription opened over `POST /graphql` with `Accept: text/event-stream`
/// — the leg a gateway uses — read frame by frame.
pub struct SseSubscription {
    response: reqwest::Response,
    buffer: String,
}

/// One server-sent event: its `event` name and its `data` line.
#[derive(Debug, Clone)]
pub struct SseFrame {
    pub event: String,
    pub data: String,
}

impl SseFrame {
    /// The `data` line as JSON (`null` for the empty `complete` frame).
    pub fn json(&self) -> serde_json::Value {
        if self.data.is_empty() {
            return serde_json::Value::Null;
        }
        serde_json::from_str(&self.data).expect("a json sse data line")
    }
}

impl SseSubscription {
    pub async fn open(
        http: &reqwest::Client,
        url: &str,
        passport: &str,
        query: &str,
        variables: serde_json::Value,
    ) -> SseSubscription {
        let response = http
            .post(url)
            .header("x-passport", passport)
            .header("accept", "text/event-stream")
            .json(&serde_json::json!({ "query": query, "variables": variables }))
            .send()
            .await
            .expect("the sse request reaches the service");
        assert!(
            response.status().is_success(),
            "the sse request is answered with a stream: {}",
            response.status()
        );
        SseSubscription {
            response,
            buffer: String::new(),
        }
    }

    /// The next frame, or `None` once the stream ended.
    pub async fn next_frame(&mut self, within: Duration) -> Option<SseFrame> {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            if let Some(end) = self.buffer.find("\n\n") {
                let raw: String = self.buffer.drain(..end + 2).collect();
                let mut frame = SseFrame {
                    event: String::new(),
                    data: String::new(),
                };
                for line in raw.lines() {
                    if let Some(event) = line.strip_prefix("event:") {
                        frame.event = event.trim().to_string();
                    } else if let Some(data) = line.strip_prefix("data:") {
                        frame.data.push_str(data.trim_start());
                    }
                }
                if frame.event.is_empty() && frame.data.is_empty() {
                    continue;
                }
                return Some(frame);
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let chunk = tokio::time::timeout(remaining, self.response.chunk())
                .await
                .expect("an sse frame arrives before the deadline")
                .expect("the sse stream reads");
            match chunk {
                Some(bytes) => self
                    .buffer
                    .push_str(std::str::from_utf8(&bytes).expect("an utf-8 sse stream")),
                None => return None,
            }
        }
    }

    /// The first delta of an admitted stream; fails on a refusal.
    pub async fn next_payload(&mut self, within: Duration) -> serde_json::Value {
        let frame = self
            .next_frame(within)
            .await
            .expect("the stream answers a delta before it ends");
        assert_eq!(frame.event, "next", "a delta, not {frame:?}");
        let body = frame.json();
        assert!(
            body.get("errors").is_none(),
            "the subscription answered a refusal, not a delta: {body}"
        );
        body["data"].clone()
    }

    /// The refusal the stream opens with; then asserts the stream completes
    /// and ends with no delta.
    pub async fn refusal(mut self, within: Duration) -> serde_json::Value {
        let frame = self
            .next_frame(within)
            .await
            .expect("the stream answers its refusal before it ends");
        assert_eq!(frame.event, "next", "the refusal rides a next frame");
        let body = frame.json();
        assert!(
            body["data"].is_null(),
            "a refused subscription carries no data: {body}"
        );
        let errors = body["errors"].clone();
        assert!(errors.is_array(), "the refusal carries errors: {body}");
        let complete = self
            .next_frame(within)
            .await
            .expect("a complete follows the refusal");
        assert_eq!(complete.event, "complete", "{complete:?}");
        assert!(
            self.next_frame(within).await.is_none(),
            "the stream ends after its complete"
        );
        errors
    }
}
