# reqwest-eventsource

Provides a simple wrapper for [`reqwest`] to provide an Event Source implementation with support for multiple content types including Server-Sent Events (SSE) and Amazon EventStream.

You can learn more about Server Sent Events (SSE) at [the MDN
docs](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events)

## Features

- **Server-Sent Events (SSE)** - Standard `text/event-stream` support
- **Amazon EventStream** - Binary `application/vnd.amazon.eventstream` support (feature gated)
- **Custom Parsers** - Extensible parser system for any content type
- **Automatic Retries** - Configurable retry logic for failed connections
- **Flexible API** - Works with any `reqwest::RequestBuilder`

## Quick Start

### Basic Usage

```rust
use reqwest_eventsource::{EventSource, Event};
use futures::StreamExt;

let mut es = EventSource::get("http://localhost:8000/events");
while let Some(event) = es.next().await {
    match event {
        Ok(Event::Open) => println!("Connection Open!"),
        Ok(Event::Message(message)) => println!("Message: {:#?}", message),
        Err(err) => {
            println!("Error: {}", err);
            es.close();
        }
    }
}
```

### Using with Amazon Bedrock (EventStream)

```rust
use reqwest_eventsource::EventSource;
use futures::StreamExt;

let client = reqwest::Client::new();
let request = client
    .post("https://bedrock-runtime.us-east-1.amazonaws.com/model/anthropic.claude-sonnet-4-5-v2/invoke-with-response-stream")
    .header("Content-Type", "application/json")
    .header("Authorization", "AWS4-HMAC-SHA256 ...") // Your AWS auth
    .json(&serde_json::json!({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 1000,
        "messages": [{"role": "user", "content": "Hello!"}]
    }));

let mut es = EventSource::new(request)?;
while let Some(event) = es.next().await {
    match event? {
        Event::Open => println!("Stream started"),
        Event::Message(message) => {
            match message.event.as_str() {
                "content_block_delta" => println!("Text: {}", message.data),
                "message_stop" => break,
                _ => println!("Event: {}", message.event),
            }
        }
    }
}
```

## Custom Parsers

You can extend the library to support any content type by implementing the `ContentParser` trait:

### Implementing a Custom Parser

```rust
use reqwest_eventsource::{ContentParser, ParserRegistryBuilder, EventSource};
use eventsource_stream::Event as MessageEvent;
use futures_util::StreamExt;

/// Custom parser for JSON Lines format (application/x-ndjson)
pub struct JsonLinesParser;

impl ContentParser for JsonLinesParser {
    fn can_parse(&self, content_type: &str) -> bool {
        content_type.contains("application/x-ndjson") || 
        content_type.contains("application/jsonlines")
    }
    
    fn parse(&self, response: reqwest::Response) -> Result<reqwest_eventsource::parser::ParsedEventStream, Box<dyn std::error::Error + Send + Sync>> {
        // Parse JSON lines - each line is a separate JSON object
        let stream = response.bytes_stream()
            .scan(String::new(), |buffer, chunk_result| {
                async move {
                    match chunk_result {
                        Ok(chunk) => {
                            buffer.push_str(&String::from_utf8_lossy(&chunk));
                            
                            let mut events = Vec::new();
                            while let Some(newline_pos) = buffer.find('\n') {
                                let line = buffer.drain(..=newline_pos).collect::<String>();
                                let line = line.trim();
                                
                                if !line.is_empty() {
                                    if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(line) {
                                        let event = MessageEvent {
                                            id: json_value.get("id")
                                                .and_then(|v| v.as_str())
                                                .unwrap_or("")
                                                .to_string(),
                                            event: json_value.get("event")
                                                .and_then(|v| v.as_str())
                                                .unwrap_or("message")
                                                .to_string(),
                                            retry: None,
                                            data: json_value.to_string(),
                                        };
                                        events.push(Ok(event));
                                    }
                                }
                            }
                            Some(events)
                        }
                        Err(e) => {
                            Some(vec![Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>)])
                        }
                    }
                }
            })
            .flat_map(futures_util::stream::iter);
            
        Ok(Box::pin(stream))
    }
    
    fn name(&self) -> &'static str {
        "JSON Lines Parser"
    }
}
```

### Using a Custom Parser

```rust
use reqwest_eventsource::{EventSource, ParserRegistryBuilder};

// Create a registry with default parsers + your custom parser
let registry = ParserRegistryBuilder::new()
    .with_default_parsers()              // Includes SSE and Amazon EventStream
    .with_parser(JsonLinesParser)        // Add your custom parser
    .build();

// Create EventSource with custom registry
let request = reqwest::Client::new()
    .get("https://api.example.com/stream")
    .header("Accept", "application/x-ndjson");

let mut es = EventSource::with_parser_registry(request, registry)?;

// The library will automatically use JsonLinesParser for ndjson content
while let Some(event) = es.next().await {
    // Handle events...
}
```

### Parser Examples

The library includes examples for various parser types:

- **Server-Sent Events** (`text/event-stream`) - Built-in
- **Amazon EventStream** (`application/vnd.amazon.eventstream`) - Built-in
- **JSON Lines** (`application/x-ndjson`) - Custom parser example
- **XML Events** (`application/xml+events`) - Custom parser example

See `examples/custom_parser.rs` for complete implementations.

## Feature Flags

### Default Features

```toml
[dependencies]
reqwest-eventsource = "0.6.0"
```

Includes: SSE parser + Amazon EventStream parser

### Minimal Build (SSE only)

```toml
[dependencies]
reqwest-eventsource = { version = "0.6.0", default-features = false }
```

Includes: SSE parser only (lighter dependencies)

### Custom Features

```toml
[dependencies]
reqwest-eventsource = { version = "0.6.0", features = ["amazon-eventstream"] }
```

## API Reference

### Core Types

- `EventSource` - Main stream type
- `Event` - Event enum (Open, Message)
- `ContentParser` - Trait for implementing custom parsers
- `ParserRegistry` - Registry holding multiple parsers
- `ParserRegistryBuilder` - Builder for creating registries

### Key Methods

- `EventSource::new(RequestBuilder)` - Create with default parsers
- `EventSource::with_parser_registry(RequestBuilder, ParserRegistry)` - Create with custom parsers
- `EventSource::get(url)` - Simple GET request
- `ParserRegistryBuilder::with_default_parsers()` - Add built-in parsers
- `ParserRegistryBuilder::with_parser(parser)` - Add custom parser

## License

MIT OR Apache-2.0
