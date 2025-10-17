use reqwest_eventsource::{EventSource, ContentParser, ParserRegistryBuilder};
use eventsource_stream::Event as MessageEvent;
use reqwest::Response;

/// Custom parser for JSON lines format
pub struct JsonLinesParser;

impl ContentParser for JsonLinesParser {
    fn can_parse(&self, content_type: &str) -> bool {
        content_type.contains("application/x-ndjson") || 
        content_type.contains("application/jsonlines")
    }
    
    fn parse(&self, response: Response) -> Result<reqwest_eventsource::parser::ParsedEventStream, Box<dyn std::error::Error + Send + Sync>> {
        use futures_util::StreamExt;
        
        // Parse JSON lines - each line is a separate JSON object
        let stream = response.bytes_stream()
            .scan(String::new(), |buffer, chunk_result| {
                let events = match chunk_result {
                    Ok(chunk) => {
                        buffer.push_str(&String::from_utf8_lossy(&chunk));
                        
                        let mut events = Vec::new();
                        while let Some(newline_pos) = buffer.find('\n') {
                            let line = buffer.drain(..=newline_pos).collect::<String>();
                            let line = line.trim();
                            
                            if !line.is_empty() {
                                // Parse each line as JSON and create an event
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
                                        retry: json_value.get("retry")
                                            .and_then(|v| v.as_u64())
                                            .map(|ms| std::time::Duration::from_millis(ms)),
                                        data: json_value.to_string(),
                                    };
                                    events.push(Ok(event));
                                }
                            }
                        }
                        events
                    }
                    Err(e) => {
                        vec![Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>)]
                    }
                };
                
                async move { Some(events) }
            })
            .flat_map(futures_util::stream::iter);
            
        Ok(Box::pin(stream))
    }
    
    fn name(&self) -> &'static str {
        "JSON Lines Parser"
    }
}

/// Custom parser for XML events (simplified example)
pub struct XmlEventsParser;

impl ContentParser for XmlEventsParser {
    fn can_parse(&self, content_type: &str) -> bool {
        content_type.contains("application/xml+events") || 
        content_type.contains("text/xml+events")
    }
    
    fn parse(&self, response: Response) -> Result<reqwest_eventsource::parser::ParsedEventStream, Box<dyn std::error::Error + Send + Sync>> {
        use futures_util::StreamExt;
        
        // Simple XML event parsing (in reality you'd use a proper XML parser)
        let stream = response.bytes_stream()
            .scan(String::new(), |buffer, chunk_result| {
                let events = match chunk_result {
                    Ok(chunk) => {
                        buffer.push_str(&String::from_utf8_lossy(&chunk));
                        
                        let mut events = Vec::new();
                        // Look for complete <event>...</event> tags
                        while let Some(_start) = buffer.find("<event>") {
                            if let Some(end) = buffer.find("</event>") {
                                let event_xml = buffer.drain(..end + 8).collect::<String>();
                                
                                // Very simple XML parsing - extract data between <data> tags
                                let data = if let Some(data_start) = event_xml.find("<data>") {
                                    if let Some(data_end) = event_xml.find("</data>") {
                                        event_xml[data_start + 6..data_end].to_string()
                                    } else {
                                        "".to_string()
                                    }
                                } else {
                                    event_xml
                                };
                                
                                let event = MessageEvent {
                                    id: String::new(),
                                    event: "xml-event".to_string(),
                                    retry: None,
                                    data,
                                };
                                events.push(Ok(event));
                            } else {
                                break; // Wait for more data
                            }
                        }
                        events
                    }
                    Err(e) => {
                        vec![Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>)]
                    }
                };
                
                async move { Some(events) }
            })
            .flat_map(futures_util::stream::iter);
            
        Ok(Box::pin(stream))
    }
    
    fn name(&self) -> &'static str {
        "XML Events Parser"
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Custom Parser Example");
    
    // Create a custom parser registry
    let registry = ParserRegistryBuilder::new()
        .with_default_parsers() // Include SSE and Amazon EventStream
        .with_parser(JsonLinesParser) // Add JSON Lines support
        .with_parser(XmlEventsParser) // Add XML Events support
        .build();
    
    println!("Supported content types:");
    for content_type in registry.supported_content_types() {
        println!("  - {}", content_type);
    }
    
    // Example 1: JSON Lines endpoint
    println!("\n--- JSON Lines Example ---");
    let json_request = reqwest::Client::new()
        .get("https://api.example.com/stream")
        .header("Accept", "application/x-ndjson");
    
    let _es = EventSource::with_parser_registry(json_request, registry)?;
    
    // In a real scenario, you'd connect to an actual endpoint
    println!("Would connect to JSON Lines endpoint...");
    
    // Example 2: Show how different parsers would handle different content types
    println!("\n--- Parser Selection Demo ---");
    
    let test_cases = vec![
        ("text/event-stream", "Server-Sent Events"),
        ("application/vnd.amazon.eventstream", "Amazon EventStream"),
        ("application/x-ndjson", "JSON Lines"),
        ("application/xml+events", "XML Events"),
        ("application/unsupported", "No parser available"),
    ];
    
    let registry = ParserRegistryBuilder::new()
        .with_default_parsers()
        .with_parser(JsonLinesParser)
        .with_parser(XmlEventsParser)
        .build();
    
    for (content_type, _expected) in test_cases {
        let parser = registry.find_parser(content_type);
        match parser {
            Some(p) => println!("  {}: {} ✓", content_type, p.name()),
            None => println!("  {}: No parser available ✗", content_type),
        }
    }
    
    Ok(())
}